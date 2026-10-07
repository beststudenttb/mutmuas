"""Agent-facing operations, shared by the MCP server and the CLI.

Each function takes an open Hub and the acting agent's address and returns
plain JSON-able dicts, so the same behaviour is reachable from Claude Code,
Codex (via MCP) and from scripts/humans (via ``agentctl``).
"""

from __future__ import annotations

import asyncio
import functools
import re
import os
import shlex
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import letters, node   # node imports tools as well: its names are looked up when called
from .hub import Hub
from .ids import Address, parse_iso
from .visibility import acl, artifact_visible, is_participant, short
from .protocol import (ACCEPTANCE_VERDICTS, DELIVERED, OPEN_STATES, TERMINAL_STATES, ArtifactRef, Envelope,
                       request_body, result_body)


def _current_task() -> str | None:
    return os.environ.get("MUTMUAS_TASK_ID") or None


def card_summary(card: dict[str, Any]) -> dict[str, Any]:
    keys = ("address", "display", "role", "mode", "auto_worker", "runtime", "provider", "model", "capabilities", "permissions",
            "accept_from", "state", "availability", "online", "last_heartbeat", "session", "session_seen",
            "description")
    return {k: card.get(k) for k in keys if card.get(k) not in (None, "", [])}


async def list_agents(hub: Hub, capability: str | None = None, online_only: bool = False) -> list[dict]:
    return [card_summary(c) for c in await hub.agents(capability, online_only)]


async def find_agent(hub: Hub, capability: str) -> dict[str, Any]:
    candidates = await hub.agents(capability)
    if not candidates:
        return {"found": False, "message": f"no agent advertises capability or role {capability!r}"}
    return {"found": True, "best": card_summary(candidates[0]),
            "alternatives": [c["address"] for c in candidates[1:]]}


DEADLINE_MARGIN_S = 1800     # a default deadline comes at least this long after the task's timeout_s


async def send_request(hub: Hub, me: str, to: str, objective: str, reason: str, *, kind: str = "query",
                       inputs: Any = None, expected_outputs: Any = None, constraints: Any = None,
                       acceptance_criteria: Any = None, timeout_s: float | None = None,
                       deadline: str | None = None, artifacts: list[dict] | None = None,
                       parent_task: str | None = None, priority: str = "normal",
                       reply: str | None = None, observers: list[str] | None = None,
                       leader: bool = False, project: str | None = None,
                       acceptance: str | None = None) -> dict[str, Any]:
    """acceptance="manual" (D-109; what agents get through MCP): the delivery waits for this agent to accept it
    (accept_delivery). Otherwise it is accepted on delivery, as for scripts."""
    default_deadline = None
    for observer in observers or []:
        Address.parse(observer)        # before anything goes out: a bad one is refused, not half sent
    deadline = from_now(deadline, "deadline")
    if not deadline and reply != "none":
        default_deadline = default_reply_deadline(hub, timeout_s, kind)
    body = request_body(objective, reason, kind=kind, inputs=inputs, expected_outputs=expected_outputs,
                        constraints=constraints, acceptance_criteria=acceptance_criteria,
                        deadline=deadline or default_deadline, timeout_s=timeout_s, reply=reply, observers=observers,
                        deadline_default=bool(default_deadline),  # the owner can tell it from a chosen one
                        leader=leader, acceptance=acceptance)
    body["title"] = letters.title("request", {"objective": objective})
    return await _send_request(hub, me, to, body, artifacts=artifacts, parent_task=parent_task, priority=priority,
                               project=project)


LONG_KINDS = ("experiment", "code")       # their default deadline is long_reply_deadline_s (D-098)


UNITS_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _interval_s(text: str) -> float | None:
    """'30s' / '10m' / '5h' / '1d' (a leading + is allowed) in seconds; None if it is not one."""
    m = re.fullmatch(r"\+?(\d+(?:\.\d+)?)([smhd])", text or "")
    return float(m[1]) * UNITS_S[m[2]] if m else None


def from_now(text: str | None, field: str) -> str | None:
    """A time given from now: +30s, +10m, +2h, +1d (more than zero), as an ISO time. The only way deadlines, etas
    and reminders are written (D-102): one in the past, or without a timezone, cannot be given."""
    if text is None:
        return None
    if not (text.startswith("+") and (delay := _interval_s(text)) and delay >= 1):
        raise ValueError(f"{field}={text!r}: give it from now, at least a second: +30m, +2h, +1d")
    return (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat(timespec="seconds")


def default_reply_deadline(hub: Hub, timeout_s: float | None, kind: str | None = "query") -> str | None:
    if hub.cfg.default_reply_deadline_s > 0:
        # Without a deadline nothing ever chases a missing reply (no-stall design, G3): take the node's default, but
        # never before the task's own run limit plus a margin, or a long task would be chased while it still runs
        # normally. Experiments and code get the longer default (D-098).
        wait_s = hub.cfg.default_reply_deadline_s
        if kind in LONG_KINDS and hub.cfg.long_reply_deadline_s > 0:
            wait_s = hub.cfg.long_reply_deadline_s
        if timeout_s:
            wait_s = max(wait_s, timeout_s + DEADLINE_MARGIN_S)
        return (datetime.now(timezone.utc) + timedelta(seconds=wait_s)).isoformat(timespec="seconds")
    return None


async def _send_request(hub: Hub, me: str, to: str, body: dict[str, Any], *, artifacts, parent_task, priority,
                        project) -> dict[str, Any]:
    default_deadline = body.get("deadline") if body.get("deadline_default") else None
    if project:
        body["project"] = project                    # D-069: the recipient routes it to that project's directory
    sender = str(hub.local_agent(me)[0])
    for ref in artifacts or []:
        # Attaching grants the recipient access (visibility.artifact_visible), so only what the sender may see
        # itself can be attached: no forwarding of guessed or foreign URIs.
        if not artifact_visible(hub.ledger, sender, ref.get("uri", "")):
            raise PermissionError(f"{sender} may not attach {ref.get('uri')!r}: attach only artifacts you "
                                  "published or received")
    target = await hub.card_or_none(to)
    task_id, delivery = await hub.request(
        me, to, body, artifacts=[ArtifactRef.from_dict(a) for a in artifacts or []],
        parent_task=parent_task or _current_task(), priority=priority)
    out = {"task_id": task_id, "to": await hub.resolve(to), "delivery": delivery}
    if default_deadline:
        out["note_deadline"] = (f"default deadline {default_deadline} (node.yaml default_reply_deadline_s, "
                                "long_reply_deadline_s for experiment/code); "
                                "pass deadline= to set your own")
    if delivery == "queued":
        out["note"] = "message bus unreachable; kept in the local outbox and sent automatically on reconnect"
    elif target is None:
        out["note"] = "target agent is not registered yet; the message is stored durably until it comes online"
    elif not target.get("online"):
        out["note"] = "target agent is offline; the message waits in its durable inbox"
    return out


def _brief(view: dict[str, Any]) -> dict[str, Any]:
    keys = ("task_id", "status", "eta", "result_status", "requester", "owner", "objective", "result", "output_refs",
            "attempts", "updated_at", "timed_out_waiting")
    out = {k: view.get(k) for k in keys if view.get(k) not in (None, "", [])}
    thread = view.get("thread") or []
    if thread:
        out["recent_messages"] = [{"type": m.get("type"), "from": m.get("from"), "at": m.get("timestamp"),
                                   "note": _note(m)} for m in thread[-6:]]
    return out


def _note(m: dict[str, Any]) -> str:
    if m.get("note"):
        return m["note"]
    body = m.get("body") or {}
    for key in ("summary", "message", "reason", "question", "answer", "objective"):
        if body.get(key):
            return str(body[key])[:300]
    return ""


async def check_task(hub: Hub, task_id: str, me: str | None = None) -> dict[str, Any]:
    view = await hub.task_view(task_id, me)
    return _brief(view) if view else {"error": f"unknown task {task_id} (or not visible to you)"}


async def wait_for_result(hub: Hub, task_id: str, timeout_s: float = 600, me: str | None = None) -> dict[str, Any]:
    return _brief(await hub.wait_result(task_id, timeout_s, viewer=me))


# Messages that need a decision from the recipient; ACKs and progress UPDATEs are informational.
ACTIONABLE = ("REQUEST", "QUESTION", "ANSWER", "RESULT", "BLOCKED", "REJECT", "CANCEL", "ERROR")
# What should interrupt an interactive session right away: someone needs *me* to act. RESULTs of my own
# requests are not in it: they are read when I next look, or when I explicitly wait on that task. Exception:
# an agent with wake_on_own_results (node.yaml, per agent, default off) is also woken by the RESULT of its own
# request that wants a reply (ledger._type_filter; docs/MESSAGE_PROTOCOL.md "Waking").
WAKE = ("REQUEST", "QUESTION", "ANSWER", "BLOCKED", "REJECT", "CANCEL", "ERROR")


def _project_view(hub: Hub, me: str) -> tuple[str, str] | None:
    """A project's session sees only its project's requests (D-072): (its project, the post's default project);
    the rest go to the worker, so only when there is one. None: the session sees all."""
    addr, agent = hub.local_agent(me)
    session = hub.ledger.session_of(str(addr))
    alive = session and node.session_alive(session)
    if alive and agent.auto_worker and not session.get("accepting", 1):
        return ("\x00off", agent.default_project)    # switched off: no request is the session's (none matches)
    mine = agent.session_project(session["cwd"]) if alive else None
    return (mine, agent.default_project) if mine and agent.auto_worker else None


def _wake_view(hub: Hub, me: str, types: tuple[str, ...] | None) -> tuple[str | None, bool]:
    """For the wake/actionable views: the address a baton-passing message names (body.next), and whether the
    agent is also woken by the results of its own requests."""
    addr, agent = hub.local_agent(me)
    next_to = str(addr) if types in (WAKE, ACTIONABLE) else None
    return next_to, bool(next_to) and agent.wake_on_own_results


async def inbox_page(hub: Hub, me: str, peek: bool = False, types: tuple[str, ...] | None = None,
                     before_seq: int | None = None, leader_before_seq: int | None = None,
                     limit: int = 50, show: bool = True) -> dict[str, Any]:
    """inbox() for a session looking at its mail, plus what the page left out (D-074): how many unread the cursor
    covers, how many it lists, how many older ones it did not, and `next`, the cursor for the following page.
    Paging has two stages: the leader's mail first (leader_before_seq), then the other
    mail (before_seq), each newest to oldest, so every message is listed exactly once."""
    addr, _ = hub.local_agent(me)
    next_to, own_results = _wake_view(hub, me, types)
    count = functools.partial(hub.ledger.unseen_count, str(addr), types, next_to=next_to, own_results=own_results,
                              project=_project_view(hub, me))        # the same view the listing has (D-072)
    total = count(before_seq=before_seq, leader_before_seq=leader_before_seq)
    rows = await inbox(hub, me, limit=limit, peek=peek, types=types, before_seq=before_seq, show=show,
                       leader_before_seq=leader_before_seq)
    page: dict[str, Any] = {"messages": rows, "unread": total, "listed": len(rows),
                            "older_unlisted": max(0, total - len(rows))}
    if page["older_unlisted"]:
        others = [r["seq"] for r in rows if not (r["body"] or {}).get("leader")]
        leaders = [r["seq"] for r in rows if (r["body"] or {}).get("leader")]
        if others:
            page["next"] = {"before_seq": min(others)}
        elif leaders and count(leader_before_seq=min(leaders), leader_only=True):
            page["next"] = {"leader_before_seq": min(leaders)}               # more of the leader's mail first
        else:
            page["next"] = {"before_seq": hub.ledger.last_rowid() + 1}     # the leader's done: the rest from the top
        page.update(page["next"])
        key, value = next(iter(page["next"].items()))
        page["more"] = (f"{page['older_unlisted']} older unread not listed: inbox({key}={value})"
                        f" / agentctl inbox --{key.replace('_', '-')} {value}")
    return page


async def inbox(hub: Hub, me: str, include_seen: bool = False, limit: int = 50, peek: bool = False,
                wait_s: float | None = None, types: tuple[str, ...] | None = None,
                since: str | None = None, show: bool = True, before_seq: int | None = None,
                leader_before_seq: int | None = None) -> list[dict[str, Any]]:
    """Unread messages for ``me``. peek: do not mark them read. wait_s: block until one arrives (or timeout).
    types: only these message types (e.g. ACTIONABLE), for both waiting and listing.
    since: only messages that reached this node's ledger after this ISO timestamp (a notifier's cursor,
    so --peek does not report the same unread message again and again).
    show=False: a notifier's read (watch, push), which does not count as showing the mail to the session."""
    addr, _ = hub.local_agent(me)
    project = _project_view(hub, me)
    # A message that hands me the baton (body.next == me) needs me as much as a REQUEST does. Results of my own requests
    # too only with wake_on_own_results (no-stall G2, per agent, off by default).
    next_to, own_results = _wake_view(hub, me, types)
    if wait_s and not include_seen:
        # Messages reach this node's ledger through the daemon, so waiting on the ledger is enough
        # (a second JetStream consumer on the same mailbox would split the messages).
        deadline = asyncio.get_running_loop().time() + wait_s
        while (hub.ledger.unseen_count(str(addr), types, since, next_to=next_to, own_results=own_results,
                                       project=project) == 0
               and asyncio.get_running_loop().time() < deadline):
            await asyncio.sleep(0.5)
    if include_seen:
        envs = hub.ledger.list_recent_inbound(str(addr), limit, show=show)
    else:
        # The leader's mail first (D-049), then the newest (D-074); a page further back (before_seq) is plain
        # newest-first. A watcher's cursor (since) keeps arrival order: its cursor is the last row.
        envs = hub.ledger.unseen(str(addr), limit, mark=not peek, types=types, since=since, next_to=next_to,
                                 show=show, own_results=own_results, leader_first=before_seq is None,
                                 project=project, before_seq=before_seq, leader_before_seq=leader_before_seq)
        if not peek:
            await _read_receipts(hub, str(addr), envs)
    meta = {r[0]: (r[1], r[2], r[3]) for r in hub.ledger.db.execute(
        f"SELECT message_id, created_at, rowid, last_error FROM messages WHERE direction='in' AND message_id IN "
        f"({','.join('?' * len(envs))})", [e.message_id for e in envs]).fetchall()} if envs else {}
    rows = []
    for e in envs:
        received, seq, note = meta.get(e.message_id, (None, None, None))
        row = {"message_id": e.message_id, "type": e.type, "from": e.sender, "task_id": e.task_id,
               "timestamp": e.timestamp, "received_at": received, "seq": seq, "body": e.body,
               "artifacts": [a.to_dict() for a in e.artifacts]}
        if note:
            row["note"] = note          # e.g. "rejected: permission denied: …" — already refused, FYI
        rows.append(row)
    return rows


async def clear_inbox(hub: Hub, me: str, before_seq: int) -> dict[str, Any]:
    """Mark every unread message up to seq (the rowid shown by inbox) as read. For the session only, after it
    has looked at the list: clearing a backlog of old mail it has already dealt with elsewhere."""
    addr, _ = hub.local_agent(me)
    cleared = hub.ledger.mark_seen_before(str(addr), int(before_seq))
    await _read_receipts(hub, str(addr), cleared, read=False)   # close cleared notices, but don't say "read"
    not_shown = hub.ledger.unshown_count(str(addr), int(before_seq))
    out = {"marked_read": len(cleared), "up_to_seq": int(before_seq)}
    if not_shown:
        out["left_unread"] = f"{not_shown} message(s) never listed by inbox: look at them first"
    return out


async def remind_me(hub: Hub, me: str, at: str, text: str, every: str | None = None,
                    task_id: str | None = None) -> dict[str, Any]:
    """The node puts `text` into the agent's inbox at `at` (+30s/+10m/+2h from now) as a message
    that hands the agent the baton, so it wakes a session and waits in the inbox while
    none runs (D-066). every='5h' repeats it at that interval until cancel_reminder. task_id: the task a worker run
    sets it for: a post with no session gets that task run again when it fires (D-098)."""
    addr, _ = hub.local_agent(me)
    every_s = None
    if every is not None:
        every_s = _interval_s(every)
        if not every_s:
            raise ValueError(f"every={every!r}: use an interval like 30m, 5h or 1d")
    due = parse_iso(from_now(at, "at"))
    due_iso = due.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return {"reminder": hub.ledger.add_reminder(str(addr), due_iso, text, every_s, task_id), "due": due_iso,
            "every_s": every_s}


async def cancel_reminder(hub: Hub, me: str, reminder: int) -> dict[str, Any]:
    addr, _ = hub.local_agent(me)
    return {"reminder": reminder, "cancelled": hub.ledger.cancel_reminder(str(addr), int(reminder))}


async def _read_receipts(hub: Hub, me: str, envs: list[Envelope], read: bool = True) -> None:
    """A REQUEST sent with reply: none is answered by being read: the session has now seen it, so close the
    task with a read receipt. Only the session reading its mail gets here (peek never does).
    read=False (clear_inbox): the notice is closed too, but the receipt says it was cleared without being
    read, so a sender can tell a blind clear from a real read."""
    for env in envs:
        if env.type != "REQUEST" or (env.body or {}).get("reply") != "none":
            continue
        task = hub.ledger.task(env.task_id, "owner")
        if task and task["owner"] == me and task["status"] not in TERMINAL_STATES:
            summary = (f"read by {me} (no reply requested)" if read
                       else f"cleared by {me} without reading (clear_inbox; no reply requested)")
            await hub.finish(env.task_id, result_body("complete", summary), record=False)   # a notice, not work


async def accept_task(hub: Hub, me: str, task_id: str, eta: str | None = None) -> dict[str, Any]:
    eta = from_now(eta, "eta")
    if _check_actor(hub, _owned(hub, me, task_id)) == task_id:
        # A worker's task was accepted for it when the daemon started it: nothing to do, and no session claim
        return {"task_id": task_id, "accepted": True, "note": "already accepted for you when the worker started"}
    refused = hub.ledger.claim_task(task_id, "session", OPEN_STATES)
    if refused and "worker" in refused:
        raise PermissionError(f"{task_id} is being done by the worker the daemon started before this session; "
                              "it is not interrupted (D-032a): wait for its result (whoami: worker_running)")
    body = {"state": "RUNNING", "message": f"accepted by {me}" + (f"; eta {eta}" if eta else ""),
            "title": letters.title("receipt", {}, _original(hub, task_id))}
    if eta:
        body["eta"] = eta                         # the requester's node chases it once it passes (D-076)
        hub.ledger.update_task(task_id, "owner", eta=eta)
    ok = await hub.owner_transition(task_id, "RUNNING", body["message"], msg_type="ACK", body=body)
    return {"task_id": task_id, "accepted": ok, **({"eta": eta} if eta else {})}


async def start_job(hub: Hub, me: str, command: str, note: str, task_id: str | None = None,
                    cwd: str | None = None) -> dict[str, Any]:
    """Start a long job and let the task wait on it (D-104): the command runs on its own (its own session, so the
    end or stop of the run that started it, or of the daemon, leaves it alone), its output goes to a log under the
    node's runs/jobs, and its exit code to a done-file. The node wakes the task when it ends."""
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    addr, agent = hub.local_agent(me)
    if not agent.has("RUN_EXPERIMENT"):                 # a shell command: what grants this post's workers Bash
        raise PermissionError(f"{addr} lacks RUN_EXPERIMENT permission: it may not start commands")
    _check_actor(hub, _owned(hub, me, task_id))            # refused before anything starts
    jobs = hub.cfg.data_path / "runs" / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    stem = jobs / f"{task_id}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    log, done = f"{stem}.log", f"{stem}.done"
    # a subshell, so an `exit` in the command still leaves its exit code in the done-file
    script = f"(\n{command}\n)\necho $? > {shlex.quote(done)}.tmp && mv {shlex.quote(done)}.tmp {shlex.quote(done)}\n"
    with open(log, "wb") as out:
        proc = subprocess.Popen(["/bin/sh", "-c", script], cwd=cwd or os.getcwd(), stdin=subprocess.DEVNULL,
                                stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    job = await add_job(hub, me, task_id, pid=proc.pid, done_file=done, log=log, note=note)
    return {**job, "pid": proc.pid, "log": log, "done_file": done}


async def add_job(hub: Hub, me: str, task_id: str | None = None, pid: int | None = None,
                  done_file: str | None = None, log: str | None = None, note: str | None = None,
                  children: bool = False) -> dict[str, Any]:
    """Register a background job (e.g. training) the task waits on (D-050). The task becomes WAITING; a worker may
    then exit without a result, and is not treated as failed. The node's heartbeat notices when the job ends
    (process pid gone, or done_file appears) and wakes the post: a worker's task is queued again, a session gets
    a wake-up. No time limit. children=True (D-066): wait on the task's direct child tasks instead (sent with
    parent_task); it ends when each has a result, was refused or cancelled, or is past its deadline."""
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    if pid is None and not done_file and not children:
        raise ValueError("give pid or done_file: how the node tells that the job has ended")
    task = _owned(hub, me, task_id)
    _check_actor(hub, task)
    if children and not hub.ledger.children(task_id):
        raise ValueError(f"{task_id} has no child task to wait on: send them with parent_task={task_id} first")
    job_id = hub.ledger.add_job(task_id, task["owner"], pid, node.proc_start(pid) if pid else None, done_file, log,
                                note, children=children)
    what = note or ("its child tasks" if children else f"pid {pid}" if pid else f"until {done_file}")
    await hub.owner_transition(task_id, "WAITING", f"waiting on a background job ({what}); resumes when it ends",
                               body={"state": "WAITING", "message": f"waiting on a background job ({what})"})
    return {"task_id": task_id, "job_id": job_id, "state": "WAITING"}


async def reject_task(hub: Hub, me: str, task_id: str, reason: str, suggest: str | None = None) -> dict[str, Any]:
    _check_actor(hub, _owned(hub, me, task_id))
    body = {"reason": reason, "title": letters.title("refusal", {}, _original(hub, task_id)),
            **({"suggest": suggest} if suggest else {})}                     # who to ask instead (D-109)
    ok = await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT", body=body)
    return {"task_id": task_id, "rejected": ok}


async def report_progress(hub: Hub, me: str, message: str, task_id: str | None = None,
                          state: str | None = None, next: str | None = None,
                          eta: str | None = None) -> dict[str, Any]:
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    task = _owned(hub, me, task_id)
    _check_actor(hub, task)
    new_state = state or task["status"]
    if new_state not in ("RUNNING", "WAITING", "BLOCKED"):
        new_state = "RUNNING"
    msg_type = "BLOCKED" if new_state == "BLOCKED" else "UPDATE"
    body = {"reason": message} if msg_type == "BLOCKED" else {"state": new_state, "message": message}
    body["title"] = letters.title("progress", {}, task["request"].get("objective"))
    if next:
        body["next"] = next
    if (eta := from_now(eta, "eta")):
        body["eta"] = eta                         # a new estimate: what a chase asks for (D-076)
        hub.ledger.update_task(task_id, "owner", eta=eta)
    ok = await hub.owner_transition(task_id, new_state, message, msg_type=msg_type, body=body)
    if (new_state == "WAITING" and not any(j["children"] for j in hub.ledger.jobs(task_id))
            and any(c["status"] not in TERMINAL_STATES for c in hub.ledger.children(task_id))):
        # Waiting with open child tasks is waiting on them: the node wakes this task when they are done (D-066)
        hub.ledger.add_job(task_id, task["owner"], None, None, None, None, "child tasks", children=True)
    return {"task_id": task_id, "state": new_state, "sent": ok}


async def submit_result(hub: Hub, me: str, status: str, summary: str, *, task_id: str | None = None,
                        outputs: Any = None, artifacts: list[dict] | None = None, evidence: Any = None,
                        limitations: Any = None, follow_up: Any = None, next: str | None = None, how: str | None = None,
                        notes: str | None = None) -> dict[str, Any]:
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    task = _owned(hub, me, task_id)
    if task["status"] in TERMINAL_STATES:
        return {"task_id": task_id, "error": f"task already {task['status']}; result not changed"}
    if task["status"] == DELIVERED:
        raise ValueError(f"{task_id} is delivered and waits for {task['requester']} to accept it: withdraw_delivery "
                         "first to deliver anew")
    worker_of = _check_actor(hub, task)
    body = result_body(status, summary, outputs=outputs, evidence=evidence, limitations=limitations,
                       follow_up=follow_up, how=how, notes=notes)
    body["title"] = letters.title("delivery", {}, task["request"].get("objective"))
    if next:
        body["next"] = next
    refs = [ArtifactRef.from_dict(a) for a in artifacts or []]
    if worker_of == task_id:
        # Inside a daemon-run task: store a draft; the daemon sends it when the process exits.
        hub.ledger.update_task(task_id, "owner", result_draft={**body, "artifacts": [r.to_dict() for r in refs]})
        return {"task_id": task_id, "recorded": True, "status": status,
                "note": "result will be delivered when this run ends"}
    await hub.finish(task_id, body, refs)
    return {"task_id": task_id, "delivered": True, "status": status}


async def ask_question(hub: Hub, me: str, task_id: str, question: str, next: str | None = None) -> dict[str, Any]:
    delivery = await hub.reply(me, task_id, "QUESTION", {"question": question, **({"next": next} if next else {}),
                                                         "title": letters.title("question", {}, _original(hub, task_id))})
    return {"task_id": task_id, "delivery": delivery}


CONTROLS = ("pause", "resume", "interrupt")


async def control_task(hub: Hub, me: str, task_id: str, action: str, message: str) -> dict[str, Any]:
    """Pause, resume or interrupt one task (D-089): an UPDATE about it to its owner, which affects that task only.
    From its requester it goes the usual way; from anyone else (a trusted controller such as the secretary, on a
    task other posts asked for) to the owner the shared task record names. The owner's node decides: pause and
    resume for the requester or a trusted controller, interrupt for a trusted controller only; anything else
    from a stranger is ignored there."""
    if action not in CONTROLS:
        raise ValueError(f"action={action!r}: one of {', '.join(CONTROLS)}")
    addr, _ = hub.local_agent(me)
    body = {"message": message, action: True,
            "title": letters.title("control", {"action": action}, _original(hub, task_id))}
    asked = hub.ledger.task(task_id, "requester")
    if asked and asked["local_agent"] == str(addr):
        delivery = await hub.reply(me, task_id, "UPDATE", body)
    else:
        record = await hub._remote_task(task_id, None) if hub.bus else None
        if not record:
            raise KeyError(f"unknown task {task_id}: no shared task record names its owner")
        delivery = await hub.send(Envelope(type="UPDATE", sender=str(addr), to=record["owner"], task_id=task_id,
                                           body=body))
    return {"task_id": task_id, "action": action, "delivery": delivery}


async def quota_waits(hub: Hub, me: str) -> list[dict[str, Any]]:
    """Every open task, on any node, that waits for the account's usage limit to come back (D-104): from the shared
    task records (all of them for a coordinator such as the secretary)."""
    return [r for r in await hub.all_tasks(limit=10000, viewer=me)
            if r.get("wait_reason") == "quota" and r.get("status") not in TERMINAL_STATES]


async def resume_quota_waits(hub: Hub, me: str, message: str = "the usage limit is back: carry on") -> dict[str, Any]:
    """Resume every task that waits for quota (control_task resume to each owner; the owner's node decides)."""
    resumed = []
    for record in await quota_waits(hub, me):
        await control_task(hub, me, record["task_id"], "resume", message)
        resumed.append(record["task_id"])
    return {"resumed": resumed}


def _original(hub: Hub, task_id: str) -> str | None:
    """The title of the task a letter is about: its objective."""
    task = hub.ledger.task(task_id)
    return ((task or {}).get("request") or {}).get("objective")


async def send_notice(hub: Hub, me: str, to: str, text: str, *, priority: str = "normal",
                      kind: str = "notice") -> dict[str, Any]:
    """告知 (D-109): a notice that needs no reply (it closes once read). kind: notice, or patrol (巡查汇总)."""
    body = request_body(text, letters.TEMPLATES[kind]["name"], reply="none")
    body["title"] = letters.title(kind, {"text": text})
    return await _send_request(hub, me, to, body, artifacts=None, parent_task=None, priority=priority, project=None)


async def send_data(hub: Hub, me: str, to: str, artifacts: list[dict], note: str,
                    task_id: str | None = None) -> dict[str, Any]:
    """数据 (D-109): artifacts with a note on what they are and where they go. On a task: an UPDATE to its other
    party; else a notice carrying them."""
    if not task_id:
        body = request_body(note, letters.TEMPLATES["data"]["name"], reply="none")
        body["title"] = letters.title("data", {"note": note})
        return await _send_request(hub, me, to, body, artifacts=artifacts, parent_task=None, priority="normal",
                                   project=None)
    body = {"message": note, "data": True, "title": letters.title("data", {"note": note})}
    delivery = await hub.reply(me, task_id, "UPDATE", body, [ArtifactRef.from_dict(a) for a in artifacts],
                               to=await hub.resolve(to))
    return {"task_id": task_id, "delivery": delivery}


async def chase_task(hub: Hub, me: str, task_id: str, message: str | None = None) -> dict[str, Any]:
    """催交 (D-109): the requester asks the owner where its task stands; it names the owner next (wakes it)."""
    addr, _ = hub.local_agent(me)
    task = hub.ledger.task(task_id, "requester")
    if task is None or task["local_agent"] != str(addr):
        raise PermissionError(f"{addr} did not request {task_id}: only its requester chases it")
    body = {"message": message or "where does this stand? an eta or the result, please", "next": task["owner"],
            "title": letters.title("chase", {}, _original(hub, task_id))}
    return {"task_id": task_id, "delivery": await hub.reply(me, task_id, "UPDATE", body)}


async def accept_delivery(hub: Hub, me: str, task_id: str, verdict: str, reason: str | None = None) -> dict[str, Any]:
    """验收 (D-109): the requester of a delivered task says whether it got what it asked for. pass: done (a complete
    result only); reject: back to its owner, with the reason; close: a partial or failed result ends as failed
    (such a result never counts as done). The verdict goes to the owner and stays on the task's record."""
    if verdict not in ACCEPTANCE_VERDICTS:
        raise ValueError(f"verdict={verdict!r}: one of {', '.join(ACCEPTANCE_VERDICTS)}")
    addr, _ = hub.local_agent(me)
    task = hub.ledger.task(task_id, "requester")
    if task is None or task["local_agent"] != str(addr):
        raise PermissionError(f"{addr} did not request {task_id}: only its requester accepts its delivery")
    if task["status"] != DELIVERED:
        raise ValueError(f"{task_id} is {task['status']}: there is no delivery waiting for acceptance")
    complete = task.get("result_status") == "complete"
    if verdict == "pass" and not complete:
        raise ValueError(f"{task_id} delivered a {task.get('result_status')} result: only a complete one can pass; "
                         "reject it (with a reason) or close it as failed")
    if verdict != "pass" and not reason:
        raise ValueError(f"verdict {verdict} needs a reason")
    if verdict == "close" and complete:
        raise ValueError(f"{task_id} delivered a complete result: pass it, or reject it with a reason")
    body = {"message": f"{verdict}" + (f": {reason}" if reason else ""), "acceptance": verdict,
            **({"reason": reason} if reason else {}),
            **({"next": task["owner"]} if verdict == "reject" else {}),       # the owner carries on: wake it
            "title": letters.title("acceptance", {"verdict": verdict}, _original(hub, task_id))}
    delivery = await hub.reply(me, task_id, "UPDATE", body)
    status = {"pass": "COMPLETED", "close": "FAILED", "reject": "ACCEPTED"}[verdict]
    hub.ledger.update_task(task_id, "requester", status=status)
    return {"task_id": task_id, "verdict": verdict, "status": status, "delivery": delivery}


async def withdraw_delivery(hub: Hub, me: str, task_id: str, reason: str) -> dict[str, Any]:
    """撤回 (D-109): the owner takes back a delivery its requester has not accepted yet; the task is running again."""
    task = _owned(hub, me, task_id)
    if task["status"] != DELIVERED:
        raise ValueError(f"{task_id} is {task['status']}: no delivery waiting for acceptance to withdraw")
    _check_actor(hub, task)
    body = {"state": "RUNNING", "message": f"delivery withdrawn: {reason}", "withdraw_delivery": True,
            "title": letters.title("withdrawal", {}, _original(hub, task_id))}
    ok = await hub.owner_transition(task_id, "RUNNING", body["message"], body=body)
    hub.ledger.update_task(task_id, "owner", result=None, result_status=None)
    return {"task_id": task_id, "withdrawn": ok}


async def answer(hub: Hub, me: str, task_id: str, text: str, next: str | None = None) -> dict[str, Any]:
    delivery = await hub.reply(me, task_id, "ANSWER", {"answer": text, **({"next": next} if next else {}),
                                                       "title": letters.title("answer", {}, _original(hub, task_id))})
    return {"task_id": task_id, "delivery": delivery}


async def report_activity(hub: Hub, me: str, activity: str) -> dict[str, Any]:
    """D-108: the session says it is at work (busy: it started on a prompt) or done (idle: it stopped). Its hooks
    call it; the card shows the post working while busy, with or without a mutmuas task."""
    if activity not in ("busy", "idle"):
        raise ValueError(f"activity={activity!r}: busy or idle")
    addr, _ = hub.local_agent(me)
    if not hub.ledger.set_activity(str(addr), activity):
        raise KeyError(f"{addr} has no session on this node: nothing to mark {activity}")
    return {"address": str(addr), "activity": activity}


async def set_session_taking_work(hub: Hub, me: str, on: bool) -> dict[str, Any]:
    """`mutmuas <post> off|on`: the session stays online but takes no new work; the worker does. Only a post with
    a worker can hand its work over."""
    addr, agent = hub.local_agent(me)
    if not on and not agent.auto_worker:
        raise PermissionError(f"{addr} has no worker (auto_worker) to take the work: its session stays on")
    if not hub.ledger.set_session_accepting(str(addr), on):
        raise KeyError(f"{addr} has no session on this node")
    return {"address": str(addr), "session_takes_work": on}


async def push_due(hub: Hub, me: str, cursor: int | None) -> tuple[list[dict[str, Any]], int]:
    """What the session's MCP server pushes now (spec v1.1 §5.1): when it starts (cursor None) every unread
    message that needs the session, pushed before or not; afterwards each new one once. Not again and again in
    between: a busy session is not interrupted (§5.4); the Stop hook has it look before it ends a turn.
    Records each push (count, time). Returns (messages, next cursor)."""
    if cursor is None:
        # every unread one up to now, oldest first, page by page (not just the newest page); the live cursor starts
        # at the boundary only once all before it are covered
        boundary, rows, since = hub.ledger.last_rowid(), [], 0
        while page := [r for r in await inbox(hub, me, peek=True, types=WAKE, since=str(since), limit=200,
                                              show=False) if (r["seq"] or 0) <= boundary]:
            rows += page
            since = page[-1]["seq"]
        cursor = boundary
    else:
        rows = await inbox(hub, me, peek=True, types=WAKE, since=str(cursor), limit=20, show=False)
        cursor = max([cursor, *(r["seq"] or cursor for r in rows)])
    hub.ledger.mark_pushed([r["message_id"] for r in rows])
    return rows, cursor


async def cancel_children(hub: Hub, task_id: str, reason: str) -> list[str]:
    """A cancelled task's open child tasks are withdrawn too (D-066); their own nodes cascade further down."""
    cancelled = []
    for child in hub.ledger.children(task_id):
        if child["status"] not in TERMINAL_STATES:
            await cancel_task(hub, child["local_agent"], child["task_id"], f"parent {task_id} cancelled: {reason}")
            cancelled.append(child["task_id"])
    return cancelled


async def cancel_task(hub: Hub, me: str, task_id: str, reason: str = "") -> dict[str, Any]:
    task = hub.ledger.task(task_id, "requester")
    if task is None:
        raise KeyError(f"{task_id} was not requested from this node")
    delivery = await hub.reply(me, task_id, "CANCEL", {"reason": reason} if reason else {})
    # The requester has withdrawn; don't keep waiting on an owner that may never answer
    # (offline for good, or never received the REQUEST).
    hub.ledger.update_task(task_id, "requester", status="CANCELLED")
    return {"task_id": task_id, "delivery": delivery}


async def publish_artifact(hub: Hub, me: str, path: str, *, key: str | None = None, id: str = "",
                           description: str = "", backend: str = "object",
                           task_id: str | None = None) -> dict[str, Any]:
    addr, agent = hub.local_agent(me)
    if not agent.has("PUBLISH_ARTIFACT"):
        raise PermissionError(f"{addr} lacks PUBLISH_ARTIFACT permission")
    src = Path(path).expanduser()
    if not src.is_absolute():
        src = (Path.cwd() / src)
    if not key:
        # The key is shared metadata (object-store listings), so by default it names no local file: a random id,
        # keeping only a short extension so a fetched copy still opens with the right tool. Pass key= to publish
        # under a readable name on purpose.
        import secrets
        suffix = src.suffix if src.is_file() and len(src.suffix) <= 8 and src.suffix[1:].isalnum() else ""
        key = f"{addr.node}/{addr.agent}/{task_id or _current_task() or 'adhoc'}/{secrets.token_hex(8)}{suffix}"
    ref = await hub.artifacts.publish(src, key, id=id, description=description, backend=backend)
    hub.ledger.record_published(ref.uri, str(addr))
    return ref.to_dict()


async def add_observer(hub: Hub, me: str, task_id: str, observer: str) -> dict[str, Any]:
    """Let someone else read a task I take part in (requester, owner or observer): they get copies of its
    REQUEST and RESULT, and the other participants are told so that later RESULTs reach them too."""
    addr, _ = hub.local_agent(me)
    me_s = str(addr)
    if me_s not in acl(hub.ledger, task_id):
        raise PermissionError(f"{me_s} does not take part in {task_id}; only its participants can add observers")
    observer = str(Address.parse(observer))
    hub.ledger.add_observers(task_id, [observer])
    rows = [r for r in (hub.ledger.task(task_id, role) for role in
                        ("requester", "owner", f"observer:{me_s}")) if r]
    base = rows[0]
    copies = []
    if me_s in (base["requester"], base["owner"]):
        copies = await send_observer_copies(hub, me_s, task_id, [observer])
    # An observer does not send copies itself: a node that never saw the task could only check the sender against the
    # task record, which names requester and owner. The owner relays them when it hears of the new observer below.
    for other in sorted({base["requester"], base["owner"]} - {me_s}):
        # so the requester's side forwards a later RESULT, and every side knows the ACL
        await hub.send(Envelope(type="UPDATE", sender=me_s, to=other, task_id=task_id,
                                body={"message": f"{me_s} added observer {observer}", "fyi": True,
                                      "observers_add": [observer]}))
    out = {"task_id": task_id, "observer": observer, "copies_sent": copies}
    if not copies and me_s not in (base["requester"], base["owner"]):
        out["copies_relayed_by"] = base["owner"]
    return out


async def send_observer_copies(hub: Hub, sender: str, task_id: str, observers: list[str]) -> list[str]:
    """Copies of a task's REQUEST and (if there is one) RESULT for new observers, from one of its parties."""
    rows = [r for r in (hub.ledger.task(task_id, role) for role in ("requester", "owner")) if r]
    if not rows:
        return []
    base, sent = rows[0], []
    request = next((r["request"] for r in rows if r.get("request")), None)
    result = next((r["result"] for r in rows if r.get("result")), None)
    for kind, body, frm, to in (("REQUEST", request, base["requester"], base["owner"]),
                                ("RESULT", result, base["owner"], base["requester"])):
        if body:
            await hub.copy_to_observers(sender, Envelope(type=kind, sender=frm, to=to, task_id=task_id,
                                                         body=body), observers)
            sent.append(kind)
    return sent


async def list_tasks(hub: Hub, me: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Tasks on this node's ledger that `me` takes part in (not every agent's on this machine)."""
    viewer = str(hub.local_agent(me)[0])
    return [t for t in hub.ledger.tasks(limit=None)
            if is_participant(hub.ledger, viewer, t["task_id"])][:limit]


async def whoami(hub: Hub, me: str | None = None) -> dict[str, Any]:
    """My own details, read from this node's ledger: private, never on the shared card."""
    addr, agent = hub.local_agent(me)
    open_tasks = hub.ledger.tasks(role="owner", local_agent=str(addr),
                                  statuses=OPEN_STATES)
    out = {"address": str(addr), "project": hub.cfg.project, "role": agent.role, "mode": agent.mode,
           "workdir": str(agent.workdir_path), "permissions": agent.permissions,
           "capabilities": agent.capabilities, "open_tasks": [t["task_id"] for t in open_tasks],
           "inbox_unread": hub.ledger.unseen_count(str(addr)),
           **_push_fields(hub.ledger.push_state(str(addr))),
           "coordinator": str(addr) in (hub.cfg.coordinators or [])}
    if agent.mode == "interactive":
        out.update(node.session_fields(hub.ledger.session_of(str(addr))))
    out["jobs_waiting"] = [{k: j[k] for k in ("job_id", "task_id", "pid", "done_file", "log", "note", "created_at")}
                           for j in hub.ledger.jobs(owner=str(addr))]
    if agent.auto_worker:
        out["auto_worker"] = True
        out["worker_running"] = [_worker_run(hub, t, agent) for t in hub.ledger.tasks(
            role="owner", local_agent=str(addr), statuses=("ACCEPTED", "RUNNING")) if t.get("runner") == "worker"]
    return out


def _push_fields(state: dict[str, Any]) -> dict[str, Any]:
    """How long the oldest unread message has waited, and when mail was last pushed (spec v1.1 §5.1)."""
    out: dict[str, Any] = {"last_push_at": state["last_push_at"]}
    if state["oldest_unread_at"]:
        out["oldest_unread_s"] = round((datetime.now(timezone.utc)
                                        - parse_iso(state["oldest_unread_at"])).total_seconds())
    return out


def _worker_run(hub: Hub, task: dict[str, Any], agent) -> dict[str, Any]:
    """What the session sees of a task the worker is doing: which, since when, and when it ends at the latest."""
    timeout = float((task.get("request") or {}).get("timeout_s") or agent.task_timeout_s)
    since = parse_iso(task["updated_at"])
    return {"task_id": task["task_id"], "status": task["status"], "requester": task["requester"],
            "objective": short((task.get("request") or {}).get("objective")), "since": task["updated_at"],
            "latest_end": (since + timedelta(seconds=timeout)).isoformat(timespec="seconds")}


async def list_artifacts(hub: Hub, me: str | None = None) -> list[dict[str, Any]]:
    """Artifacts `me` published or was sent (visibility.py), not the whole object store."""
    viewer = str(hub.local_agent(me)[0])
    return [a for a in await hub.artifacts.list() if artifact_visible(hub.ledger, viewer, a["uri"])]


async def fetch_artifact(hub: Hub, uri: str, dest_dir: str | None = None, sha256: str | None = None,
                         me: str | None = None) -> dict:
    if not artifact_visible(hub.ledger, str(hub.local_agent(me)[0]), uri):
        return {"uri": uri, "error": "not visible to you: only its publisher and those it was sent to may fetch it"}
    dest = dest_dir or os.path.join(os.getcwd(), "mutmuas_artifacts")
    ref = ArtifactRef(uri=uri, sha256=sha256)
    path = await hub.artifacts.fetch(ref, dest)
    return {"uri": uri, "path": str(path), "size": path.stat().st_size if path.is_file() else None}


def _check_actor(hub: Hub, task: dict[str, Any]) -> str | None:
    """Every owner-side change (accept, reject, progress, result) is made by whoever holds the task, so a task is
    never done twice and a running worker is not interrupted. The caller is a worker when the daemon started it
    for a task that is still running (node.worker_task), else the session (D-102).
    - A worker acts on its own task only.
    - A task held by the worker is changed only by its worker.
    - A task held by the session is not changed by a worker.
    Returns the task the caller is the worker of (None: not a worker)."""
    worker_of = node.worker_task(hub.ledger, task["owner"])
    if worker_of and task["task_id"] != worker_of:
        raise PermissionError(f"a worker process (task {worker_of}) acts only on its own task, not on "
                              f"{task['task_id']}, which belongs to the session or to another run")
    if task.get("runner") == "worker" and worker_of != task["task_id"]:
        raise PermissionError(f"{task['task_id']} is being done by the worker the daemon started; it is not "
                              "interrupted (D-032a): wait for its result (whoami: worker_running)")
    if task.get("runner") == "session" and worker_of == task["task_id"]:
        raise PermissionError(f"{task['task_id']} was taken by the session; a worker may not act on it")
    return worker_of


def _owned(hub: Hub, me: str, task_id: str) -> dict[str, Any]:
    addr, _ = hub.local_agent(me)
    task = hub.ledger.task(task_id, "owner")
    if task is None or task["owner"] != str(addr):
        raise PermissionError(f"{task_id} is not owned by {addr}")
    return task


