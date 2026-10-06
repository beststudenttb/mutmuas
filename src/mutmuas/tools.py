"""Agent-facing operations, shared by the MCP server and the CLI.

Each function takes an open Hub and the acting agent's address and returns
plain JSON-able dicts, so the same behaviour is reachable from Claude Code,
Codex (via MCP) and from scripts/humans (via ``agentctl``).
"""

from __future__ import annotations

import asyncio
import functools
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .hub import Hub
from .ids import Address, parse_iso
from .visibility import acl, artifact_visible, is_participant, message_visible, short
from .protocol import OPEN_STATES, TERMINAL_STATES, ArtifactRef, Envelope, request_body, result_body


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
                       leader: bool = False, project: str | None = None, internal: bool = False,
                       model: str | None = None, depends_on: list[str] | None = None) -> dict[str, Any]:
    default_deadline = None
    after_s = None
    if depends_on and deadline and deadline.startswith("+"):
        _deadline(deadline)                                       # checked now, counted from the release
        after_s, deadline = _interval_s(deadline), None
    deadline = _deadline(deadline)
    if not deadline and reply != "none" and not depends_on:      # a held request gets it when it is sent
        default_deadline = default_reply_deadline(hub, timeout_s, kind)
    body = request_body(objective, reason, kind=kind, inputs=inputs, expected_outputs=expected_outputs,
                        constraints=constraints, acceptance_criteria=acceptance_criteria,
                        deadline=deadline or default_deadline, timeout_s=timeout_s, reply=reply, observers=observers,
                        deadline_default=bool(default_deadline),  # the owner can tell it from a chosen one
                        leader=leader)
    if after_s:
        body["deadline_after_s"] = after_s        # held (depends_on): the deadline is set on release
    return await _send_request(hub, me, to, body, artifacts=artifacts, parent_task=parent_task, priority=priority,
                               project=project, internal=internal, model=model, depends_on=depends_on)


LONG_KINDS = ("experiment", "code")       # their default deadline is long_reply_deadline_s (D-098)


def _deadline(text: str | None) -> str | None:
    """A deadline as given: an ISO time with timezone, or relative (+30m, +2h, +1d); one already past is refused
    (D-098: it used to be taken and reported overdue at once)."""
    if not text:
        return None
    now = datetime.now(timezone.utc)
    if text.startswith("+") and (delay := _interval_s(text)) is not None:
        if delay <= 0:
            raise ValueError(f"deadline={text!r}: a relative deadline must be ahead (+30m, +2h, +1d)")
        return (now + timedelta(seconds=delay)).isoformat(timespec="seconds")
    hint = f"deadline={text!r}: use an ISO time with timezone (e.g. 2026-10-07T18:00:00+09:00) or +30m / +2h / +1d"
    try:
        due = parse_iso(text)
    except ValueError:
        raise ValueError(hint) from None
    if due.tzinfo is None:
        raise ValueError(hint)
    if due <= now:
        raise ValueError(f"deadline {text} is in the past (now {now.isoformat(timespec='seconds')}); {hint}")
    return text


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
                        project, internal, model, depends_on) -> dict[str, Any]:
    default_deadline = body.get("deadline") if body.get("deadline_default") else None
    if internal:
        # D-073: a brain's own long work, run by its worker (never the session); sent to itself, in its project
        if await hub.resolve(to) != str(hub.local_agent(me)[0]):
            raise ValueError("an internal subtask is sent to yourself (to = your own address)")
        body["internal"] = True
        if model:
            body["model"] = model
        parent = parent_task or _current_task()
        owner = hub.ledger.task(parent, "owner") if parent else None
        if owner:
            # a sub belongs to its parent's project
            agent = hub.local_agent(me)[1]
            inherited = agent.project_of(owner.get("request"))
            if project and project != inherited:
                raise ValueError(f"an internal subtask belongs to its parent's project ({inherited}), "
                                 f"not {project}")
            project = inherited
    if project:
        body["project"] = project                    # D-069: the recipient routes it to that project's directory
    if depends_on:
        unknown = [d for d in depends_on if hub.ledger.task(d) is None]
        if unknown:
            raise ValueError(f"depends_on names tasks this node does not know: {', '.join(unknown)}")
        sender = str(hub.local_agent(me)[0])
        foreign = [d for d in depends_on if not is_participant(hub.ledger, sender, d)]
        if foreign:                                  # their results travel with it: only one's own tasks
            raise PermissionError(f"depends_on may name only tasks {sender} takes part in, not {', '.join(foreign)}")
        body["depends_on"] = list(depends_on)       # held here until they are done (D-073 batch 2)
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
        parent_task=parent_task or _current_task(), priority=priority, hold_for=depends_on)
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
    from .node import session_alive
    addr, agent = hub.local_agent(me)
    session = hub.ledger.session_of(str(addr))
    alive = session and session_alive(session)
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
                     limit: int = 50) -> dict[str, Any]:
    """inbox() for a session looking at its mail, plus what the page left out (D-074): how many unread the cursor
    covers, how many it lists, how many older ones it did not, and `next`, the cursor for the following page.
    Paging has two stages: the leader's mail first (leader_before_seq), then the other
    mail (before_seq), each newest to oldest, so every message is listed exactly once."""
    addr, _ = hub.local_agent(me)
    next_to, own_results = _wake_view(hub, me, types)
    count = functools.partial(hub.ledger.unseen_count, str(addr), types, next_to=next_to, own_results=own_results,
                              project=_project_view(hub, me))        # the same view the listing has (D-072)
    total = count(before_seq=before_seq, leader_before_seq=leader_before_seq)
    rows = await inbox(hub, me, limit=limit, peek=peek, types=types, before_seq=before_seq,
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


def _interval_s(text: str) -> float | None:
    """'30s' / '10m' / '5h' / '1d' (a leading + is allowed) in seconds; None if it is not one."""
    text = text.lstrip("+")
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in units and text[:-1].replace(".", "", 1).isdigit():
        return float(text[:-1]) * units[text[-1]]
    return None


async def remind_me(hub: Hub, me: str, at: str, text: str, every: str | None = None,
                    task_id: str | None = None) -> dict[str, Any]:
    """The node puts `text` into the agent's inbox at `at` (ISO time with timezone, or +30s/+10m/+2h) as a message
    that hands the agent the baton, so it wakes a session and waits in the inbox while
    none runs (D-066). every='5h' repeats it at that interval until cancel_reminder. task_id: the task a worker run
    sets it for: a post with no session gets that task run again when it fires (D-098)."""
    addr, _ = hub.local_agent(me)
    every_s = None
    if every is not None:
        every_s = _interval_s(every)
        if not every_s:
            raise ValueError(f"every={every!r}: use an interval like 30m, 5h or 1d")
    if (delay := _interval_s(at)) is not None and at.startswith("+"):
        due = datetime.now(timezone.utc) + timedelta(seconds=delay)
    else:
        hint = f"at={at!r}: use an ISO time with timezone (e.g. 2026-09-25T18:00:00+09:00) or +30s / +10m / +2h"
        try:
            due = parse_iso(at)
        except ValueError:
            raise ValueError(hint) from None
        if due.tzinfo is None:
            raise ValueError(hint)
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


def _eta(eta: str | None) -> str | None:
    """An eta is an ISO time with timezone (D-076)."""
    if eta is None:
        return None
    try:
        when = parse_iso(eta)
    except (TypeError, ValueError):
        when = None
    if when is None or when.tzinfo is None:
        raise ValueError(f"eta={eta!r}: use an ISO time with timezone, e.g. 2026-10-03T09:00:00+00:00")
    return eta


async def accept_task(hub: Hub, me: str, task_id: str, eta: str | None = None) -> dict[str, Any]:
    eta = _eta(eta)
    if _check_actor(hub, _owned(hub, me, task_id)) == task_id:
        # A worker's task was accepted for it when the daemon started it: nothing to do, and no session claim
        return {"task_id": task_id, "accepted": True, "note": "already accepted for you when the worker started"}
    refused = hub.ledger.claim_task(task_id, "session", OPEN_STATES)
    if refused and "worker" in refused:
        raise PermissionError(f"{task_id} is being done by the worker the daemon started before this session; "
                              "it is not interrupted (D-032a): wait for its result (whoami: worker_running)")
    body = {"state": "RUNNING", "message": f"accepted by {me}" + (f"; eta {eta}" if eta else "")}
    if eta:
        body["eta"] = eta                         # the requester's node chases it once it passes (D-076)
        hub.ledger.update_task(task_id, "owner", eta=eta)
    ok = await hub.owner_transition(task_id, "RUNNING", body["message"], msg_type="ACK", body=body)
    return {"task_id": task_id, "accepted": ok, **({"eta": eta} if eta else {})}


async def add_job(hub: Hub, me: str, task_id: str | None = None, pid: int | None = None,
                  done_file: str | None = None, log: str | None = None, note: str | None = None,
                  children: bool = False) -> dict[str, Any]:
    """Register a background job (e.g. training) the task waits on (D-050). The task becomes WAITING; a worker may
    then exit without a result, and is not treated as failed. The node's heartbeat notices when the job ends
    (process pid gone, or done_file appears) and wakes the post: a worker's task is queued again, a session gets
    a wake-up. No time limit. children=True (D-066): wait on the task's direct child tasks instead (sent with
    parent_task); it ends when each has a result, was refused or cancelled, or is past its deadline."""
    from .node import proc_start
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    if pid is None and not done_file and not children:
        raise ValueError("give pid or done_file: how the node tells that the job has ended")
    task = _owned(hub, me, task_id)
    _check_actor(hub, task)
    if children and not hub.ledger.children(task_id):
        raise ValueError(f"{task_id} has no child task to wait on: send them with parent_task={task_id} first")
    job_id = hub.ledger.add_job(task_id, task["owner"], pid, proc_start(pid) if pid else None, done_file, log,
                                note, children=children)
    what = note or ("its child tasks" if children else f"pid {pid}" if pid else f"until {done_file}")
    await hub.owner_transition(task_id, "WAITING", f"waiting on a background job ({what}); resumes when it ends",
                               body={"state": "WAITING", "message": f"waiting on a background job ({what})"})
    return {"task_id": task_id, "job_id": job_id, "state": "WAITING"}


async def reject_task(hub: Hub, me: str, task_id: str, reason: str) -> dict[str, Any]:
    _check_actor(hub, _owned(hub, me, task_id))
    ok = await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT", body={"reason": reason})
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
    if next:
        body["next"] = next
    if (eta := _eta(eta)):
        body["eta"] = eta                         # a new estimate: what a chase asks for (D-076)
        hub.ledger.update_task(task_id, "owner", eta=eta)
    ok = await hub.owner_transition(task_id, new_state, message, msg_type=msg_type, body=body)
    if (new_state == "WAITING" and not any(j["children"] for j in hub.ledger.jobs(task_id))
            and any(c["status"] not in TERMINAL_STATES for c in hub.ledger.children(task_id))):
        # Waiting with open child tasks is waiting on them: the node wakes this task when they are done (D-066)
        hub.ledger.add_job(task_id, task["owner"], None, None, None, None, "child tasks", children=True)
    hub.mark_sub_on_plan(task_id, "!" if new_state == "BLOCKED" else ">", message if new_state == "BLOCKED" else None)
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
    _check_actor(hub, task)
    body = result_body(status, summary, outputs=outputs, evidence=evidence, limitations=limitations,
                       follow_up=follow_up, how=how, notes=notes)
    if next:
        body["next"] = next
    refs = [ArtifactRef.from_dict(a) for a in artifacts or []]
    if task_id == _current_task():
        # Inside a daemon-run task: store a draft; the daemon sends it when the process exits.
        hub.ledger.update_task(task_id, "owner", result_draft={**body, "artifacts": [r.to_dict() for r in refs]})
        return {"task_id": task_id, "recorded": True, "status": status,
                "note": "result will be delivered when this run ends"}
    await hub.finish(task_id, body, refs)
    return {"task_id": task_id, "delivered": True, "status": status}


async def ask_question(hub: Hub, me: str, task_id: str, question: str, next: str | None = None) -> dict[str, Any]:
    delivery = await hub.reply(me, task_id, "QUESTION", {"question": question, **({"next": next} if next else {})})
    return {"task_id": task_id, "delivery": delivery}


async def answer(hub: Hub, me: str, task_id: str, text: str, next: str | None = None) -> dict[str, Any]:
    delivery = await hub.reply(me, task_id, "ANSWER", {"answer": text, **({"next": next} if next else {})})
    return {"task_id": task_id, "delivery": delivery}


NUDGE_EVERY_S = 3 * 3600     # a task is nudged by hand at most this often (D-076 §5: "同一任务数小时一次")


async def nudge(hub: Hub, me: str, task_id: str, note: str = "") -> dict[str, Any]:
    """Wake the owner of a task of ours that seems stuck (D-073 batch 2): it must answer what it is doing, where
    it is stuck and a new eta. A stalled worker is laid out again by its node. At most once in NUDGE_EVERY_S."""
    task = hub.ledger.task(task_id, "requester")
    if task is None or task["local_agent"] != str(hub.local_agent(me)[0]):
        raise KeyError(f"{task_id} was not requested by {me} from this node")
    if task["status"] in TERMINAL_STATES:
        raise ValueError(f"{task_id} is {task['status']}: nothing to nudge")
    now = datetime.now(timezone.utc)
    if not hub.ledger.claim_nudge(task_id, now.isoformat(), (now - timedelta(seconds=NUDGE_EVERY_S)).isoformat()):
        # one compare-and-set in the ledger, so two nudges at once (or from two processes) send one
        raise PermissionError(f"{task_id} was nudged at {hub.ledger.task(task_id, 'requester')['nudged_at']}; "
                              f"at most once in {NUDGE_EVERY_S // 3600} h")
    text = (f"nudge from {me}: {note or 'how is it going?'} Reply with what you are doing, where you are stuck "
            "and a new eta (report_progress eta=…).")
    delivery = await hub.send(Envelope(type="UPDATE", sender=task["local_agent"], to=task["owner"], task_id=task_id,
                                       body={"message": text, "next": task["owner"], "nudge": True}))
    return {"task_id": task_id, "nudged": task["owner"], "delivery": delivery}


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
    if hub.ledger.drop_held(task_id):
        # never sent (it waited for its dependencies): nobody else knows it, so no CANCEL goes out
        hub.ledger.update_task(task_id, "requester", status="CANCELLED")
        return {"task_id": task_id, "delivery": "withdrawn before it was sent"}
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
    from .ids import Address
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
        try:          # so the requester's side forwards a later RESULT, and every side knows the ACL
            await hub.send(Envelope(type="UPDATE", sender=me_s, to=other, task_id=task_id,
                                    body={"message": f"{me_s} added observer {observer}", "fyi": True,
                                          "observers_add": [observer]}))
        except Exception:
            pass
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
    from .node import session_fields
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
        out.update(session_fields(hub.ledger.session_of(str(addr))))
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


async def history(hub: Hub, me: str | None = None, task_id: str | None = None, limit: int = 500) -> list[dict]:
    """Messages from the stream that `me` sent or received. Everyone else's mail is not for `me`."""
    viewer = str(hub.local_agent(me)[0])
    rows = []
    for _subject, data in await hub.bus.history(limit=limit):
        try:
            env = Envelope.from_json(data).to_dict()
        except Exception:
            continue
        if (task_id is None or env.get("task_id") == task_id) and message_visible(viewer, env):
            rows.append(env)
    return rows


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
    never done twice and a running worker is not interrupted.
    Who the caller is comes from the process tree: a process descending from a worker the daemon started (pid
    and start time recorded) is that worker, whatever its environment says. MUTMUAS_TASK_ID can only add a
    restriction (a process that claims to be a worker is treated as one), never prove anything.
    - A worker acts on its own task only.
    - A task held by the worker is changed only by that worker's processes.
    - A task held by the session is not changed by a worker.
    Returns the task the caller is the worker of (None: not a worker)."""
    from .node import _ancestors, worker_tasks_of
    proven = worker_tasks_of(hub.ledger, task["owner"], {os.getpid(), *_ancestors(os.getpid())})
    worker_of = next(iter(proven), None) or _current_task()
    if worker_of and task["task_id"] != worker_of:
        raise PermissionError(f"a worker process (task {worker_of}) acts only on its own task, not on "
                              f"{task['task_id']}, which belongs to the session or to another run")
    if task.get("runner") == "worker" and task["task_id"] not in proven:
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



def internal_caller(hub: Hub) -> str | None:
    """The internal subtask (D-073) whose worker the calling process descends from, if any. Decided from the
    process tree (the worker's pid and start time are recorded when the daemon starts it), as for the lease and
    _check_actor; MUTMUAS_TASK_ID can only add the restriction, never lift it."""
    from .node import _ancestors, worker_tasks_of
    chain = {os.getpid(), *_ancestors(os.getpid())}
    ids = {t for a in hub.cfg.agents for t in worker_tasks_of(hub.ledger, str(Address(hub.cfg.node, a.id)), chain)}
    if os.environ.get("MUTMUAS_TASK_ID"):
        ids.add(os.environ["MUTMUAS_TASK_ID"])
    for task_id in ids:
        task = hub.ledger.task(task_id, "owner")
        if ((task or {}).get("request") or {}).get("internal"):
            return task_id
    return None


def _not_for_subs(fn):
    """An internal subtask's worker sees no mail and sends none (D-073)."""
    @functools.wraps(fn)
    async def guarded(hub: Hub, *args, **kwargs):
        if sub := internal_caller(hub):
            raise PermissionError(f"{sub} is an internal subtask (D-073): its worker may only report progress, "
                                  "register a job and submit its result")
        return await fn(hub, *args, **kwargs)
    return guarded


for _name in ("send_request", "check_task", "wait_for_result", "inbox", "clear_inbox", "remind_me",
              "cancel_reminder", "accept_task", "ask_question", "answer", "cancel_task", "add_observer",
              "list_tasks", "history", "nudge"):
    globals()[_name] = _not_for_subs(globals()[_name])
