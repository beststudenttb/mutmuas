"""Agent-facing operations, shared by the MCP server and the CLI.

Each function takes an open Hub and the acting agent's address and returns
plain JSON-able dicts, so the same behaviour is reachable from Claude Code,
Codex (via MCP) and from scripts/humans (via ``agentctl``).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from .hub import Hub
from .visibility import artifact_visible, message_visible
from .protocol import TERMINAL_STATES, ArtifactRef, Envelope, request_body, result_body


def _current_task() -> str | None:
    return os.environ.get("MUTMUAS_TASK_ID") or None


def card_summary(card: dict[str, Any]) -> dict[str, Any]:
    keys = ("address", "display", "role", "mode", "runtime", "provider", "model", "capabilities", "permissions",
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


async def send_request(hub: Hub, me: str, to: str, objective: str, reason: str, *, kind: str = "query",
                       inputs: Any = None, expected_outputs: Any = None, constraints: Any = None,
                       acceptance_criteria: Any = None, timeout_s: float | None = None,
                       deadline: str | None = None, artifacts: list[dict] | None = None,
                       parent_task: str | None = None, priority: str = "normal",
                       reply: str | None = None, observers: list[str] | None = None) -> dict[str, Any]:
    body = request_body(objective, reason, kind=kind, inputs=inputs, expected_outputs=expected_outputs,
                        constraints=constraints, acceptance_criteria=acceptance_criteria,
                        deadline=deadline, timeout_s=timeout_s, reply=reply, observers=observers)
    target = await hub.card_or_none(to)
    task_id, delivery = await hub.request(
        me, to, body, artifacts=[ArtifactRef.from_dict(a) for a in artifacts or []],
        parent_task=parent_task or _current_task(), priority=priority)
    out = {"task_id": task_id, "to": await hub.resolve(to), "delivery": delivery}
    if delivery == "queued":
        out["note"] = "message bus unreachable; kept in the local outbox and sent automatically on reconnect"
    elif target is None:
        out["note"] = "target agent is not registered yet; the message is stored durably until it comes online"
    elif not target.get("online"):
        out["note"] = "target agent is offline; the message waits in its durable inbox"
    return out


def _brief(view: dict[str, Any]) -> dict[str, Any]:
    keys = ("task_id", "status", "result_status", "requester", "owner", "objective", "result", "output_refs",
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
# requests are not in it: they are read when I next look, or when I explicitly wait on that task.
WAKE = ("REQUEST", "QUESTION", "ANSWER", "BLOCKED", "REJECT", "CANCEL", "ERROR")


async def inbox(hub: Hub, me: str, include_seen: bool = False, limit: int = 50, peek: bool = False,
                wait_s: float | None = None, types: tuple[str, ...] | None = None,
                since: str | None = None) -> list[dict[str, Any]]:
    """Unread messages for ``me``. peek: do not mark them read. wait_s: block until one arrives (or timeout).
    types: only these message types (e.g. ACTIONABLE), for both waiting and listing.
    since: only messages that reached this node's ledger after this ISO timestamp (a notifier's cursor,
    so --peek does not report the same unread message again and again)."""
    addr, _ = hub.local_agent(me)
    # A message that hands me the baton (body.next == me) needs me as much as a REQUEST does.
    next_to = str(addr) if types == WAKE else None
    if wait_s and not include_seen:
        # Messages reach this node's ledger through the daemon, so waiting on the ledger is enough
        # (a second JetStream consumer on the same mailbox would split the messages).
        deadline = asyncio.get_running_loop().time() + wait_s
        while (hub.ledger.unseen_count(str(addr), types, since, next_to=next_to) == 0
               and asyncio.get_running_loop().time() < deadline):
            await asyncio.sleep(0.5)
    if include_seen:
        rows = hub.ledger.db.execute("SELECT envelope FROM messages WHERE direction='in' AND local_agent=?"
                                     " ORDER BY rowid DESC LIMIT ?", (str(addr), limit)).fetchall()
        envs = [Envelope.from_json(r["envelope"]) for r in rows]
    else:
        envs = hub.ledger.unseen(str(addr), limit, mark=not peek, types=types, since=since, next_to=next_to)
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
    return {"marked_read": hub.ledger.mark_seen_before(str(addr), int(before_seq)), "up_to_seq": int(before_seq)}


async def remind_me(hub: Hub, me: str, at: str, text: str) -> dict[str, Any]:
    """Have the session's mutmuas MCP process push `text` into the session at `at` (ISO time with timezone, or
    +10m/+2h). Needs a session running with the channel; a reminder due while no session runs fires at the next
    session start."""
    from datetime import datetime, timedelta, timezone

    from .ids import parse_iso
    addr, _ = hub.local_agent(me)
    units = {"m": "minutes", "h": "hours", "d": "days"}
    if at.startswith("+") and at[-1] in units and at[1:-1].replace(".", "", 1).isdigit():
        due = datetime.now(timezone.utc) + timedelta(**{units[at[-1]]: float(at[1:-1])})
    else:
        due = parse_iso(at)
        if due.tzinfo is None:
            raise ValueError("at needs a timezone (e.g. 2026-09-25T18:00:00+09:00) or +10m / +2h")
    due_iso = due.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return {"reminder": hub.ledger.add_reminder(str(addr), due_iso, text), "due": due_iso}


async def _read_receipts(hub: Hub, me: str, envs: list[Envelope]) -> None:
    """A REQUEST sent with reply: none is answered by being read: the session has now seen it, so close the
    task with a read receipt. Only the session reading its mail gets here (peek never does)."""
    for env in envs:
        if env.type != "REQUEST" or (env.body or {}).get("reply") != "none":
            continue
        task = hub.ledger.task(env.task_id, "owner")
        if task and task["owner"] == me and task["status"] not in TERMINAL_STATES:
            await hub.finish(env.task_id, result_body("complete", f"read by {me} (no reply requested)"))


async def accept_task(hub: Hub, me: str, task_id: str) -> dict[str, Any]:
    _owned(hub, me, task_id)
    ok = await hub.owner_transition(task_id, "RUNNING", f"accepted by {me}", msg_type="ACK",
                                    body={"state": "RUNNING", "message": f"accepted by {me}"})
    return {"task_id": task_id, "accepted": ok}


async def reject_task(hub: Hub, me: str, task_id: str, reason: str) -> dict[str, Any]:
    _owned(hub, me, task_id)
    ok = await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT", body={"reason": reason})
    return {"task_id": task_id, "rejected": ok}


async def report_progress(hub: Hub, me: str, message: str, task_id: str | None = None,
                          state: str | None = None, next: str | None = None) -> dict[str, Any]:
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    task = _owned(hub, me, task_id)
    new_state = state or task["status"]
    if new_state not in ("RUNNING", "WAITING", "BLOCKED"):
        new_state = "RUNNING"
    msg_type = "BLOCKED" if new_state == "BLOCKED" else "UPDATE"
    body = {"reason": message} if msg_type == "BLOCKED" else {"state": new_state, "message": message}
    if next:
        body["next"] = next
    ok = await hub.owner_transition(task_id, new_state, message, msg_type=msg_type, body=body)
    return {"task_id": task_id, "state": new_state, "sent": ok}


async def submit_result(hub: Hub, me: str, status: str, summary: str, *, task_id: str | None = None,
                        outputs: Any = None, artifacts: list[dict] | None = None, evidence: Any = None,
                        limitations: Any = None, follow_up: Any = None, next: str | None = None) -> dict[str, Any]:
    task_id = task_id or _current_task()
    if not task_id:
        raise ValueError("task_id is required outside of a delegated task")
    task = _owned(hub, me, task_id)
    if task["status"] in TERMINAL_STATES:
        return {"task_id": task_id, "error": f"task already {task['status']}; result not changed"}
    body = result_body(status, summary, outputs=outputs, evidence=evidence, limitations=limitations,
                       follow_up=follow_up)
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
    key = key or f"{addr.node}/{addr.agent}/{task_id or _current_task() or 'adhoc'}/{src.name}"
    ref = await hub.artifacts.publish(src, key, id=id, description=description, backend=backend)
    return ref.to_dict()


async def add_observer(hub: Hub, me: str, task_id: str, observer: str) -> dict[str, Any]:
    """Let someone else read a task I take part in: they get copies of its REQUEST and RESULT. On the
    requester's side they also get the RESULT when it arrives later."""
    from .ids import Address
    addr, _ = hub.local_agent(me)
    task = hub.ledger.db.execute("SELECT * FROM tasks WHERE task_id=? AND local_agent=?",
                                 (task_id, str(addr))).fetchone()
    if task is None:
        raise PermissionError(f"{addr} does not take part in {task_id}; only its requester or owner can add observers")
    observer = str(Address.parse(observer))
    task = hub.ledger.task(task_id, task["role"])
    request = dict(task.get("request") or {})
    request["observers"] = sorted(set(request.get("observers") or []) | {observer})
    hub.ledger.update_task(task_id, task["role"], request=request, force=True)
    sent = []
    for row in hub.ledger.thread(task_id):
        if row.get("type") in ("REQUEST", "RESULT"):
            env = Envelope.from_dict({k: v for k, v in row.items() if k not in ("direction", "delivery", "error")})
            await hub.copy_to_observers(str(addr), env, [observer])
            sent.append(row["type"])
    return {"task_id": task_id, "observer": observer, "copies_sent": sent}


async def whoami(hub: Hub, me: str | None = None) -> dict[str, Any]:
    """My own details, read from this node's ledger: private, never on the shared card."""
    from .node import session_fields
    addr, agent = hub.local_agent(me)
    open_tasks = hub.ledger.tasks(role="owner", local_agent=str(addr),
                                  statuses=("PENDING", "ACCEPTED", "RUNNING", "WAITING", "BLOCKED"))
    out = {"address": str(addr), "project": hub.cfg.project, "role": agent.role, "mode": agent.mode,
           "workdir": str(agent.workdir_path), "permissions": agent.permissions,
           "capabilities": agent.capabilities, "open_tasks": [t["task_id"] for t in open_tasks],
           "inbox_unread": hub.ledger.unseen_count(str(addr)),
           "coordinator": str(addr) in (hub.cfg.coordinators or [])}
    if agent.mode == "interactive":
        out.update(session_fields(hub.ledger.session_of(str(addr)), agent.workdir_path))
    return out


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


def _owned(hub: Hub, me: str, task_id: str) -> dict[str, Any]:
    addr, _ = hub.local_agent(me)
    task = hub.ledger.task(task_id, "owner")
    if task is None or task["owner"] != str(addr):
        raise PermissionError(f"{task_id} is not owned by {addr}")
    return task

