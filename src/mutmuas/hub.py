"""Hub: the node-local service layer shared by the daemon, the CLI and the MCP server.

It owns the three stores of a node — the bus (NATS), the local ledger (SQLite)
and the artifact store — and implements the operations agents need:
registry lookup, sending (outbox first), task views, owner-side task state.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import re
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from .artifacts import ArtifactStore
from .bus import Bus, BusUnavailable
from .config import AgentConfig, NodeConfig
from .ids import Address, new_task_id, parse_iso
from .ledger import Ledger
from .visibility import acl, is_coordinator, is_participant, status_layer
from .protocol import (REQUEST_KINDS, TERMINAL_STATES, ArtifactRef, Envelope, ProtocolError,
                       task_state_for_result)

log = logging.getLogger(__name__)


class PermissionDenied(PermissionError):
    pass


class Hub:
    def __init__(self, cfg: NodeConfig, bus: Bus | None, ledger: Ledger):
        self.cfg = cfg
        self.bus = bus
        self.ledger = ledger
        self.artifacts = ArtifactStore(bus, cfg.project, cfg.node, cfg.artifact_max_mb)

    @classmethod
    async def open(cls, cfg: NodeConfig, client_name: str, *, require_bus: bool = True,
                   reconnect: bool = True, initial_connect_attempts: int | None = None) -> Hub:
        ledger = Ledger(cfg.db_path)
        try:
            bus = await Bus.open(cfg.nats, cfg.project, f"mutmuas:{cfg.node}:{client_name}",
                                 cfg.message_retention_days, reconnect=reconnect,
                                 initial_connect_attempts=initial_connect_attempts)
        except BusUnavailable:
            if require_bus:
                ledger.close()
                raise
            log.warning("bus unavailable; working from the local ledger only")
            bus = None
        return cls(cfg, bus, ledger)

    async def close(self) -> None:
        if self.bus:
            await self.bus.close()
        self.ledger.close()

    # ---- identity -----------------------------------------------------

    def local_agent(self, who: str | None) -> tuple[Address, AgentConfig]:
        """Resolve the acting agent. Only agents configured on *this* node can act from it."""
        if who is None:
            if not self.cfg.agents:
                raise PermissionDenied(f"no agents configured on node {self.cfg.node}")
            interactive = [a for a in self.cfg.agents if a.mode == "interactive"]
            agent = (interactive or self.cfg.agents)[0]
            return Address(self.cfg.node, agent.id), agent
        addr = Address.parse(who) if ":" in who else Address(self.cfg.node, who)
        if addr.node != self.cfg.node:
            raise PermissionDenied(f"{addr} is not on this node ({self.cfg.node}); cannot act as it")
        return addr, self.cfg.agent(addr.agent)

    # ---- registry -----------------------------------------------------

    def _require_bus(self) -> Bus:
        if self.bus is None:
            raise BusUnavailable("not connected to NATS")
        return self.bus

    async def agents(self, capability: str | None = None, online_only: bool = False) -> list[dict[str, Any]]:
        bus = self._require_bus()
        cards = list((await bus.kv_all(bus.names.agents_kv)).values())
        for card in cards:
            card["online"] = is_online(card)
        if capability:
            cap = capability.lower()
            cards = [c for c in cards
                     if cap in [x.lower() for x in c.get("capabilities", [])] or cap == c.get("role", "").lower()]
        if online_only:
            cards = [c for c in cards if c["online"]]
        # Best candidates first: online, idle, short queue.
        cards.sort(key=lambda c: (not c["online"], c.get("availability") == "busy", c.get("state") != "idle",
                                  c["address"]))
        return cards

    async def agent_card(self, address: str) -> dict[str, Any] | None:
        bus = self._require_bus()
        addr = Address.parse(address)
        card = await bus.kv_get(bus.names.agents_kv, f"{addr.node}.{addr.agent}")
        if card:
            card["online"] = is_online(card)
        return card

    def _bus_up(self) -> bool:
        return self.bus is not None and self.bus.connected

    async def card_or_none(self, target: str) -> dict[str, Any] | None:
        """Registry lookup that never blocks sending: None if unknown *or* the registry is unreachable."""
        if not self._bus_up():
            return None
        try:
            return await self.agent_card(await self.resolve(target))
        except Exception as e:
            log.debug("registry lookup failed: %r", e)
            return None

    async def resolve(self, target: str) -> str:
        """Accept 'B:representation' or a display alias like 'B:a1'. Best effort when offline."""
        addr = Address.parse(target)
        if not self._bus_up():
            return str(addr)
        try:
            if await self.agent_card(str(addr)):
                return str(addr)
            for card in await self.agents():
                if card.get("display") == target:
                    return card["address"]
        except Exception as e:
            log.debug("alias resolution failed: %r", e)
        return str(addr)   # unknown yet: the durable stream still keeps the message for it

    async def nodes(self) -> list[dict[str, Any]]:
        bus = self._require_bus()
        nodes = list((await bus.kv_all(bus.names.nodes_kv)).values())
        for n in nodes:
            n["online"] = is_online(n)
        return sorted(nodes, key=lambda n: n["node"])

    # ---- sending ------------------------------------------------------

    async def send(self, env: Envelope) -> str:
        """Outbox first, then try to publish. Returns 'sent' or 'queued' (daemon will retry)."""
        env.validate()
        self.ledger.queue_outgoing(env)
        return await self.try_publish(env)

    async def try_publish(self, env: Envelope) -> str:
        if self.bus is None or not self.bus.connected:
            return "queued"
        try:
            await self.bus.publish(env)
        except Exception as e:
            self.ledger.mark_send_error(env.message_id, repr(e))
            log.warning("publish failed for %s (kept in outbox): %r", env.short(), e)
            return "queued"
        self.ledger.mark_sent(env.message_id)
        log.info("sent %s", env.short())
        return "sent"

    async def flush_outbox(self) -> int:
        sent = 0
        for env in self.ledger.outbox():
            if await self.try_publish(env) != "sent":
                break
            sent += 1
        return sent

    async def request(self, sender: str | None, to: str, body: dict[str, Any], *,
                      artifacts: list[ArtifactRef] | None = None, parent_task: str | None = None,
                      priority: str = "normal", task_id: str | None = None) -> tuple[str, str]:
        """Send a REQUEST. Returns (task_id, delivery)."""
        addr, agent = self.local_agent(sender)
        if not agent.has("REQUEST_TASK"):
            raise PermissionDenied(f"{addr} lacks REQUEST_TASK permission")
        if body.get("kind", "query") not in REQUEST_KINDS:
            raise ProtocolError(f"kind must be one of {sorted(REQUEST_KINDS)}")
        if parent_task:
            body = {**body, "parent_task": parent_task}
        env = Envelope(type="REQUEST", sender=str(addr), to=await self.resolve(to), body=body,
                       task_id=task_id or new_task_id(), priority=priority, artifacts=artifacts or [])
        delivery = await self.send(env)
        await self.copy_to_observers(str(addr), env, body.get("observers") or [])
        return env.task_id, delivery

    async def copy_to_observers(self, sender: str, original: Envelope, observers: list[str]) -> None:
        """Observers read a task's content through copies of its REQUEST and RESULT (they are participants,
        visibility.py). Each copy carries the task's participants, so the observer's node can check that it
        came from one of them. FYI only: it never wakes them."""
        people = sorted(acl(self.ledger, original.task_id) | {original.sender, original.to, *observers})
        if original.type == "REQUEST" and observers:
            # the record goes out before the copies, so an observer's node can check a copy as it arrives
            await self.publish_task_record(original.task_id, role="requester")
        for observer in observers:
            if observer in (original.sender, original.to):
                continue
            try:
                await self.send(Envelope(
                    type="UPDATE", sender=sender, to=observer, task_id=original.task_id,
                    conversation_id=original.conversation_id, artifacts=original.artifacts, body={
                        "message": f"observer copy: {original.type} {original.sender} -> {original.to}",
                        "fyi": True, "participants": people,
                        "copy_of": {"type": original.type, "from": original.sender, "to": original.to,
                                    "body": original.body}}))
            except Exception as e:
                log.warning("copy to observer %s failed: %r", observer, e)

    async def reply(self, local: str, task_id: str, type: str, body: dict[str, Any] | None = None,
                    artifacts: list[ArtifactRef] | None = None, to: str | None = None) -> str:
        """Send a message about an existing task to the other party. Only its requester or owner may.
        to: the addressee the caller meant; refused unless it is that other party."""
        addr, _ = self.local_agent(local)
        task = next((t for t in (self.ledger.task(task_id, r) for r in ("requester", "owner"))
                     if t and t["local_agent"] == str(addr)), None)
        if task is None:
            if self.ledger.task(task_id) is None:
                raise KeyError(f"unknown task {task_id}")
            raise PermissionDenied(f"{addr} is not the requester or owner of {task_id}")
        peer = task["requester"] if task["owner"] == str(addr) else task["owner"]
        if to is not None and to != peer:
            raise PermissionDenied(f"messages on {task_id} go to {peer}, not {to}")
        env = Envelope(type=type, sender=str(addr), to=peer, body=body or {}, task_id=task_id,
                       conversation_id=task["conversation_id"], artifacts=artifacts or [],
                       reply_to=task.get("last_message"))
        return await self.send(env)

    # ---- task views ---------------------------------------------------

    async def task_view(self, task_id: str, viewer: str | None = None) -> dict[str, Any] | None:
        """What `viewer` may see of a task: everything for a participant, the status layer for a coordinator,
        nothing for anyone else (None, as if unknown). viewer None: this node's default agent."""
        viewer = str(self.local_agent(viewer)[0])
        view = await self._task_view(task_id)
        if view is None:
            return None
        if is_participant(self.ledger, viewer, task_id, view):
            return view
        if is_coordinator(self.cfg, viewer):
            return status_layer(view) | {"visibility": "status only (coordinator)"}
        return None

    async def _task_view(self, task_id: str) -> dict[str, Any] | None:
        """Merge the local ledger with the owner's published record (authoritative for owner state)."""
        local = self.ledger.task(task_id)
        remote = None
        if self.bus is not None:
            try:
                remote = await self._remote_task(task_id, local["owner"] if local else None)
            except Exception as e:
                log.debug("remote task lookup failed: %r", e)
        if local is None and remote is None:
            return None
        view: dict[str, Any] = dict(remote or {})
        if local:
            view.setdefault("task_id", task_id)
            for key in ("requester", "owner", "parent_task", "request", "created_at", "eta"):
                view.setdefault(key, local.get(key))
            # Prefer whichever side saw the more recent change.
            if not remote or (local["updated_at"] >= remote.get("updated_at", "")):
                for key in ("status", "result_status", "result", "output_refs", "updated_at"):
                    if local.get(key) not in (None, [], ""):
                        view[key] = local[key]
            # The owner's record carries no result; a result this ledger holds is never dropped because the
            # record is newer (it is republished).
            for key in ("result_status", "result", "output_refs"):
                if view.get(key) in (None, [], "") and local.get(key) not in (None, [], ""):
                    view[key] = local[key]
            view["thread"] = self.ledger.thread(task_id)
            view["local_role"] = local["role"]
            view["local_status"] = local["status"]      # the snapshot wait_result decides on
        return view

    async def _remote_task(self, task_id: str, owner: str | None) -> dict[str, Any] | None:
        bus = self._require_bus()
        if owner:
            return await bus.kv_get(bus.names.tasks_kv, bus.names.task_key(Address.parse(owner), task_id))
        keys = await bus.kv_keys(bus.names.tasks_kv, [f"*.*.{task_id}"])
        return await bus.kv_get(bus.names.tasks_kv, keys[0]) if keys else None

    async def all_tasks(self, limit: int = 100, viewer: str | None = None) -> list[dict[str, Any]]:
        """Network task list from the shared KV (status layer only): all of it for a coordinator, the tasks
        `viewer` takes part in for everyone else."""
        viewer = str(self.local_agent(viewer)[0])
        bus = self._require_bus()
        records = [status_layer(r) for r in (await bus.kv_all(bus.names.tasks_kv)).values()]
        if not is_coordinator(self.cfg, viewer):
            records = [r for r in records if is_participant(self.ledger, viewer, r["task_id"], r)]
        records.sort(key=lambda r: r.get("created_at", ""), reverse=True)
        return records[:limit]

    async def wait_result(self, task_id: str, timeout: float = 600, poll: float = 0.5,
                          viewer: str | None = None) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            view = await self.task_view(task_id, viewer)
            if view is None:
                raise KeyError(f"unknown task {task_id}")
            if view.get("status") in TERMINAL_STATES and self._closed_here(task_id, view):
                return view
            if asyncio.get_running_loop().time() >= deadline:
                view["timed_out_waiting"] = True
                return view
            await asyncio.sleep(poll)

    def _closed_here(self, task_id: str, view: dict[str, Any]) -> bool:
        """The requester's own ledger has handled the message that closed the task (RESULT, REJECT, CANCEL).
        The owner's KV record can arrive first, and returning then gives a finished task without its result
        (found through the v4 grace test on B, 8de9448). Others (observers, coordinators) only get the record.
        Decided on the snapshot the view was built from, not on a second read: the RESULT could be handled in
        between and the stale view returned (717d7e0 on B)."""
        if view.get("local_role") != "requester":
            return True
        return view.get("local_status") in TERMINAL_STATES

    # ---- owner side ---------------------------------------------------

    async def publish_task_record(self, task_id: str, role: str = "owner") -> None:
        """Mirror a task into the shared task KV so any node can observe it: the owner's node keeps it current;
        the requester's node writes the first one when it sends copies to observers (D-102)."""
        task = self.ledger.task(task_id, role)
        if task is None or self.bus is None:
            return
        # Only the status layer goes to the shared KV (readable by every node): no reason, inputs, thread,
        # result or artifact references. Content stays with the participants (visibility.py).
        record = status_layer(task)
        try:
            await self.bus.kv_put(self.bus.names.tasks_kv,
                                  self.bus.names.task_key(Address.parse(task["owner"]), task_id), record)
        except Exception as e:
            log.warning("could not publish task record %s: %r", task_id, e)

    async def owner_transition(self, task_id: str, status: str, message: str, *, notify: bool = True,
                               msg_type: str = "UPDATE", body: dict[str, Any] | None = None) -> bool:
        """Change an owned task's state, tell the requester, mirror to KV."""
        task = self.ledger.task(task_id, "owner")
        if task is None:
            raise KeyError(f"task {task_id} is not owned by this node")
        env = None
        if notify:
            payload = body if body is not None else {"state": status, "message": message}
            env = Envelope(type=msg_type, sender=task["owner"], to=task["requester"], body=payload, task_id=task_id,
                           conversation_id=task["conversation_id"], reply_to=task.get("last_message")).validate()
        if not self.ledger.update_task(task_id, "owner", status=status, queue=env):
            return False
        if env is not None:
            await self.try_publish(env)
        await self.publish_task_record(task_id)
        if status in TERMINAL_STATES:
            self.drop_inbox_line(task_id)                    # refused or withdrawn: off the 收件 section
        log.info("task %s -> %s (%s)", task_id, status, message)
        return True

    def add_inbox_line(self, task: dict[str, Any]) -> None:
        """Arriving work goes on the post's plan, in the 收件 section (spec v1.1 §3.1): one unchecked line naming
        the task; the agent moves it into its plan when it takes the work, and delivery removes it."""
        request = task.get("request") or {}
        agent = self.local_agent(task["owner"])[1]
        board = agent.home(agent.project_of(request)) / "PLAN.md"
        first = " ".join(str(request.get("objective") or "").splitlines()[:1])[:100]
        line = f"- [ ] {task['task_id']} from {task['requester']}: {first}"
        board.parent.mkdir(parents=True, exist_ok=True)
        rewrite_plan(board, lambda text: add_to_section(text, INBOX_SECTION, line), create=True)

    def drop_inbox_line(self, task_id: str) -> None:
        task = self.ledger.task(task_id, "owner")
        if not task:
            return
        agent = self.local_agent(task["owner"])[1]
        board = agent.home(agent.project_of(task.get("request"))) / "PLAN.md"
        if board.is_file():
            rewrite_plan(board, lambda text: drop_section_line(text, INBOX_SECTION, task_id))

    async def finish(self, task_id: str, result: dict[str, Any], artifacts: list[ArtifactRef] | None = None,
                     record: bool = True) -> bool:
        """Send the RESULT for an owned task and close it. Refuses to finish a task twice.
        record (D-052): the mechanical part of delivery is done here, not by the model: this task's PLAN.md section
        goes into outputs.plan (and off the board), and the post's worker-log.md gets its R7.11 line. Off for
        notices closed by being read."""
        task = self.ledger.task(task_id, "owner")
        if task is None:
            raise KeyError(f"task {task_id} is not owned by this node")
        if task["status"] in TERMINAL_STATES:
            return False                                         # finished already: the board is not touched
        # an invalid RESULT is refused before anything changes, the board included
        Envelope(type="RESULT", sender=task["owner"], to=task["requester"], body=result, task_id=task_id,
                 artifacts=artifacts or []).validate()
        agent = self.local_agent(task["owner"])[1]
        workdir = agent.home(agent.project_of(task.get("request"))) if record else None   # its project (D-069)
        board = workdir / "PLAN.md" if workdir else None
        taken: list[str] = []

        def take(text: str) -> str:
            # this task's section goes into the result and off the board in one locked rewrite, so a line the node adds
            # meanwhile (收件) is not overwritten
            found = plan_section(text, task_id)
            if not found:
                return text
            taken.append(found)
            return drop_plan_section(text, task_id)
        if board and board.is_file():
            rewrite_plan(board, take)
        section = taken[0] if taken else None
        if section:
            outputs = result.get("outputs")
            outputs = outputs if isinstance(outputs, dict) else {"value": outputs} if outputs else {}
            result = {**result, "outputs": {**outputs, "plan": section}}
        env = Envelope(type="RESULT", sender=task["owner"], to=task["requester"], body=result, task_id=task_id,
                       conversation_id=task["conversation_id"], artifacts=artifacts or [],
                       reply_to=task.get("last_message"))
        env.validate()
        refs = [a.to_dict() for a in env.artifacts]
        if not self.ledger.update_task(task_id, "owner", status=task_state_for_result(result["status"]),
                                       result=result, result_status=result["status"], output_refs=refs, queue=env):
            return False
        await self.try_publish(env)
        await self.publish_task_record(task_id)
        log.info("task %s finished: %s", task_id, result["status"])
        self.drop_inbox_line(task_id)
        if workdir:
            workdir.mkdir(parents=True, exist_ok=True)          # as a run does; a post may not have run yet
            with open(workdir / "worker-log.md", "a") as f:
                f.write(log_line(task, result) + "\n")
        return True


INBOX_SECTION = "## 收件"


def rewrite_plan(board: Path, change, create: bool = False) -> None:
    """Rewrite PLAN.md with change(text): one writer at a time over the whole read-modify-write (a cross-process
    lock), through a temporary file of its own."""
    with open(board.with_name(".PLAN.md.lock"), "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not board.is_file() and not create:
            return
        text = board.read_text() if board.is_file() else "# PLAN\n"
        changed = change(text)
        if changed != text:
            fd, tmp = tempfile.mkstemp(dir=board.parent, prefix=".PLAN.md.", suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                f.write(changed)
            Path(tmp).replace(board)


def add_to_section(text: str, heading: str, line: str) -> str:
    """Append line at the end of the section with this heading, creating the section at the end if missing."""
    lines = text.rstrip("\n").split("\n")
    if heading not in lines:
        return "\n".join(lines + ["", heading, line]) + "\n"
    i = lines.index(heading) + 1
    while i < len(lines) and not _HEADING.match(lines[i]):
        i += 1
    while i > lines.index(heading) + 1 and not lines[i - 1].strip():
        i -= 1
    return "\n".join(lines[:i] + [line] + lines[i:]) + "\n"


def drop_section_line(text: str, heading: str, task_id: str) -> str:
    """Remove the first line of that section naming task_id."""
    lines = text.split("\n")
    if heading not in lines:
        return text
    i = lines.index(heading) + 1
    pattern = re.compile(rf"\b{re.escape(task_id)}\b")
    while i < len(lines) and not _HEADING.match(lines[i]):
        if pattern.search(lines[i]):
            return "\n".join(lines[:i] + lines[i + 1:])
        i += 1
    return text


def mark_plan_line(text: str, task_id: str, marker: str, note: str | None = None) -> str:
    """The first checklist line naming task_id gets the marker ([>] [x] [!] ...) and, if given, a note."""
    pattern = re.compile(rf"^(\s*[-*]\s*)\[[ >xw!]\](.*\b{re.escape(task_id)}\b.*?)(\s+— .*)?$", re.M)
    note = " ".join(str(note).split())[:200] if note else ""
    return pattern.sub(lambda m: f"{m.group(1)}[{marker}]{m.group(2)}" + (f" — {note}" if note else ""), text,
                       count=1)


_HEADING = re.compile(r" {0,3}(#{1,6})(?:\s|$)")
_FENCE = re.compile(r" {0,3}(```|~~~)")


def _plan_span(lines: list[str], task_id: str) -> tuple[int, int] | None:
    """Where a task's section of a PLAN.md is: from the first ATX heading (up to 3 spaces in) that names the
    whole task id, down to the next heading of the same or a higher level. Lines inside fenced code blocks are
    never headings (a pasted command's '# comment'). Nothing is guessed: no such heading, no section."""
    named = re.compile(rf"(?<![\w-]){re.escape(task_id)}(?![\w-])")
    start, level, fence = None, 0, None
    for i, line in enumerate(lines):
        mark = _FENCE.match(line)
        if fence:
            fence = None if mark and mark.group(1) == fence else fence
            continue
        if mark:
            fence = mark.group(1)
            continue
        heading = _HEADING.match(line)
        if not heading:
            continue
        if start is None and named.search(line):
            start, level = i, len(heading.group(1))
        elif start is not None and len(heading.group(1)) <= level:
            return start, i
    return (start, len(lines)) if start is not None else None


def plan_section(board: str, task_id: str) -> str | None:
    lines = board.splitlines(keepends=True)
    span = _plan_span(lines, task_id)
    return "".join(lines[span[0]:span[1]]).rstrip("\n") + "\n" if span else None


def drop_plan_section(board: str, task_id: str) -> str:
    lines = board.splitlines(keepends=True)
    span = _plan_span(lines, task_id)
    return "".join(lines[:span[0]] + lines[span[1]:]) if span else board


def log_line(task: dict[str, Any], result: dict[str, Any]) -> str:
    """handbook R7.11: time | from | task | output / to whom | how | notes (one line; '|' inside a field -> '/')."""
    def field(text: Any, n: int = 200) -> str:
        text = " ".join(str(text or "").replace("|", "/").split())
        return text[:n] + ("…" if len(text) > n else "")
    objective = (task.get("request") or {}).get("objective")
    return " | ".join([
        datetime.now().astimezone().strftime("%Y-%m-%d %H:%M"), task["requester"],
        f"{task['task_id']} {field(objective, 60)}".rstrip(),
        f"{result['status']}: {field(result.get('summary'), 100)} -> {task['requester']}",
        field(result.get("how")) or "未填", field(result.get("notes")) or "未填"])


def is_online(card: dict[str, Any]) -> bool:
    if card.get("state") == "offline":
        return False
    try:
        age = (datetime.now(timezone.utc) - parse_iso(card["last_heartbeat"])).total_seconds()
    except (KeyError, ValueError):
        return False
    return age < 3 * float(card.get("heartbeat_s", 5)) + 2


def _note(m: dict[str, Any]) -> str:
    body = m.get("body") or {}
    for key in ("objective", "summary", "message", "reason", "question", "answer"):
        if body.get(key):
            return str(body[key])[:200]
    return ""
