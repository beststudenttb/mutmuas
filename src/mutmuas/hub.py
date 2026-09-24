"""Hub: the node-local service layer shared by the daemon, the CLI and the MCP server.

It owns the three stores of a node — the bus (NATS), the local ledger (SQLite)
and the artifact store — and implements the operations agents need:
registry lookup, sending (outbox first), task views, owner-side task state.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .artifacts import ArtifactStore
from .bus import Bus, BusUnavailable
from .config import AgentConfig, NodeConfig
from .ids import Address, new_task_id, parse_iso
from .ledger import Ledger
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
                   reconnect: bool = True) -> Hub:
        ledger = Ledger(cfg.db_path)
        try:
            bus = await Bus.open(cfg.nats, cfg.project, f"mutmuas:{cfg.node}:{client_name}",
                                 cfg.message_retention_days, reconnect=reconnect)
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
        cards.sort(key=lambda c: (not c["online"], c.get("state") != "idle", c.get("queue", 0), c["address"]))
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
        return env.task_id, await self.send(env)

    async def reply(self, local: str, task_id: str, type: str, body: dict[str, Any] | None = None,
                    artifacts: list[ArtifactRef] | None = None) -> str:
        """Send a message about an existing task to the other party."""
        addr, _ = self.local_agent(local)
        task = self.ledger.task(task_id)
        if task is None:
            raise KeyError(f"unknown task {task_id}")
        peer = task["requester"] if task["owner"] == str(addr) else task["owner"]
        env = Envelope(type=type, sender=str(addr), to=peer, body=body or {}, task_id=task_id,
                       conversation_id=task["conversation_id"], artifacts=artifacts or [],
                       reply_to=task.get("last_message"))
        return await self.send(env)

    # ---- task views ---------------------------------------------------

    async def task_view(self, task_id: str) -> dict[str, Any] | None:
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
            for key in ("requester", "owner", "parent_task", "request", "created_at"):
                view.setdefault(key, local.get(key))
            # Prefer whichever side saw the more recent change.
            if not remote or (local["updated_at"] >= remote.get("updated_at", "")):
                for key in ("status", "result_status", "result", "output_refs", "updated_at"):
                    if local.get(key) not in (None, [], ""):
                        view[key] = local[key]
            view["thread"] = self.ledger.thread(task_id)
            view["local_role"] = local["role"]
        return view

    async def _remote_task(self, task_id: str, owner: str | None) -> dict[str, Any] | None:
        bus = self._require_bus()
        if owner:
            return await bus.kv_get(bus.names.tasks_kv, bus.names.task_key(Address.parse(owner), task_id))
        keys = await bus.kv_keys(bus.names.tasks_kv, [f"*.*.{task_id}"])
        return await bus.kv_get(bus.names.tasks_kv, keys[0]) if keys else None

    async def all_tasks(self, limit: int = 100) -> list[dict[str, Any]]:
        """Global task list from the shared KV (every owner publishes its tasks there)."""
        bus = self._require_bus()
        records = list((await bus.kv_all(bus.names.tasks_kv)).values())
        records.sort(key=lambda r: r.get("created_at", ""), reverse=True)
        return records[:limit]

    async def wait_result(self, task_id: str, timeout: float = 600, poll: float = 0.5) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            view = await self.task_view(task_id)
            if view is None:
                raise KeyError(f"unknown task {task_id}")
            if view.get("status") in TERMINAL_STATES:
                return view
            if asyncio.get_running_loop().time() >= deadline:
                view["timed_out_waiting"] = True
                return view
            await asyncio.sleep(poll)

    # ---- owner side ---------------------------------------------------

    async def publish_task_record(self, task_id: str) -> None:
        """Mirror an owned task into the shared task KV so any node can observe it."""
        task = self.ledger.task(task_id, "owner")
        if task is None or self.bus is None:
            return
        record = {k: task[k] for k in ("task_id", "requester", "owner", "parent_task", "status", "result_status",
                                       "created_at", "updated_at", "attempts", "input_refs", "output_refs")}
        request = task.get("request") or {}
        record.update(objective=request.get("objective"), reason=request.get("reason"), kind=request.get("kind"),
                      result=task.get("result"))
        record["thread"] = [{k: m.get(k) for k in ("message_id", "type", "from", "to", "timestamp")}
                            | {"note": _note(m)} for m in self.ledger.thread(task_id)]
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
        if not self.ledger.update_task(task_id, "owner", status=status):
            return False
        if notify:
            payload = body if body is not None else {"state": status, "message": message}
            await self.reply(task["owner"], task_id, msg_type, payload)
        await self.publish_task_record(task_id)
        log.info("task %s -> %s (%s)", task_id, status, message)
        return True

    async def finish(self, task_id: str, result: dict[str, Any], artifacts: list[ArtifactRef] | None = None) -> bool:
        """Send the RESULT for an owned task and close it. Refuses to finish a task twice."""
        task = self.ledger.task(task_id, "owner")
        if task is None:
            raise KeyError(f"task {task_id} is not owned by this node")
        env = Envelope(type="RESULT", sender=task["owner"], to=task["requester"], body=result, task_id=task_id,
                       conversation_id=task["conversation_id"], artifacts=artifacts or [],
                       reply_to=task.get("last_message"))
        env.validate()
        refs = [a.to_dict() for a in env.artifacts]
        if not self.ledger.update_task(task_id, "owner", status=task_state_for_result(result["status"]),
                                       result=result, result_status=result["status"], output_refs=refs):
            return False
        await self.send(env)
        await self.publish_task_record(task_id)
        log.info("task %s finished: %s", task_id, result["status"])
        return True


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

