"""agent-node: the per-machine daemon.

Loops (all asyncio tasks):
  * receiver    per agent: pull from the durable JetStream mailbox, commit to the
                local ledger, *then* ack. Redelivery is harmless (dedup on message_id).
  * dispatcher  per agent: handle committed messages in order (REQUEST -> task, replies
                -> requester-side task state).
  * runners     per worker agent: execute accepted tasks via the agent's runtime,
                with timeout / cancel / retry-after-crash.
  * heartbeat   publish node + agent cards (presence, current task, queue, inbox).
  * outbox      retry messages that could not be published (network down).

Crash safety: state lives in SQLite + JetStream, never only in memory. On
start, ``recover()`` re-queues every owned task that was not finished.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import platform
import socket
from pathlib import Path
from typing import Any

from nats.errors import TimeoutError as NatsTimeoutError

from . import __version__
from .bus import Names
from .config import AgentConfig, NodeConfig
from .hub import Hub
from .ids import Address, now_iso
from .protocol import (REQUEST_KINDS, TERMINAL_STATES, ArtifactRef, Envelope, ProtocolError, result_body,
                       task_state_for_result)
from .runtime import TaskContext, make_runtime
from .worktree import GitError, Worktree

log = logging.getLogger(__name__)


def _code_version() -> str:
    """The git commit this daemon runs from (what 'same version on every node' is checked against)."""
    import subprocess
    try:
        out = subprocess.run(["git", "-C", str(Path(__file__).resolve().parent), "describe", "--always", "--dirty"],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or __version__
    except Exception:
        return __version__

# How a reply received by the *requester* moves its local task state.
REQUESTER_TRANSITIONS = {"ACK": "ACCEPTED", "BLOCKED": "BLOCKED", "QUESTION": "WAITING", "REJECT": "FAILED",
                         "ERROR": "FAILED"}


class NodeDaemon:
    def __init__(self, cfg: NodeConfig):
        self.cfg = cfg
        self.hub: Hub | None = None
        self._tasks: list[asyncio.Task] = []
        self._wake: dict[str, asyncio.Event] = {}
        self._queues: dict[str, asyncio.Queue[str]] = {}
        self._queued: dict[str, set[str]] = {}               # task ids queued or running, per agent
        self._running: dict[str, asyncio.Task] = {}          # task_id -> runner task
        self._cancel_requested: set[str] = set()
        self._outbox_wake = asyncio.Event()
        self.started = asyncio.Event()
        self.code_version = _code_version()
        self._stopping = False

    # ---- lifecycle ----------------------------------------------------

    async def start(self) -> None:
        self.hub = await self._connect()
        hub = self.hub
        for agent in self.cfg.agents:
            addr = str(Address(self.cfg.node, agent.id))
            self._wake[addr] = asyncio.Event()
            self._queued[addr] = set()
            sub = await hub.bus.ensure_inbox(Address(self.cfg.node, agent.id))
            self._spawn(self._receiver(addr, sub), f"recv:{addr}")
            self._spawn(self._dispatcher(agent, addr), f"dispatch:{addr}")
            if agent.mode == "worker":
                self._queues[addr] = asyncio.Queue()
                for i in range(max(1, agent.max_concurrent)):
                    self._spawn(self._runner(agent, addr), f"run:{addr}:{i}")
        self._spawn(self._heartbeat(), "heartbeat")
        self._spawn(self._outbox_loop(), "outbox")
        await self.recover()
        await self._publish_cards()
        await self._forget_removed_agents()
        self.started.set()
        log.info("node %s up: project=%s agents=%s", self.cfg.node, self.cfg.project,
                 [a.id for a in self.cfg.agents])

    async def _connect(self) -> Hub:
        delay = 1.0
        while True:
            try:
                return await Hub.open(self.cfg, "node")
            except Exception as e:
                log.warning("cannot connect to NATS (%s); retrying in %.0fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def stop(self) -> None:
        """Graceful stop: running tasks are interrupted and resumed by recover() next start."""
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.hub:
            with contextlib.suppress(Exception):
                await self._publish_cards(state_override="offline")
            await self.hub.close()
        log.info("node %s stopped", self.cfg.node)

    async def run_forever(self) -> None:
        await self.start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.stop()

    def _spawn(self, coro, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(self._on_loop_done)
        self._tasks.append(task)

    def _on_loop_done(self, task: asyncio.Task) -> None:
        if task.cancelled() or self._stopping:
            return
        if task.exception():
            log.error("loop %s crashed: %r", task.get_name(), task.exception())

    async def _forget_removed_agents(self) -> None:
        """Agents deleted from the config (e.g. renamed) must not linger in the registry as ghosts."""
        bus = self.hub.bus
        configured = {a.id for a in self.cfg.agents}
        for key in await bus.kv_keys(bus.names.agents_kv, [f"{self.cfg.node}.*"]):
            agent_id = key.split(".", 1)[1]
            if agent_id not in configured:
                outcome = await bus.remove_agent(Address(self.cfg.node, agent_id))
                log.warning("agent %s:%s is no longer configured: %s", self.cfg.node, agent_id, outcome)

    # ---- recovery -----------------------------------------------------

    async def recover(self) -> None:
        hub = self.hub
        # every open task, not just the newest page (A:codex: a backlog > 200 left old tasks stuck)
        for task in hub.ledger.tasks(role="owner", statuses=("PENDING", "ACCEPTED", "RUNNING"), limit=None):
            agent = self._agent_cfg(task["owner"])
            if agent is None or agent.mode != "worker":
                continue
            if task["status"] == "RUNNING":
                await hub.owner_transition(task["task_id"], "ACCEPTED",
                                           f"node {self.cfg.node} restarted; task will be resumed")
            elif task["status"] == "PENDING":
                await self._accept(task["task_id"])
            self._enqueue(task["owner"], task["task_id"])
        for task in hub.ledger.tasks(role="owner", limit=500):
            await hub.publish_task_record(task["task_id"])
        await hub.flush_outbox()

    # ---- receive ------------------------------------------------------

    async def _receiver(self, addr: str, sub) -> None:
        while True:
            try:
                msgs = await sub.fetch(batch=16, timeout=1)
            except (NatsTimeoutError, asyncio.TimeoutError):
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("fetch on %s failed: %r", addr, e)
                await asyncio.sleep(1)
                continue
            for msg in msgs:
                await self._receive_one(addr, msg)

    async def _receive_one(self, addr: str, msg) -> None:
        hub = self.hub
        try:
            env = Envelope.from_json(msg.data)
        except ProtocolError as e:
            log.warning("invalid message on %s: %s", msg.subject, e)
            await self._error_back(msg.data, addr, e.code, str(e))
            await msg.term()
            return
        claimed_node = Names.sender_node_from_subject(msg.subject)
        if env.sender_addr.node != claimed_node or env.to != addr:
            log.warning("dropping spoofed/misrouted message %s (subject %s)", env.short(), msg.subject)
            await msg.term()
            return
        if hub.ledger.ingest(env):
            log.info("received %s", env.short())
            self._wake[addr].set()
        else:
            log.info("duplicate delivery ignored: %s", env.message_id)
        await msg.ack()

    async def _error_back(self, raw: bytes, addr: str, code: str, text: str) -> None:
        """Best effort: tell the sender its message was invalid, if we can tell who sent it."""
        try:
            d = json.loads(raw)
            sender = Address.parse(d["from"])
        except Exception:
            return
        env = Envelope(type="ERROR", sender=addr, to=str(sender), task_id=d.get("task_id"),
                       body={"code": code, "message": text}, reply_to=d.get("message_id"))
        await self.hub.send(env)

    # ---- dispatch -----------------------------------------------------

    async def _dispatcher(self, agent: AgentConfig, addr: str) -> None:
        wake = self._wake[addr]
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=2)
            wake.clear()
            for env in self.hub.ledger.unhandled(addr):
                try:
                    state = await self._handle(agent, env)
                    self.hub.ledger.mark_handled(env.message_id, state or "handled")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("handling %s failed", env.short())
                    self.hub.ledger.mark_handled(env.message_id, "dropped", repr(e))

    async def _handle(self, agent: AgentConfig, env: Envelope) -> str | None:
        if env.type == "REQUEST":
            return await self._on_request(agent, env)
        elif env.type == "CANCEL":
            return await self._on_cancel(env)
        elif env.type == "ANSWER":
            pass   # surfaced to the agent through its inbox (MCP/CLI); see technical debt in docs
        else:
            await self._on_reply(env)

    async def _on_request(self, agent: AgentConfig, env: Envelope) -> str | None:
        hub = self.hub
        existing = hub.ledger.task(env.task_id, "owner")
        if existing:
            if existing["status"] in TERMINAL_STATES and existing.get("result"):
                # Requester re-sent a finished task: repeat the answer, never re-execute.
                await hub.reply(env.to, env.task_id, "RESULT", existing["result"],
                                [ArtifactRef.from_dict(a) for a in existing["output_refs"]])
            return
        hub.ledger.create_owned_task(env)
        denial = self._check_policy(agent, env)
        if denial:
            await hub.owner_transition(env.task_id, "FAILED", denial, msg_type="REJECT",
                                       body={"reason": denial})
            return "rejected"
        await hub.publish_task_record(env.task_id)
        if agent.mode == "worker":
            await self._accept(env.task_id)
            self._enqueue(env.to, env.task_id)
            await self._notify(agent, env.to, env.task_id,
                               f"FYI: {env.to} accepted a {env.body.get('kind', 'query')} task from {env.sender}: "
                               f"{env.body.get('objective', '')[:300]}")
        else:
            # Interactive agents accept explicitly (accept_task). Tell the requester it arrived meanwhile,
            # so "delivered but not picked up yet" is distinguishable from "lost".
            await hub.reply(env.to, env.task_id, "UPDATE", {
                "state": "PENDING", "message": f"delivered to the inbox of {env.to}; waiting to be accepted"})

    def _check_policy(self, agent: AgentConfig, env: Envelope) -> str | None:
        if not agent.accepts(env.sender):
            return f"permission denied: {env.to} does not accept requests from {env.sender}"
        needed = REQUEST_KINDS[env.body.get("kind", "query")]
        if not agent.has(needed):
            return f"permission denied: {env.to} lacks {needed} required for kind={env.body.get('kind', 'query')}"
        return None

    async def _accept(self, task_id: str) -> None:
        await self.hub.owner_transition(task_id, "ACCEPTED", "accepted into queue", msg_type="ACK",
                                        body={"state": "ACCEPTED", "message": "accepted into queue"})

    async def _on_cancel(self, env: Envelope) -> str | None:
        task = self.hub.ledger.task(env.task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES:
            return None
        if task["status"] == "PENDING":
            # Withdrawn before anyone picked it up: close it and keep it out of the interactive inbox
            # (both the REQUEST and this CANCEL are noise to someone who never saw the request).
            await self.hub.owner_transition(env.task_id, "CANCELLED",
                                            env.body.get("reason") or "withdrawn before it was accepted")
            self.hub.ledger.mark_seen(env.task_id)
            return None
        self._cancel_requested.add(env.task_id)
        runner = self._running.get(env.task_id)
        if runner:
            runner.cancel()      # runner reports CANCELLED itself
        else:
            await self.hub.owner_transition(env.task_id, "CANCELLED",
                                            env.body.get("reason") or "cancelled by requester")

    async def _on_reply(self, env: Envelope) -> None:
        """A message about a task *we* requested."""
        ledger = self.hub.ledger
        task = ledger.task(env.task_id, "requester")
        if task is None or task["local_agent"] != env.to or env.body.get("fyi"):
            # Not a reply to this agent's own request: e.g. an FYI copy to a node lead about a task that
            # another agent on the same node requested. It stays in the recipient's inbox only.
            log.info("message about task %s (%s) kept in inbox only", env.task_id, env.type)
            return
        fields: dict[str, Any] = {"last_message": env.message_id}
        status = REQUESTER_TRANSITIONS.get(env.type)
        if env.type == "UPDATE":
            status = env.body.get("state")
        elif env.type == "RESULT":
            status = task_state_for_result(env.body["status"])
            fields.update(result=env.body, result_status=env.body["status"],
                          output_refs=[a.to_dict() for a in env.artifacts])
        elif env.type in ("REJECT", "ERROR"):
            summary = env.body.get("reason") or env.body.get("message")
            fields.update(result=result_body("failed", f"{env.type.lower()}: {summary}"), result_status="failed")
        ledger.update_task(env.task_id, "requester", status=status, **fields)

    # ---- execution ----------------------------------------------------

    def _enqueue(self, addr: str, task_id: str) -> None:
        if task_id in self._queued[addr]:
            return
        self._queued[addr].add(task_id)
        self._queues[addr].put_nowait(task_id)

    async def _runner(self, agent: AgentConfig, addr: str) -> None:
        queue = self._queues[addr]
        while True:
            task_id = await queue.get()
            try:
                runner = asyncio.create_task(self._execute(agent, task_id), name=f"task:{task_id}")
                self._running[task_id] = runner
                try:
                    await asyncio.shield(runner)
                except asyncio.CancelledError:
                    if not runner.done():       # daemon shutdown: stop the task, recover() resumes it later
                        runner.cancel()
                        await asyncio.gather(runner, return_exceptions=True)
                        raise
            finally:
                self._running.pop(task_id, None)
                self._queued[addr].discard(task_id)

    async def _execute(self, agent: AgentConfig, task_id: str) -> None:
        hub = self.hub
        task = hub.ledger.task(task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES:
            return
        attempt = hub.ledger.bump_attempts(task_id)
        if attempt > agent.max_attempts:
            await hub.finish(task_id, result_body(
                "failed", f"gave up after {agent.max_attempts} attempt(s); the agent process kept dying",
                limitations=["see node log / runs/ directory on the owner node"]))
            return
        request = hub.ledger.request_envelope(task_id)
        timeout = float(request.body.get("timeout_s") or agent.task_timeout_s)
        hub.ledger.update_task(task_id, "owner", result_draft=None)
        ctx = TaskContext(task_id, request, agent, self.cfg, attempt)
        wt = None
        if request.body.get("kind") == "code" and agent.repo:
            inputs = request.body.get("inputs")
            base_ref = inputs.get("base_ref", "HEAD") if isinstance(inputs, dict) else "HEAD"
            try:
                wt = await Worktree.create(Path(agent.repo), self.cfg.node, agent.id, task_id, base_ref)
            except GitError as e:
                await hub.finish(task_id, result_body("failed", f"could not create worktree: {e}"))
                return
            ctx.workdir, ctx.git_branch = wt.path, wt.branch
        where = f", branch {wt.branch}" if wt else ""
        await hub.owner_transition(task_id, "RUNNING", f"started (attempt {attempt}, runtime {agent.runtime}{where})")
        try:
            outcome = await asyncio.wait_for(make_runtime(agent, self.cfg).run(ctx), timeout)
        except asyncio.TimeoutError:
            await hub.finish(task_id, result_body("failed", f"timed out after {timeout:.0f}s",
                                                  limitations=["process was killed at the deadline"]))
            return
        except asyncio.CancelledError:
            if task_id in self._cancel_requested:
                self._cancel_requested.discard(task_id)
                await hub.owner_transition(task_id, "CANCELLED", "cancelled by requester; process stopped")
                return
            raise
        except Exception as e:
            log.exception("runtime failed for %s", task_id)
            await hub.finish(task_id, result_body("failed", f"runtime error: {e!r}"))
            return

        current = hub.ledger.task(task_id, "owner")
        if current["status"] in TERMINAL_STATES:
            return      # the agent already closed the task through its tools
        if current["status"] == "BLOCKED" and not current.get("result_draft"):
            return      # blocked and nothing to deliver: wait for the requester
        # A submitted result always wins over an earlier BLOCKED report.
        body, refs = _result_from(current.get("result_draft"), outcome)
        if wt:
            await self._attach_git(wt, agent, task_id, body, refs)
        await hub.finish(task_id, body, refs)
        await self._notify(agent, current["owner"], task_id,
                           f"FYI: {current['owner']} finished {task_id} for {current['requester']} "
                           f"({body['status']}): {body.get('summary', '')[:300]}")

    async def _notify(self, agent: AgentConfig, sender: str, task_id: str, text: str) -> None:
        """Copy a node's lead (agent.notify) on work its workers take on. Best effort, informational only."""
        for target in agent.notify:
            try:
                await self.hub.send(Envelope(type="UPDATE", sender=sender, to=target, task_id=task_id,
                                             body={"message": text, "fyi": True}))
            except Exception as e:
                log.warning("notify %s failed: %r", target, e)

    async def _attach_git(self, wt: Worktree, agent: AgentConfig, task_id: str, body: dict[str, Any],
                          refs: list[ArtifactRef]) -> None:
        """Code tasks deliver commits: a git ref plus a patch artifact any machine can `git am`."""
        try:
            summary = await wt.summary()
            patch = await wt.write_patch(self.cfg.data_path / "runs" / f"{task_id}.patch")
            if summary["commits"]:
                await wt.import_branch()
        except GitError as e:
            body.setdefault("limitations", []).append(f"could not read worktree: {e}")
            return
        outputs = body.get("outputs")
        body["outputs"] = {**(outputs if isinstance(outputs, dict) else {"value": outputs} if outputs else {}),
                           "git": summary}
        refs.append(ArtifactRef(uri=f"git://{self.cfg.node}{wt.repo}@{wt.branch}", id="BRANCH",
                                description=f"head {summary['head']}"))
        if patch:
            refs.append(await self.hub.artifacts.publish(
                patch, f"{self.cfg.node}/{agent.id}/{task_id}/changes.patch", id="PATCH",
                description=f"{len(summary['commits'])} commit(s) on {wt.branch}; apply with git am"))
        if summary["uncommitted_changes"]:
            body.setdefault("limitations", []).append("worktree has uncommitted changes that are not in the patch")
        if not summary["commits"] and body.get("status") == "complete":
            body["status"] = "partial"
            body.setdefault("limitations", []).append("code task finished without any commit")

    # ---- heartbeat / cards --------------------------------------------

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            try:
                await self._publish_cards()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("heartbeat failed: %r", e)

    async def _publish_cards(self, state_override: str | None = None) -> None:
        hub = self.hub
        bus = hub.bus
        now = now_iso()
        await bus.kv_put(bus.names.nodes_kv, self.cfg.node, {
            "node": self.cfg.node, "project": self.cfg.project, "description": self.cfg.description,
            "hostname": socket.gethostname(), "platform": f"{platform.system()} {platform.machine()}",
            "resources": self.cfg.resources, "agents": [a.id for a in self.cfg.agents],
            "version": __version__, "code": self.code_version, "python": platform.python_version(),
            "heartbeat_s": self.cfg.heartbeat_s,
            "last_heartbeat": now,
            "state": state_override or "online",
            "outbox_queued": hub.ledger.count("out", "queued")})
        for agent in self.cfg.agents:
            addr = str(Address(self.cfg.node, agent.id))
            running = [t for t in self._running if t in self._queued.get(addr, set())]
            owned_open = hub.ledger.tasks(role="owner", local_agent=addr,
                                          statuses=("PENDING", "ACCEPTED", "RUNNING", "WAITING", "BLOCKED"))
            if agent.mode == "interactive":      # an accepted task is RUNNING until its result is submitted
                running = [t["task_id"] for t in owned_open if t["status"] == "RUNNING"]
            state = state_override or ("working" if running else "idle")
            pending = None
            with contextlib.suppress(Exception):
                pending = await bus.inbox_pending(Address(self.cfg.node, agent.id))
            await bus.kv_put(bus.names.agents_kv, f"{self.cfg.node}.{agent.id}", {
                "address": addr, "node": self.cfg.node, "agent_id": agent.id, "display": agent.display,
                "role": agent.role, "description": agent.description, "provider": agent.provider,
                "model": agent.model, "runtime": agent.runtime, "mode": agent.mode,
                "capabilities": agent.capabilities, "permissions": agent.permissions,
                "accept_from": agent.accept_from, "resources": self.cfg.resources,
                "state": state, "current_task": running[0] if running else None,
                "open_tasks": len(owned_open), "queue": max(0, len(self._queued.get(addr, ())) - len(running)),
                "inbox_unread": (hub.ledger.unseen_count(addr) if agent.mode == "interactive"
                                 else hub.ledger.count("in", "new", addr) + (pending or 0)),
                "heartbeat_s": self.cfg.heartbeat_s, "last_heartbeat": now})

    # ---- outbox -------------------------------------------------------

    async def _outbox_loop(self) -> None:
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._outbox_wake.wait(), timeout=1)
            self._outbox_wake.clear()
            try:
                await self.hub.flush_outbox()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("outbox flush failed: %r", e)

    def _agent_cfg(self, address: str) -> AgentConfig | None:
        addr = Address.parse(address)
        return next((a for a in self.cfg.agents if a.id == addr.agent and addr.node == self.cfg.node), None)


def _result_from(draft: dict[str, Any] | None, outcome) -> tuple[dict[str, Any], list[ArtifactRef]]:
    """Pick the agent's structured result. Never upgrade an unstructured finish to 'complete'."""
    candidate = draft or outcome.result
    if candidate and candidate.get("status") in ("complete", "partial", "failed") and candidate.get("summary"):
        refs = [ArtifactRef.from_dict(a) for a in candidate.get("artifacts", [])]
        body = {k: v for k, v in candidate.items() if k != "artifacts"}
        if outcome.exit_code != 0 and body["status"] == "complete":
            body["status"] = "partial"
            body.setdefault("limitations", []).append(f"agent process exited with code {outcome.exit_code}")
        return body, refs
    status = "failed" if outcome.exit_code != 0 else "partial"
    outputs = {"raw_output_tail": outcome.output_tail[-2000:]}
    errors = _error_lines(outcome.log_path)
    if errors:   # the CLI's own stderr usually says why (e.g. "workspace is out of credits")
        outputs["error_lines"] = errors
    summary = f"agent finished without a structured result (exit code {outcome.exit_code})"
    if errors:
        summary += f": {errors[-1][:200]}"
    return result_body(status, summary, outputs=outputs,
                       limitations=["no submit_result call; outcome could not be verified",
                                    f"log: {outcome.log_path}"]), []


def _error_lines(log_path: str | None, limit: int = 8) -> list[str]:
    """Error-looking lines from a run log (stderr goes there), so the requester sees why a run failed."""
    if not log_path:
        return []
    try:
        lines = Path(log_path).read_text(errors="replace").splitlines()[1:]    # skip the "$ command" line
    except OSError:
        return []
    keys = ("error", "exception", "traceback", "denied", "not found", "failed", "out of credits", "quota",
            "rate limit", "unauthorized", "crash")
    hits = [ln.strip() for ln in lines if any(k in ln.lower() for k in keys)]
    return list(dict.fromkeys(hits))[-limit:]
