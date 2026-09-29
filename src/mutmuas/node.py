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
import os
import signal
import platform
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nats.errors import TimeoutError as NatsTimeoutError

from . import __version__
from .bus import Names
from .config import AgentConfig, NodeConfig
from .hub import Hub
from .ids import Address, now_iso, parse_iso
from .protocol import (OPEN_STATES, REQUEST_KINDS, TERMINAL_STATES, ArtifactRef, Envelope, ProtocolError,
                       reply_required, result_body, task_state_for_result)
from .runtime import TaskContext, make_runtime
from .visibility import CARD_KEYS, accepts_kinds, acl, short
from .worktree import GitError, Worktree

log = logging.getLogger(__name__)

# auto_worker (D-030): after a session ends, wait this long before running tasks as a worker again (it may only
# be the leader restarting his terminal).
AUTO_WORKER_GRACE_S = 120
# Stopping a worker that outlived the daemon (restart): TERM, then KILL after this many seconds.
STOP_GRACE_S = 5
SESSION_STALE_S = 50          # the session's MCP process beats every 15 s (mcp_server.HEARTBEAT_S)
FOLLOW_UP_EVERY_S = 30


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def session_alive(session: dict[str, Any]) -> bool:
    return session_fields(session, Path("/")).get("session") == "online"


def session_present(ledger, agent: str) -> str | None:
    """auto_worker: why the leader's session counts as there (live, or gone less than AUTO_WORKER_GRACE_S ago),
    or None when the daemon may run the agent's tasks as a worker."""
    row = ledger.session_of(agent)
    if not row:
        return None
    if session_alive(row):
        return "a session holds the agent"
    age = (datetime.now(timezone.utc) - parse_iso(row["last_seen"])).total_seconds()
    return "a session ended moments ago (grace period)" if age < AUTO_WORKER_GRACE_S else None


# The lease decision trusts the process tree, so the tree must not come from anything the caller controls:
# not a PATH-resolved `ps` (Codex review of dfdd719). Linux: /proc; elsewhere: ps by absolute path.
_PS = next((p for p in ("/bin/ps", "/usr/bin/ps") if os.path.exists(p)), None)


def _proc_field(pid: int, index: int, ps_column: str) -> str | None:
    """One field of a process: /proc/<pid>/stat (index counted after the command name, which may contain ")") or
    an absolute-path ps; None when it cannot be read (never a guess)."""
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        try:
            return stat.read_text().rsplit(")", 1)[1].split()[index]
        except (OSError, IndexError):
            return None
    if _PS is None:
        return None
    import subprocess
    out = subprocess.run([_PS, "-o", f"{ps_column}=", "-p", str(pid)], capture_output=True, text=True,
                         env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
    return out.stdout.strip() or None


def _ppid(pid: int) -> int | None:
    value = _proc_field(pid, 1, "ppid")                                # stat field 4
    return int(value) if value and value.isdigit() else None


def proc_start(pid: int) -> str | None:
    """When a process started (identifies it across pid reuse)."""
    return _proc_field(pid, 19, "lstart")                              # stat field 22: starttime


def _zombie(pid: int) -> bool:
    """An exited process not yet reaped by its parent: it runs nothing any more."""
    return (_proc_field(pid, 0, "stat") or "").startswith("Z")         # stat field 3: state


def same_process(pid: int | None, start: str | None) -> bool:
    """The recorded process is still running (not a new process that got the same pid). Both are needed: with no
    recorded start time nothing proves it is the same process (fail closed; Codex review of f8c105e)."""
    return (bool(pid) and start is not None and _pid_alive(pid) and proc_start(pid) == start
            and not _zombie(pid))


def group_state(pgid: int) -> tuple[str, list[int]]:
    """Which processes of a process group still run (zombies excluded): ("members", pids), ("empty", []), or
    ("unknown", []) when the process list cannot be trusted: no ps, ps failed, empty or malformed output, or a
    list that does not even contain this process. A worker leads its own group (start_new_session), so its
    children are listed too. "unknown" never counts as empty (Codex review of 0a9f031)."""
    if not pgid:
        return "empty", []
    if _PS is None:
        return "unknown", []
    import subprocess
    try:
        out = subprocess.run([_PS, "-A", "-o", "pid=,pgid=,stat="], capture_output=True, text=True, timeout=10,
                             env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
    except (OSError, subprocess.SubprocessError):
        return "unknown", []
    if out.returncode != 0:
        return "unknown", []
    rows = []
    for line in out.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 3 or not parts[0].isdigit() or not parts[1].isdigit():
            return "unknown", []
        rows.append((int(parts[0]), int(parts[1]), parts[2]))
    if not any(pid == os.getpid() for pid, _, _ in rows):
        return "unknown", []
    members = [pid for pid, group, stat in rows if group == pgid and not stat.startswith("Z")]
    return ("members", members) if members else ("empty", [])


def live_worker_runs(ledger, agent: str) -> dict[int, str]:
    """pid -> task id of the daemon-started worker processes of `agent` that are still running."""
    return {pid: task_id for pid, start, task_id in ledger.worker_runs(agent) if same_process(pid, start)}


def _ancestors(pid: int) -> list[int]:
    """pid's parent chain, from /proc or an absolute-path ps (never PATH)."""
    chain, seen = [], set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        parent = _ppid(pid)
        if parent is None:
            break
        pid = parent
        chain.append(pid)
    return chain


def lease_refusal(ledger, agent: str) -> str | None:
    """Why this process may not act as `agent` now, or None. A live session holds the agent: only that session
    (its MCP process, or anything the session itself started, e.g. agentctl from its shell) may use it.
    No environment variable exempts a process: MUTMUAS_TASK_ID is set by whoever starts the process, so it
    proves nothing (Codex review of dfdd719). Daemon-run tasks act as worker agents, which hold no lease."""
    row = ledger.session_of(agent)
    if not row or row["pid"] in (0, os.getpid()) or not session_alive(row):
        return None
    # session_pid is missing when the holder's MCP process runs older code (e2fb5d1) than this CLI, as during
    # an upgrade: then the MCP process's own parent is the session.
    session = row.get("session_pid") or next(iter(_ancestors(row["pid"])[:1]), None)
    allowed = {row["pid"], session} - {None, 0}
    # auto_worker: a task the daemon started before the session came is finished, not interrupted (D-032a);
    # its processes descend from the pid the daemon recorded when it spawned them.
    allowed |= set(live_worker_runs(ledger, agent))
    if allowed & {os.getpid(), *_ancestors(os.getpid())}:
        return None
    return (f"{agent} is held by another session (process {row['pid']}, directory {row.get('cwd')}); this process "
            "does not hold its session, so it may not read or answer its mail. One agent, one session.")


def public_session(fields: dict[str, Any]) -> dict[str, Any]:
    """On the shared card only: is the session on duty. Its directory and warnings are for the agent and
    the coordinators."""
    return {k: v for k, v in fields.items() if k in ("session", "session_seen")}


def session_fields(session: dict[str, Any] | None, workdir: Path) -> dict[str, Any]:
    """Registry card view of an interactive agent's session, from its MCP process heartbeat.
    unknown: no session has ever registered (e.g. one without the mutmuas MCP server)."""
    if session is None:
        return {"session": "unknown"}
    age = (datetime.now(timezone.utc) - parse_iso(session["last_seen"])).total_seconds()
    online = age < SESSION_STALE_S and _pid_alive(session["pid"])
    out: dict[str, Any] = {"session": "online" if online else "offline", "session_seen": session["last_seen"]}
    if online and session.get("cwd"):
        out["session_cwd"] = session["cwd"]
        if not Path(os.path.realpath(session["cwd"])).is_relative_to(os.path.realpath(workdir)):
            # Claude Code keeps memory per start directory: the wrong one means an empty memory. A project
            # directory below the workdir is the right one (staff system v4, D-029).
            out["session_warning"] = f"session started in {session['cwd']}, not in its workdir {workdir}"
    return out


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
        self._arriving: set[str] = set()                     # REQUESTs _on_request is still deciding about
        self._cancel_requested: set[str] = set()
        self._background: set[asyncio.Task] = set()          # e.g. observer-copy checks against the task KV
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
            if agent.mode == "worker" or agent.auto_worker:
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
        # background jobs (e.g. the unbounded observer-copy verifier) use the Hub too: end them before closing it
        # (Codex review of d98413f); recover() schedules the verifiers again at the next start
        background = list(self._background)
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        self._background.clear()
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
        for task in hub.ledger.tasks(role="owner", statuses=OPEN_STATES, limit=None):
            agent = self._agent_cfg(task["owner"])
            if agent is None or (agent.mode != "worker" and not agent.auto_worker):
                continue
            if agent.auto_worker:
                await self._recover_auto(task)
                continue
            if task["status"] not in ("PENDING", "ACCEPTED", "RUNNING"):
                continue
            if task["status"] == "RUNNING":
                await hub.owner_transition(task["task_id"], "ACCEPTED",
                                           f"node {self.cfg.node} restarted; task will be resumed")
            elif task["status"] == "PENDING":
                await self._accept(task["task_id"])
            self._enqueue(task["owner"], task["task_id"])
        for task in hub.ledger.tasks(role="owner", limit=500):
            await hub.publish_task_record(task["task_id"])
        # observer copies still waiting for their task record when the daemon stopped: look again
        for env in hub.ledger.inbound_in_state("unverified"):
            self._background_job(self._verify_observer_copy(env))
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
                    note = None
                    if isinstance(state, tuple):
                        state, note = state
                    self.hub.ledger.mark_handled(env.message_id, state or "handled", note)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("handling %s failed", env.short())
                    self.hub.ledger.mark_handled(env.message_id, "dropped", repr(e))

    async def _handle(self, agent: AgentConfig, env: Envelope) -> str | None:
        if env.type == "UPDATE" and env.body.get("copy_of"):
            return self._on_observer_copy(env)
        if env.type == "UPDATE" and env.body.get("observers_add"):
            return self._on_observers_added(env)
        if env.type == "REQUEST":
            return await self._on_request(agent, env)
        elif env.type == "CANCEL":
            return await self._on_cancel(env)
        elif env.type == "ANSWER":
            pass   # surfaced to the agent through its inbox (MCP/CLI); see technical debt in docs
        else:
            return await self._on_reply(env)

    def _on_observer_copy(self, env: Envelope) -> str | None:
        """A copy of a task's REQUEST or RESULT for an observer. Who takes part is never taken from the copy
        itself (a sender could list itself: Codex review of dfdd719):
        - this node knows the task: the sender must be in the persisted ACL, else rejected;
        - this node has never heard of it (a cross-node observer): kept unverified, not shown, until the shared
          task record names the sender as its requester or owner (_verify_observer_copy)."""
        copy = env.body.get("copy_of") or {}
        if copy.get("type") not in ("REQUEST", "RESULT") or not copy.get("from") or not copy.get("to"):
            return "rejected"
        # Structural check (as documented): the copy lists both its sender and its recipient. It proves nothing
        # by itself, so the authoritative checks below still decide (Codex review of 8c018ee).
        people = set(env.body.get("participants") or [])
        if env.sender not in people or env.to not in people:
            log.warning("dropped observer copy %s: sender or recipient not in its participant list", env.short())
            return "rejected"
        known = acl(self.hub.ledger, env.task_id)
        if known:
            row = self.hub.ledger.task(env.task_id)        # every row of a task has the same requester/owner
            if env.sender not in (row["requester"], row["owner"]):
                # Content copies come from the requester or owner only; an observer's grant is relayed by the
                # owner (Codex reviews of 8c018ee and d13ffc8), so an observer never authors one.
                log.warning("dropped observer copy %s: %s is not the task's requester or owner", env.short(),
                            env.sender)
                return "rejected"
            self._record_copy(env, row["requester"], row["owner"])
            return None
        self._background_job(self._verify_observer_copy(env))
        return "unverified"

    def _still_unverified(self, message_id: str) -> bool:
        return self.hub.ledger.inbound_state(message_id) in (None, "new", "unverified")

    # The copy and the owner's task record travel separately, so the record may not be there yet: look again
    # with growing gaps, then every last gap until it appears (Codex reviews of 8c018ee and d13ffc8).
    VERIFY_BACKOFF_S = (0.05, 0.2, 1, 3, 10, 30, 60, 120, 300)

    async def _verify_observer_copy(self, env: Envelope) -> None:
        """Accept a copy about a task this node does not know only if the shared task record (written by the
        owner's node) names its sender as requester or owner. That is a consistency check, not an authorisation
        boundary: any node credential can write the task KV (step 2, D-008)."""
        record, attempt = None, 0
        while not record:
            # growing gaps at first, then the last gap for as long as the copy is still waiting: an owner that is
            # offline may publish its record much later (Codex review of d13ffc8)
            await asyncio.sleep(0 if attempt == 0 else
                                self.VERIFY_BACKOFF_S[min(attempt, len(self.VERIFY_BACKOFF_S)) - 1])
            attempt += 1
            if not self.hub.bus:
                return
            if attempt > 1 and not self._still_unverified(env.message_id):
                return                                     # decided elsewhere (e.g. a second verifier)
            try:
                record = await self.hub._remote_task(env.task_id, None)
            except Exception as e:
                log.warning("could not verify observer copy %s: %r", env.short(), e)
                record = None
            if not record and attempt == len(self.VERIFY_BACKOFF_S) + 1:
                log.warning("observer copy %s still unverified: no task record for %s yet; checking every %ss",
                            env.short(), env.task_id, self.VERIFY_BACKOFF_S[-1])
        if env.sender not in (record.get("requester"), record.get("owner")):
            log.warning("dropped observer copy %s: the task record does not name %s", env.short(), env.sender)
            self.hub.ledger.mark_handled(env.message_id, "rejected")
            return
        self._record_copy(env, record["requester"], record["owner"])
        self.hub.ledger.mark_handled(env.message_id, "handled")

    def _record_copy(self, env: Envelope, requester: str, owner: str) -> None:
        """The observer's own row: requester and owner from our records or the task record, never from the
        copy; the only observer it adds is the recipient itself."""
        copy = env.body["copy_of"]
        self.hub.ledger.record_observed(
            env.task_id, env.to, requester, owner,
            request=copy.get("body") if copy["type"] == "REQUEST" else None,
            result=copy.get("body") if copy["type"] == "RESULT" else None,
            observers=[env.to])

    def _on_observers_added(self, env: Envelope) -> str | None:
        """Another participant added observers: extend our copy of the list, so this side forwards the
        RESULT to them too. Ignored unless the sender takes part in the task on our records."""
        before = acl(self.hub.ledger, env.task_id)
        if env.sender not in before:
            log.warning("ignored observers_add from non-participant %s on %s", env.sender, env.task_id)
            return "rejected"
        added = [str(o) for o in env.body["observers_add"]]
        self.hub.ledger.add_observers(env.task_id, added)
        owned = self.hub.ledger.task(env.task_id, "owner")
        new = [o for o in added if o not in before]
        from_observer = owned is not None and env.sender not in (owned["requester"], owned["owner"])
        if owned and owned["local_agent"] == env.to and new and from_observer:
            # (a requester that adds an observer sends the copies itself: relaying would duplicate them)
            # An observer added them: as the owner, relay the copies, so that a node that never saw the task can
            # check the sender against the task record (Codex review of 8c018ee).
            from .tools import send_observer_copies
            self._background_job(send_observer_copies(self.hub, env.to, env.task_id, new))
        return None

    def _background_job(self, coro) -> None:
        """A job outside the daemon's fixed loops (see _spawn), e.g. an observer-copy verifier that may run as long
        as the daemon: keep a reference, log a failure; stop() cancels and awaits every one before closing the Hub."""
        task = asyncio.create_task(coro)
        self._background.add(task)

        def done(t: asyncio.Task) -> None:
            self._background.discard(t)
            if not t.cancelled() and t.exception():
                log.error("background job failed: %r", t.exception())
        task.add_done_callback(done)

    async def _on_request(self, agent: AgentConfig, env: Envelope) -> str | None:
        # While it decides (it awaits the task record's publication), the heartbeat's _auto_dispatch must not
        # also take the new PENDING task, or it is accepted twice (found in the flaky-test review of 5576ad5).
        self._arriving.add(env.task_id)
        try:
            return await self._decide_request(agent, env)
        finally:
            self._arriving.discard(env.task_id)

    async def _decide_request(self, agent: AgentConfig, env: Envelope) -> str | None:
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
            if agent.mode == "interactive" and agent.accepts(env.sender):
                # A colleague picked the wrong kind: still refused, but the session must see that it was
                # asked, or the request silently disappears on both ends (2026-09-25, kind=code to A:claude).
                return "handled", f"rejected: {denial}"
            return "rejected"
        await hub.publish_task_record(env.task_id)
        if agent.mode == "worker" or (agent.auto_worker and not session_present(hub.ledger, env.to)):
            await self._accept(env.task_id)
            self._enqueue(env.to, env.task_id)
            await self._notify(agent, env.to, env.task_id,      # status layer only: the lead is no participant
                               f"FYI: {env.to} accepted {env.task_id} from {env.sender}: "
                               f"{short(env.body.get('objective'))}")
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
        if env.sender != task["requester"]:         # only the persisted requester may cancel its task
            log.warning("ignored CANCEL of %s from %s: not its requester %s", env.task_id, env.sender,
                        task["requester"])
            return "rejected"
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

    async def _on_reply(self, env: Envelope) -> str | None:
        """A message about a task *we* requested."""
        ledger = self.hub.ledger
        task = ledger.task(env.task_id, "requester")
        if task is None or task["local_agent"] != env.to or env.body.get("fyi"):
            # Not a reply to this agent's own request: e.g. an FYI copy to a node lead about a task that
            # another agent on the same node requested. It stays in the recipient's inbox only.
            log.info("message about task %s (%s) kept in inbox only", env.task_id, env.type)
            return None
        if env.sender != task["owner"]:
            # Only the persisted owner speaks for the task: anyone else's RESULT/UPDATE must not change it
            # (Codex review of dfdd719: a RESULT from an unrelated sender completed the task).
            log.warning("ignored %s about %s from %s: not its owner %s", env.type, env.task_id, env.sender,
                        task["owner"])
            return "rejected"
        fields: dict[str, Any] = {"last_message": env.message_id}
        status = REQUESTER_TRANSITIONS.get(env.type)
        if env.type == "UPDATE":
            status = env.body.get("state")
        elif env.type == "RESULT":
            status = task_state_for_result(env.body["status"])
            observers = (task.get("request") or {}).get("observers") or []
            await self.hub.copy_to_observers(task["local_agent"], env, observers)
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
        if agent.auto_worker and same_process(task.get("runner_pid"), task.get("runner_start")):
            log.info("task %s: a worker from before a restart still runs; not started again", task_id)
            return
        if agent.auto_worker and task.get("runner") == "worker" and await self._deliver_draft(task_id):
            return
        if agent.auto_worker:
            # Claim it for the worker in one transaction with the session check: never done twice, and while
            # the leader's session is there, a task that has not started yet is his to decide (D-032a).
            addr = task["owner"]
            refused = hub.ledger.claim_task(task_id, "worker", ("ACCEPTED", "RUNNING"),
                                            refuse_if=lambda: session_present(hub.ledger, addr))
            if refused:
                if "session" in refused and task["status"] == "ACCEPTED":
                    # Release the claim first: a stop between the two steps leaves ACCEPTED + no runner, which
                    # recover queues again (and the claim checks the session again). The other order left PENDING
                    # + runner=worker, which nothing picked up (Codex review of e6a9df9).
                    hub.ledger.release_task(task_id, "worker")
                    await hub.owner_transition(task_id, "PENDING", f"{addr}'s session is online: left for it")
                log.info("task %s not run as a worker: %s", task_id, refused)
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
        if agent.auto_worker:
            ctx.on_spawn = lambda pid: self._record_worker(task_id, pid)
        wt = None
        if request.body.get("kind") == "code" and agent.copies_code:
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
                           f"({body['status']})")

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
            ref = await self.hub.artifacts.publish(
                patch, f"{self.cfg.node}/{agent.id}/{task_id}/changes.patch", id="PATCH",
                description=f"{len(summary['commits'])} commit(s) on {wt.branch}; apply with git am")
            self.hub.ledger.record_published(ref.uri, str(Address(self.cfg.node, agent.id)))
            refs.append(ref)
        if summary["uncommitted_changes"]:
            body.setdefault("limitations", []).append("worktree has uncommitted changes that are not in the patch")
        if not summary["commits"] and body.get("status") == "complete":
            body["status"] = "partial"
            body.setdefault("limitations", []).append("code task finished without any commit")

    # ---- heartbeat / cards --------------------------------------------

    async def _heartbeat(self) -> None:
        last_follow_up = 0.0
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            try:
                await self._publish_cards()
                await self._auto_dispatch()
                if asyncio.get_running_loop().time() - last_follow_up >= FOLLOW_UP_EVERY_S:
                    last_follow_up = asyncio.get_running_loop().time()
                    await self._follow_ups()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("heartbeat failed: %r", e)

    async def _warn_session_dir(self, addr: str, session: dict[str, Any] | None, workdir: Path) -> None:
        """A session started outside its workdir gets an empty memory: tell the agent and the coordinators,
        once per session."""
        warning = session_fields(session, workdir).get("session_warning")
        if not warning or not self.hub.ledger.notice_once(f"session:{addr}:{session['started_at']}", "cwd"):
            return
        for target, extra in ((addr, {"next": addr}), *((c, {}) for c in self.cfg.coordinators if c != addr)):
            try:
                await self.hub.send(Envelope(type="UPDATE", sender=addr, to=target,
                                             task_id=f"session-{self.cfg.node}", body={
                                                 "message": f"session warning for {addr}: {warning}",
                                                 "fyi": True, "follow_up": "session_dir", **extra}))
            except Exception as e:
                log.warning("session warning to %s failed: %r", target, e)

    async def _follow_ups(self) -> None:
        """Chase replies this node is owed (like an email client's follow-up flag), each once:
        - overdue: reply required, deadline passed, no RESULT yet;
        - session_offline: the owner is an interactive agent whose node is up but whose session is gone,
          so the request sits unread (leader's rule: no session found = that terminal is offline).
        Nothing is chased while the owner's node itself is offline (a closed laptop): the clock waits."""
        hub = self.hub
        now = datetime.now(timezone.utc)
        for t in hub.ledger.tasks(role="requester", statuses=OPEN_STATES, limit=None):
            request = t.get("request") or {}
            if not reply_required(request):
                continue
            card = await hub.card_or_none(t["owner"])
            if not (card and card.get("online")):
                continue
            deadline = request.get("deadline")
            with contextlib.suppress(TypeError, ValueError):
                if deadline and parse_iso(deadline) < now and hub.ledger.notice_once(t["task_id"], "overdue"):
                    await self._follow_up(t, "overdue", f"{t['owner']} has not replied to {t['task_id']} "
                                                        f"(deadline {deadline}, status {t['status']})")
            if (card.get("mode") == "interactive" and not card.get("auto_worker")    # auto_worker: a worker takes it
                    and card.get("session") == "offline" and t["status"] == "PENDING"
                    and hub.ledger.notice_once(t["task_id"], "session_offline")):
                await self._follow_up(t, "session_offline", f"{t['owner']}'s node is up but its session is "
                                                            f"offline: {t['task_id']} waits unread")

    async def _follow_up(self, task: dict[str, Any], reason: str, text: str) -> None:
        """Tell the requester (wakes it: next = requester), and copy the escalation addresses (FYI only)."""
        requester = task["local_agent"]
        for target, extra in ((requester, {"next": requester}), *((a, {}) for a in self.cfg.escalate_to)):
            try:
                await self.hub.send(Envelope(type="UPDATE", sender=requester, to=target, task_id=task["task_id"],
                                             body={"message": f"follow-up ({reason}): {text}", "fyi": True,
                                                   "follow_up": reason, **extra}))
            except Exception as e:
                log.warning("follow-up to %s failed: %r", target, e)

    def _record_worker(self, task_id: str, pid: int) -> None:
        """Record the worker process with its start time; without one its identity cannot be proven later, so it
        is stopped and the attempt fails (Codex review of f8c105e)."""
        start = proc_start(pid)
        if start is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, signal.SIGKILL)
            raise RuntimeError(f"could not read the start time of worker process {pid}; stopped it")
        self.hub.ledger.set_runner_pid(task_id, pid, start)

    async def _recover_auto(self, task: dict[str, Any]) -> None:
        """auto_worker after a restart (option B, Codex reviews of f8c105e and e6a9df9): no task is adopted, and
        a task is run again only once nothing of its old worker runs.
        - The recorded worker still runs (pid and start time match): stop its whole process group.
        - Its leader is gone (or its start time was never recorded) but processes of the group, or the pid, still
          run: nothing proves they are the worker's, nor that they are not. Do not run the task again and do not
          signal them: fail it with the reason (quarantine).
        - Then: deliver the draft the worker submitted, or queue the task again (the runner's claim still checks
          the session). A PENDING task still claimed by the worker (a stop between its two steps) is released for
          _auto_dispatch. A session's task stays the session's."""
        hub, task_id = self.hub, task["task_id"]
        if task.get("runner") == "session":
            return
        pid, start = task.get("runner_pid"), task.get("runner_start")
        if pid and same_process(pid, start):
            stopped = await self._stop_worker(pid, start)
            if stopped != "stopped":
                await self._quarantine(task_id, pid, "could not stop the worker from before the restart"
                                       if stopped == "running" else
                                       "stopped the worker from before the restart, but its process group could "
                                       "not be checked afterwards")
                return
        elif pid:
            state, _ = group_state(pid)
            alive = _pid_alive(pid) and not _zombie(pid)
            if state == "unknown":
                await self._quarantine(task_id, pid, "the process group of the worker from before the restart "
                                                     "could not be checked (process list unavailable)")
                return
            if state == "members" or (alive and (start is None or proc_start(pid) is None)):
                # Its leader is gone or cannot be identified, but something still runs there. A start time that
                # was recorded and differs proves the pid now belongs to another process: that one is ignored.
                await self._quarantine(task_id, pid, "processes of the worker from before the restart may still "
                                                     "run and cannot be proven to be it")
                return
        if task["status"] == "PENDING":
            if task.get("runner") == "worker":
                hub.ledger.release_task(task_id, "worker")
            return
        if await self._deliver_draft(task_id):
            return
        hub.ledger.set_runner_pid(task_id, None)
        if task["status"] != "ACCEPTED":
            await hub.owner_transition(task_id, "ACCEPTED", f"node {self.cfg.node} restarted; task will be run again")
        self._enqueue(task["owner"], task_id)

    async def _quarantine(self, task_id: str, pid: int, why: str) -> None:
        """Neither run the task again nor signal anything whose identity is not proven: fail it with the reason."""
        await self.hub.finish(task_id, result_body(
            "failed", f"{why} (process group {pid}); not run again and not signalled further",
            limitations=[f"check and stop process group {pid} by hand, then send the request again"]))

    async def _stop_worker(self, pid: int, start: str | None) -> str:
        """Stop a worker's whole process group: TERM, then KILL after STOP_GRACE_S. "stopped" once no process of
        the group runs any more (not merely its leader: Codex review of e6a9df9); "running" if something still
        runs; "unknown" if the group could not be checked (Codex review of 0a9f031)."""
        def gone() -> str:
            if same_process(pid, start):
                return "running"
            state, _ = group_state(pid)
            return {"empty": "stopped", "members": "running"}.get(state, "unknown")

        for sig, wait_s in ((signal.SIGTERM, STOP_GRACE_S), (signal.SIGKILL, 2.0)):
            try:
                os.killpg(pid, sig)                 # a worker leads its own process group (start_new_session)
            except (ProcessLookupError, PermissionError):
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(pid, sig)               # not a group leader: the process itself
            deadline = asyncio.get_running_loop().time() + wait_s
            while gone() == "running" and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.05)
            result = gone()
            if result != "running":
                return result
        return "running"

    async def _deliver_draft(self, task_id: str) -> bool:
        """A worker that submitted its result and ended while the daemon was down: deliver that result rather
        than running the task again (Codex review of f8c105e)."""
        current = self.hub.ledger.task(task_id, "owner")
        draft = (current or {}).get("result_draft")
        if not draft or current["status"] in TERMINAL_STATES:
            return False
        draft = dict(draft)
        refs = [ArtifactRef.from_dict(a) for a in draft.pop("artifacts", None) or []]
        await self.hub.finish(task_id, draft, refs)
        return True

    async def _auto_dispatch(self) -> None:
        """auto_worker agents with no session (past the grace period): run the tasks still waiting for one."""
        hub = self.hub
        for agent in self.cfg.agents:
            addr = str(Address(self.cfg.node, agent.id))
            if not agent.auto_worker or session_present(hub.ledger, addr):
                continue
            for task in hub.ledger.tasks(role="owner", local_agent=addr, statuses=("PENDING",), limit=None):
                if (task.get("runner") is None and task["task_id"] not in self._queued[addr]
                        and task["task_id"] not in self._arriving):
                    await self._accept(task["task_id"])
                    self._enqueue(addr, task["task_id"])

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
                                          statuses=OPEN_STATES)
            if agent.mode == "interactive":      # an accepted task is RUNNING until its result is submitted
                running = [t["task_id"] for t in owned_open if t["status"] == "RUNNING"]
            state = state_override or ("working" if running else "idle")
            pending = None
            with contextlib.suppress(Exception):
                pending = await bus.inbox_pending(Address(self.cfg.node, agent.id))
            card = {
                "address": addr, "node": self.cfg.node, "agent_id": agent.id, "display": agent.display,
                "role": agent.role, "provider": agent.provider, "mode": agent.mode,
                **({"auto_worker": True} if agent.auto_worker else {}),
                "capabilities": agent.capabilities, "accepts_kinds": accepts_kinds(agent.permissions),
                # Public layer only (visibility.py): coarse availability, no current task, queue or inbox
                # counts, no session directory. The agent reads its own details locally (whoami).
                "state": state, "availability": "busy" if running or owned_open else "available",
                **(public_session(session_fields(hub.ledger.session_of(addr), agent.workdir_path))
                   if agent.mode == "interactive" else {}),
                "heartbeat_s": self.cfg.heartbeat_s, "last_heartbeat": now}
            await bus.kv_put(bus.names.agents_kv, f"{self.cfg.node}.{agent.id}",
                             {k: v for k, v in card.items() if k in CARD_KEYS})
            if agent.mode == "interactive":
                await self._warn_session_dir(addr, hub.ledger.session_of(addr), agent.workdir_path)

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
    # The raw output and run log are private (visibility design; Codex review of dfdd719): the requester gets
    # the kind of error only (e.g. "quota"), the text stays in the log on the owner's node.
    kinds = _error_kinds(outcome.log_path)
    outputs = {"error_kinds": kinds} if kinds else {}
    summary = f"agent finished without a structured result (exit code {outcome.exit_code})"
    if kinds:
        summary += f"; errors in its log: {', '.join(kinds)}"
    return result_body(status, summary, outputs=outputs or None,
                       limitations=["no submit_result call; outcome could not be verified",
                                    "the run log stays on the owner's node"]), []


def _error_kinds(log_path: str | None) -> list[str]:
    """Which kinds of error a run log shows (stderr goes there), so the requester learns why a run failed
    without receiving the log's text."""
    if not log_path:
        return []
    try:
        text = "\n".join(Path(log_path).read_text(errors="replace").splitlines()[1:]).lower()  # skip "$ cmd"
    except OSError:
        return []
    keys = ("error", "exception", "traceback", "denied", "not found", "failed", "out of credits", "quota",
            "rate limit", "unauthorized", "crash")
    return [k for k in keys if k in text]
