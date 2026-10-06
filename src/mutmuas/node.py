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
import fcntl
import itertools
import json
import logging
import os
import platform
import signal
import socket
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from nats.errors import TimeoutError as NatsTimeoutError

from . import __version__
from .bus import Names
from .config import AgentConfig, NodeConfig
from .hub import Hub
from .ids import Address, InvalidAddress, check_token, now_iso, parse_iso
from .ledger import is_info
from .protocol import (OPEN_STATES, REQUEST_KINDS, TERMINAL_STATES, ArtifactRef, Envelope, ProtocolError,
                       reply_required, result_body, task_state_for_result)
from .runtime import TaskContext, group_alive, make_runtime, stop_group
from .visibility import CARD_KEYS, accepts_kinds, acl, artifact_visible, is_participant, short
from .worktree import GitError, Worktree

log = logging.getLogger(__name__)

# auto_worker (D-030): after a session ends, wait this long before running tasks as a worker again (it may only
# be the leader restarting his terminal).
AUTO_WORKER_GRACE_S = 120
SESSION_STALE_S = 50          # the session's MCP process beats every 15 s (mcp_server.HEARTBEAT_S)
FOLLOW_UP_EVERY_S = 30
ETA_REPLY_GRACE_S = 3600     # 催办 (D-076): after a chase, how long to wait for a new eta before telling the requester


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
    return session_fields(session).get("session") == "online"


@contextlib.contextmanager
def daemon_lock(cfg: NodeConfig):
    """Held by the node daemon while it runs (one per data dir), and by retire-agent while it changes the node:
    BlockingIOError when someone else holds it."""
    cfg.data_path.mkdir(parents=True, exist_ok=True)
    fd = os.open(cfg.data_path / "daemon.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


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


# The lease decision trusts the process tree, so the tree must not come from anything the caller controls: not a
# PATH-resolved `ps`. Linux: /proc; elsewhere: ps by absolute path.
_PS = next((p for p in ("/bin/ps", "/usr/bin/ps") if os.path.exists(p)), None)


def _proc_field(pid: int, index: int, ps_column: str) -> tuple[str | None, bool]:
    """One field of a process and whether it came from /proc/<pid>/stat (index counted after the command name,
    which may contain ")") rather than an absolute-path ps; None when it cannot be read (never a guess)."""
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        try:
            return stat.read_text().rsplit(")", 1)[1].split()[index], True
        except (OSError, IndexError):
            return None, True
    if _PS is None:
        return None, False
    import subprocess
    out = subprocess.run([_PS, "-o", f"{ps_column}=", "-p", str(pid)], capture_output=True, text=True,
                         env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
    return out.stdout.strip() or None, False


def _ppid(pid: int) -> int | None:
    try:
        return int(_proc_field(pid, 1, "ppid")[0])                     # stat field 4
    except (TypeError, ValueError):
        return None


def proc_start(pid: int) -> str | None:
    """When a process started (identifies it across pid reuse)."""
    return _proc_field(pid, 19, "lstart")[0]                           # stat field 22: starttime


def _zombie(pid: int) -> bool:
    """An exited process not yet reaped by its parent: it runs nothing any more."""
    value, from_proc = _proc_field(pid, 0, "stat")                     # stat field 3: state
    # /proc holds exactly one state letter; ps may add flags after it
    return value == "Z" if from_proc else (value or "").startswith("Z")


def same_process(pid: int | None, start: str | None) -> bool:
    """The recorded process is still running (not a new process that got the same pid). Both are needed: with no
    recorded start time nothing proves it is the same process."""
    return (bool(pid) and start is not None and _pid_alive(pid) and proc_start(pid) == start
            and not _zombie(pid))


def worker_tasks_of(ledger, agent: str, chain: set[int]) -> set[str]:
    """The tasks of `agent` whose recorded, still running worker process is in `chain` (a process and its
    ancestors): who the caller is as a worker, for the lease and for the owner-side actor check alike."""
    return {task_id for pid, task_id in live_worker_runs(ledger, agent).items() if pid in chain}


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
    proves nothing. Daemon-run tasks act as worker agents, which hold no lease.
    A post being retired is refused to everyone."""
    if ledger.retiring(agent):
        return (f"{agent} is being retired (agent-node retire-agent): no session or tool may act as it; "
                "`retire-agent --undo` lifts this.")
    row = ledger.session_of(agent)
    if not row or row["pid"] in (0, os.getpid()) or not session_alive(row):
        return None
    session = row.get("session_pid")
    chain = {os.getpid(), *_ancestors(os.getpid())}
    # auto_worker: a task the daemon started before the session came is finished, not interrupted (D-032a);
    # its processes descend from the pid the daemon recorded when it spawned them.
    if ({row["pid"], session} - {None, 0}) & chain or worker_tasks_of(ledger, agent, chain):
        return None
    return (f"{agent} is held by another session (process {row['pid']}, directory {row.get('cwd')}); this process "
            "does not hold its session, so it may not read or answer its mail. One agent, one session.")


def public_session(fields: dict[str, Any]) -> dict[str, Any]:
    """On the shared card only: is the session on duty. Its directory and warnings are for the agent and
    the coordinators."""
    return {k: v for k, v in fields.items() if k in ("session", "session_seen")}


def session_fields(session: dict[str, Any] | None) -> dict[str, Any]:
    """Registry card view of an interactive agent's session, from its MCP process heartbeat.
    unknown: no session has ever registered (e.g. one without the mutmuas MCP server)."""
    if session is None:
        return {"session": "unknown"}
    age = (datetime.now(timezone.utc) - parse_iso(session["last_seen"])).total_seconds()
    online = age < SESSION_STALE_S and _pid_alive(session["pid"])
    out: dict[str, Any] = {"session": "online" if online else "offline", "session_seen": session["last_seen"]}
    if online and session.get("cwd"):
        out["session_cwd"] = session["cwd"]
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
        # (0 = the leader's task, task created_at, tie-break, task_id): the leader's tasks first, the rest in the
        # order they came (D-049). created_at is the ledger's, so recover() (newest first) keeps that order too.
        self._queues: dict[str, asyncio.PriorityQueue[tuple[int, str, int, str]]] = {}
        # internal subtasks of an auto_worker post (D-073): their own pool (max_concurrent), so the brain's queue
        # (one at a time: one conversation) never waits behind a long sub
        self._internal: dict[str, asyncio.PriorityQueue[tuple[int, str, int, str]]] = {}
        self._arrivals = itertools.count()
        self._queued: dict[str, set[str]] = {}               # task ids queued or running, per agent
        self._running: dict[str, asyncio.Task] = {}          # task_id -> runner task
        self._retry: set[str] = set()                        # failed runs to lay out once more (D-040)
        self._recheck: set[str] = set()                      # recovered tasks whose old worker still ran
        self._cancel_requested: set[str] = set()
        self._interrupted: dict[str, str] = {}               # task_id -> "interrupt" | "pause": a run stopped on purpose
        self._background: set[asyncio.Task] = set()          # e.g. observer-copy checks against the task KV
        self._outbox_wake = asyncio.Event()
        self.started = asyncio.Event()
        self.code_version = _code_version()
        self._stopping = False

    # ---- lifecycle ----------------------------------------------------

    async def start(self) -> None:
        # one daemon per data dir, and none while retire-agent changes the node
        lock = daemon_lock(self.cfg)
        lock.__enter__()
        self._daemon_lock = lock
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
                self._queues[addr] = asyncio.PriorityQueue()
                brains = 1 if agent.auto_worker else max(1, agent.max_concurrent)   # a brain runs one at a time
                for i in range(brains):
                    self._spawn(self._runner(agent, addr, self._queues[addr]), f"run:{addr}:{i}")
                if agent.auto_worker:
                    self._internal[addr] = asyncio.PriorityQueue()
                    for i in range(max(1, agent.max_concurrent)):
                        self._spawn(self._runner(agent, addr, self._internal[addr]), f"sub:{addr}:{i}")
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
        # background jobs (e.g. the unbounded observer-copy verifier) use the Hub too: end them before closing it ;
        # recover() schedules the verifiers again at the next start
        background = list(self._background)
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        self._background.clear()
        try:
            if self.hub:
                with contextlib.suppress(Exception):
                    await self._publish_cards(state_override="offline")
                await self.hub.close()
        finally:
            # its loops have ended: the lock goes even if going offline is cancelled or fails
            if (lock := getattr(self, "_daemon_lock", None)) is not None:
                self._daemon_lock = None
                lock.__exit__(None, None, None)
        log.info("node %s stopped", self.cfg.node)

    async def run_forever(self) -> None:
        try:
            await self.start()          # a failed start is cleaned up too: its lock let go
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
        # every open task, not just the newest page
        for task in hub.ledger.tasks(role="owner", statuses=OPEN_STATES, limit=None):
            agent = self._agent_cfg(task["owner"])
            if agent is not None and task.get("paused"):
                # paused until resumed (D-089); a worker that outlived the old node is stopped
                await self._stop_old_worker(task)
                continue
            if agent is None:
                continue                # gone from the config
            if hub.ledger.jobs(task["task_id"]):
                continue        # waits on a background job: the heartbeat wakes it when the job ends
            if task["status"] == "WAITING" and (ended := hub.ledger.jobs(task["task_id"], open_only=False)):
                # Its last job ended but the wake-up was lost (a crash between end_job and _wake_for_job): make
                # it up. _wake_for_job moves the task out of WAITING first, so this happens once.
                last = max(ended, key=lambda j: j["ended_at"])
                await self._wake_for_job(task, last, last["ended"])
                continue
            if agent.mode != "worker" and not agent.auto_worker:
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
                self._failed("receive", e, address=addr)
                await asyncio.sleep(1)
                continue
            for msg in msgs:
                await self._receive_one(addr, msg)

    async def _receive_one(self, addr: str, msg) -> None:
        hub = self.hub
        try:
            env = Envelope.from_json(msg.data)
        except ProtocolError as e:
            self._failed("receive", e, address=addr)                  # recorded and dropped (D-040)
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
                    self._read_if_info(env)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._failed("handle", e, address=addr, task_id=env.task_id)
                    self.hub.ledger.mark_handled(env.message_id, "dropped", repr(e))

    async def _handle(self, agent: AgentConfig, env: Envelope) -> str | None:
        state = await self._handle_message(agent, env)
        # not a REQUEST (it starts its own work), not a RESULT (a child's end is the children job's to judge: the parent
        # wakes once all are done), not pause/resume/interrupt
        if env.type not in ("REQUEST", "RESULT") and env.body.get("next") == env.to and not env.body.get("fyi") \
                and not any(env.body.get(k) for k in ("pause", "resume", "interrupt")) \
                and state not in ("rejected", "dropped", "unverified"):
            what = env.body.get("question") or env.body.get("reason") or env.body.get("answer") \
                or env.body.get("message") or ""
            await self._wake_named(env.to, env.task_id, f"{env.type} from {env.sender} on {env.task_id}: "
                                                        f"{short(str(what), 300)}")
        return state

    def _read_if_info(self, env: Envelope) -> None:
        """ACKs and progress are read as they arrive (D-098): they are in the inbox listing (include_seen) and
        check_task, but never count as unread, so they do not pile up."""
        if is_info(env):
            self.hub.ledger.mark_info_read(env.message_id)

    async def _wake_named(self, addr: str, task_id: str | None, why: str) -> None:
        """A post with no session is named next, or its reminder fires (D-098): nothing pushes to it, so its
        brain is run again: its own task, or the task a child of it belongs to (parent_task). A task waiting on a
        job has the wait ended with the reason; a BLOCKED one is laid out again. A session that takes the task, a
        paused task, one that runs or is queued, or a closed one is left as it is. A reminder with no task wakes
        nothing."""
        agent = self._agent_cfg(addr)
        ledger = self.hub.ledger
        if agent is None or not agent.auto_worker:
            return
        targets = []
        if task_id:
            own = ledger.task(task_id, "owner")
            asked = ledger.task(task_id, "requester")
            if own and own["owner"] == addr:
                targets = [own]
            elif asked and asked["local_agent"] == addr and asked.get("parent_task"):
                parent = ledger.task(asked["parent_task"], "owner")
                targets = [parent] if parent and parent["owner"] == addr else []
        # a reminder with no task (a session set it) only lands in the inbox: it would wake every waiting task of the
        # post, each time it repeats
        for task in targets:
            # per task, as the worker's claim decides it: a session switched off, or one in another project, leaves the
            # work to the worker
            if not self._session_takes(agent, addr, task["task_id"]):
                await self._wake_task(task, why)

    async def _wake_task(self, task: dict[str, Any], why: str) -> None:
        hub, task_id = self.hub, task["task_id"]
        if (task["status"] in TERMINAL_STATES or task.get("paused") or task.get("runner") == "session"
                or task_id in self._running or task_id in self._queued.get(task["owner"], set())):
            return
        jobs = hub.ledger.jobs(task_id)
        if jobs:
            ended = f"woken: {why} (this ended your wait on every job of the task: add_job again if you still wait)"
            for job in jobs:
                hub.ledger.end_job(job["job_id"], ended)
            await self._wake_for_job(task, jobs[-1], ended)
        elif task["status"] in ("BLOCKED", "WAITING"):
            hub.ledger.update_task(task_id, "owner", attempts=0)
            await hub.owner_transition(task_id, "ACCEPTED", f"woken: {why}")
            self._enqueue(task["owner"], task_id)

    async def _handle_message(self, agent: AgentConfig, env: Envelope) -> str | None:
        if env.type == "UPDATE" and env.body.get("copy_of"):
            return self._on_observer_copy(env)
        if env.type == "UPDATE" and env.body.get("observers_add"):
            return self._on_observers_added(env)
        if env.type == "UPDATE" and env.body.get("nudge"):
            self._on_nudge(agent, env)                   # then kept in the inbox below: it wakes the session
        if env.type in ("UPDATE", "ANSWER") and (kind := self._interrupt_kind(env)):
            await self._interrupt(agent, env, kind)      # then kept in the inbox below like any message
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
        itself:
        - this node knows the task: the sender must be in the persisted ACL, else rejected;
        - this node has never heard of it (a cross-node observer): kept unverified, not shown, until the shared
          task record names the sender as its requester or owner (_verify_observer_copy)."""
        copy = env.body.get("copy_of") or {}
        if copy.get("type") not in ("REQUEST", "RESULT") or not copy.get("from") or not copy.get("to"):
            return "rejected"
        # Structural check (as documented): the copy lists both its sender and its recipient. It proves nothing by
        # itself, so the authoritative checks below still decide.
        people = set(env.body.get("participants") or [])
        if env.sender not in people or env.to not in people:
            log.warning("dropped observer copy %s: sender or recipient not in its participant list", env.short())
            return "rejected"
        known = acl(self.hub.ledger, env.task_id)
        if known:
            row = self.hub.ledger.task(env.task_id)        # every row of a task has the same requester/owner
            if env.sender not in (row["requester"], row["owner"]):
                # Content copies come from the requester or owner only; an observer's grant is relayed by the owner, so
                # an observer never authors one.
                log.warning("dropped observer copy %s: %s is not the task's requester or owner", env.short(),
                            env.sender)
                return "rejected"
            self._record_copy(env, row["requester"], row["owner"])
            return None
        self._background_job(self._verify_observer_copy(env))
        return "unverified"

    def _still_unverified(self, message_id: str) -> bool:
        return self.hub.ledger.inbound_state(message_id) in (None, "new", "unverified")

    # The copy and the owner's task record travel separately, so the record may not be there yet: look again with
    # growing gaps, then every last gap until it appears.
    VERIFY_BACKOFF_S = (0.05, 0.2, 1, 3, 10, 30)

    async def _verify_observer_copy(self, env: Envelope) -> None:
        """Accept a copy about a task this node does not know only if the shared task record (written by the
        owner's node) names its sender as requester or owner. That is a consistency check, not an authorisation
        boundary: any node credential can write the task KV (step 2, D-008). A few tries with growing gaps; if
        the record is still not there, the copy is recorded as a failure and dropped (D-040)."""
        record = None
        for gap in (0, *self.VERIFY_BACKOFF_S):
            await asyncio.sleep(gap)
            if not self.hub.bus or (gap and not self._still_unverified(env.message_id)):
                return                                     # no bus, or decided elsewhere (a second verifier)
            try:
                record = await self.hub._remote_task(env.task_id, None)
            except Exception as e:
                self._failed("observer-copy", e, address=env.to, task_id=env.task_id)
            if record:
                break
        if not record:
            self._failed("observer-copy", "no task record names its sender; dropped", address=env.to,
                         task_id=env.task_id)
            self.hub.ledger.mark_handled(env.message_id, "dropped")
            return
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
            # (a requester that adds an observer sends the copies itself: relaying would duplicate them) An observer
            # added them: as the owner, relay the copies, so that a node that never saw the task can check the sender
            # against the task record.
            from .tools import send_observer_copies
            self._background_job(send_observer_copies(self.hub, env.to, env.task_id, new))
        return None

    def _failed(self, stage: str, error: BaseException | str, address: str | None = None, task_id: str | None = None,
                attempt: int | None = None) -> None:
        """Supervision (D-040): skip, record, carry on; the next round tries again."""
        log.warning("%s failed (%s %s): %r", stage, address or "-", task_id or "-", error)
        with contextlib.suppress(Exception):
            self.hub.ledger.record_failure(stage, error, address, task_id, attempt)

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
        position = None
        if not env.body.get("internal"):              # a sub has its own line on its brain's plan (D-073)
            hub.add_inbox_line(hub.ledger.task(env.task_id, "owner"))
            position = self._position(env.to, env.task_id)
        if (agent.mode == "worker" or env.body.get("internal")
                or (agent.auto_worker and not self._session_takes(agent, env.to, env.task_id))):
            await self._accept(env.task_id, position)
            self._enqueue(env.to, env.task_id)
            await self._notify(agent, env.to, env.task_id,      # status layer only: the lead is no participant
                               f"FYI: {env.to} accepted {env.task_id} from {env.sender}: "
                               f"{short(env.body.get('objective'))}")
        else:
            # Interactive agents accept explicitly (accept_task). Tell the requester it arrived meanwhile,
            # so "delivered but not picked up yet" is distinguishable from "lost".
            await hub.reply(env.to, env.task_id, "UPDATE", {
                "state": "PENDING", "position": position,
                "message": f"delivered to the inbox of {env.to}; waiting to be accepted; {position} in the queue "
                           "(an estimate: the leader's work goes first)"})

    def _position(self, addr: str, task_id: str) -> int:
        """Where a task stands in its owner's queue (an estimate, spec v1.1 §3.1): the open work ahead of it and
        itself; the leader's work goes first (D-049)."""
        task = self.hub.ledger.task(task_id, "owner")
        mine = bool((task.get("request") or {}).get("leader"))
        ahead = [t for t in self.hub.ledger.tasks(role="owner", local_agent=addr,
                                                  statuses=("PENDING", "ACCEPTED", "RUNNING"), limit=None)
                 if not (t.get("request") or {}).get("internal") and t["task_id"] != task_id
                 and t["created_at"] <= task["created_at"]
                 and (not mine or (t.get("request") or {}).get("leader"))]
        return len(ahead) + 1

    def _check_policy(self, agent: AgentConfig, env: Envelope) -> str | None:
        if not agent.accepts(env.sender):
            return f"permission denied: {env.to} does not accept requests from {env.sender}"
        needed = REQUEST_KINDS[env.body.get("kind", "query")]
        if not agent.has(needed):
            return f"permission denied: {env.to} lacks {needed} required for kind={env.body.get('kind', 'query')}"
        if env.body.get("internal"):
            if env.sender != env.to:
                return "refused: an internal subtask is sent by an agent to itself (D-073)"
            if not agent.auto_worker:
                # a brain, its batch and its sub pool are a post's
                return f"refused: internal subtasks are for a post with auto_worker; {env.to} has none"
            parent = self.hub.ledger.task(env.body.get("parent_task") or "", "owner")
            if parent and agent.project_of(env.body) != agent.project_of(parent.get("request")):
                return (f"refused: an internal subtask belongs to its parent's project "
                        f"({agent.project_of(parent.get('request'))}), not {agent.project_of(env.body)}")
        if project := agent.project_of(env.body):
            try:
                check_token(str(project), "project")
            except InvalidAddress as e:
                return f"refused: {e}"
            if agent.project_entry(project) is None:
                return (f"project {project} is not set up on {env.to}: {agent.home(project)} is not a directory "
                        "(by that exact name, not a link) under the post directory; run post-init for it first, "
                        "or send without project")
        return None

    def _session_takes(self, agent: AgentConfig, addr: str, task_id: str) -> str | None:
        """auto_worker: why the session, not the worker, takes this task (None: the worker may run it). A session
        started in a project directory takes only that project's work; one in the post directory takes all
        (D-072). The leader's session still comes first within its project (D-032a)."""
        ledger = self.hub.ledger
        present = session_present(ledger, addr)
        if present and not (ledger.session_of(addr) or {}).get("accepting", 1):
            return None                       # online but switched off (`mutmuas <post> off`): the worker takes it
        mine = agent.session_project((ledger.session_of(addr) or {}).get("cwd")) if present else None
        if mine is None:
            return present
        task = ledger.task(task_id, "owner")
        return present if agent.project_of((task or {}).get("request")) == mine else None

    async def _accept(self, task_id: str, position: int | None = None) -> None:
        message = "accepted into queue" + (f"; {position} in the queue (an estimate)" if position else "")
        await self.hub.owner_transition(task_id, "ACCEPTED", message, msg_type="ACK",
                                        body={"state": "ACCEPTED", "message": message, "position": position})

    async def _on_cancel(self, env: Envelope) -> str | None:
        task = self.hub.ledger.task(env.task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES:
            return None
        if env.sender != task["requester"]:         # only the persisted requester may cancel its task
            log.warning("ignored CANCEL of %s from %s: not its requester %s", env.task_id, env.sender,
                        task["requester"])
            return "rejected"
        from .tools import cancel_children
        await cancel_children(self.hub, env.task_id, env.body.get("reason") or "cancelled by its requester")
        if task["status"] == "PENDING":
            # Withdrawn before anyone picked it up: close it and keep it out of the interactive inbox
            # (both the REQUEST and this CANCEL are noise to someone who never saw the request).
            await self.hub.owner_transition(env.task_id, "CANCELLED",
                                            env.body.get("reason") or "withdrawn before it was accepted")
            self.hub.ledger.mark_seen(env.task_id)
            return None
        self._stop_jobs(env.task_id)
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
            # Only the persisted owner speaks for the task: anyone else's RESULT/UPDATE must not change it .
            log.warning("ignored %s about %s from %s: not its owner %s", env.type, env.task_id, env.sender,
                        task["owner"])
            return "rejected"
        fields: dict[str, Any] = {"last_message": env.message_id}
        if env.body.get("eta"):
            fields["eta"] = env.body["eta"]          # the owner's estimate (D-076): chased once it passes
            deadline = (task.get("request") or {}).get("deadline")
            with contextlib.suppress(TypeError, ValueError):
                if (deadline and parse_iso(env.body["eta"]) > parse_iso(deadline)
                        and ledger.notice_once(env.task_id, f"eta_after_deadline:{env.body['eta']}")):
                    await self._follow_up(task, "eta_after_deadline", f"{task['owner']} expects to deliver "
                                          f"{env.task_id} by {env.body['eta']}, after your deadline {deadline}")
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
        task = self.hub.ledger.task(task_id, "owner")
        queue = self._internal.get(addr) if task["request"].get("internal") else None
        (queue or self._queues[addr]).put_nowait((0 if task["request"].get("leader") else 1, task["created_at"],
                                                  next(self._arrivals), task_id))

    async def _runner(self, agent: AgentConfig, addr: str, queue: asyncio.PriorityQueue) -> None:
        while True:
            *_, task_id = await queue.get()
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
                    if asyncio.current_task().cancelling():
                        # this runner itself is stopped, just as its run ended: stop, never back to the queue
                        raise
                    # else the run was stopped on purpose (interrupt, pause): settled below, laid out again
            finally:
                self._running.pop(task_id, None)
                if (why := self._interrupted.pop(task_id, None)) is not None:
                    # stopped before its runtime ran (in Worktree.create, in the RUNNING transition): settled here, or
                    # the stop was lost
                    await self._settle_stopped(task_id, why)
                self._queued[addr].discard(task_id)
                if task_id in self._retry:
                    self._retry.discard(task_id)
                    self._enqueue(addr, task_id)

    async def _execute(self, agent: AgentConfig, task_id: str) -> None:
        hub = self.hub
        task = hub.ledger.task(task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES or task.get("paused"):
            return                                       # done, or paused until resumed (D-089)
        if self._worker_alive(task):
            # a worker from before a restart, or a run whose stop failed: nothing runs beside it; the heartbeat
            # lays the task out once it has gone
            log.info("task %s: an earlier run of it still runs; not started again", task_id)
            self._recheck.add(task_id)
            return
        if agent.auto_worker:
            # Claim it for the worker in one transaction with the session check: never done twice, and while
            # the leader's session is there, a task that has not started yet is his to decide (D-032a).
            addr = task["owner"]
            internal = (task.get("request") or {}).get("internal")         # a sub is never the session's
            refused = hub.ledger.claim_task(task_id, "worker", ("ACCEPTED", "RUNNING"),
                                            refuse_if=None if internal else
                                            lambda: self._session_takes(agent, addr, task_id))
            if refused:
                if "session" in refused and task["status"] == "ACCEPTED":
                    # Release the claim first: a stop between the two steps leaves ACCEPTED + no runner, which recover
                    # queues again (and the claim checks the session again). The other order left PENDING +
                    # runner=worker, which nothing picked up.
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
        # a post's brain (D-073): its runs share one conversation per project while work keeps coming (claude-code;
        # Codex starts afresh each run until its session id can be read back)
        brain = agent.auto_worker and not request.body.get("internal") and agent.runtime == "claude-code"
        project = agent.project_of(request.body)
        notes = [n for n in hub.ledger.task(task_id, "owner").get("interrupts") or []]
        if notes:
            hub.ledger.update_task(task_id, "owner", interrupts=[])      # handed to this run (D-089)
        ctx = TaskContext(task_id, request, agent, self.cfg, attempt, interrupts=notes,
                          jobs=[j for j in hub.ledger.jobs(task_id, open_only=False) if j["ended_at"]],
                          resume=hub.ledger.brain_session(task["owner"], project) if brain else None)
        if brain and not ctx.resume:
            # A new conversation gets its id from the node and is recorded before the run: a run stopped midway (D-089
            # interrupt) prints no JSON, and the next run must still resume it.
            ctx.session_id = str(uuid.uuid4())
            hub.ledger.set_brain_session(task["owner"], project, ctx.session_id)
        spawned: list[int] = []                 # this run's process (every mode: a failed stop is watched)

        def on_spawn(pid: int) -> None:
            spawned.append(pid)
            self._record_worker(task_id, pid)
        ctx.on_spawn = on_spawn
        wt = None
        if request.body.get("kind") == "code" and agent.copies_code:
            inputs = request.body.get("inputs")
            base_ref = inputs.get("base_ref", "HEAD") if isinstance(inputs, dict) else "HEAD"
            try:
                wt = await Worktree.create(Path(agent.repo), self.cfg.node, agent.id, task_id, base_ref)
            except GitError as e:
                await self._run_failed(agent, task_id, attempt,
                                       result_body("failed", f"could not create worktree: {e}"))
                return
            ctx.workdir, ctx.git_branch = wt.path, wt.branch
        where = f", branch {wt.branch}" if wt else ""
        await hub.owner_transition(task_id, "RUNNING", f"started (attempt {attempt}, runtime {agent.runtime}{where})")
        try:
            outcome = await asyncio.wait_for(make_runtime(agent, self.cfg).run(ctx), timeout)
        except asyncio.TimeoutError:
            self._mark_unstopped(task_id, spawned)
            await self._run_failed(agent, task_id, attempt, result_body(
                "failed", f"timed out after {timeout:.0f}s", limitations=["process was killed at the deadline"]))
            return
        except asyncio.CancelledError:
            self._mark_unstopped(task_id, spawned)
            if (why := self._interrupted.pop(task_id, None)) is not None:
                await self._settle_stopped(task_id, why)
                return
            if task_id in self._cancel_requested:
                self._cancel_requested.discard(task_id)
                await hub.owner_transition(task_id, "CANCELLED", "cancelled by requester; process stopped")
                return
            raise
        except Exception as e:
            self._mark_unstopped(task_id, spawned)       # its cleanup may have failed too
            await self._run_failed(agent, task_id, attempt, result_body("failed", f"runtime error: {e!r}"))
            return

        if brain:
            if outcome.exit_code != 0 and ctx.resume:
                hub.ledger.forget_brain(task["owner"], project)      # e.g. the conversation is gone: next run afresh
            elif outcome.session_id:
                hub.ledger.set_brain_session(task["owner"], project, outcome.session_id)
        current = hub.ledger.task(task_id, "owner")
        if current["status"] in TERMINAL_STATES:
            return      # the agent already closed the task through its tools
        if hub.ledger.jobs(task_id):
            return      # it waits on a background job it registered: the heartbeat wakes it when the job ends
        if current["status"] == "BLOCKED" and not current.get("result_draft"):
            return      # blocked and nothing to deliver: wait for the requester
        if outcome.limit and not current.get("result_draft"):
            # Stopped at the worker's turn/cost limit (D-066) without a result: a failed run (R5.4)
            await self._run_failed(agent, task_id, attempt, result_body(
                "failed", f"the worker stopped at its limit ({outcome.limit}) without a result",
                limitations=["raise max_turns / max_cost_usd in node.yaml, or split the task"]))
            return
        # A submitted result always wins over an earlier BLOCKED report.
        body, refs = _result_from(current.get("result_draft"), outcome)
        if outcome.exit_code != 0 and not _structured(current.get("result_draft") or outcome.result):
            await self._run_failed(agent, task_id, attempt, body, refs)     # it died without a result
            return
        if wt:
            await self._attach_git(wt, agent, task_id, body, refs)
        await hub.finish(task_id, body, refs)
        await self._notify(agent, current["owner"], task_id,
                           f"FYI: {current['owner']} finished {task_id} for {current['requester']} "
                           f"({body['status']})")

    async def _run_failed(self, agent: AgentConfig, task_id: str, attempt: int, body: dict[str, Any],
                          refs: list[ArtifactRef] | None = None) -> None:
        """Supervision (D-040): a failed run (crash, timeout, runtime error) is recorded and laid out once more;
        the next failure finishes the task as failed. An agent's own 'failed' verdict is not a failed run."""
        self._failed("run", body["summary"], address=self.hub.ledger.task(task_id, "owner")["owner"],
                     task_id=task_id, attempt=attempt)
        if attempt < agent.max_attempts:
            await self.hub.owner_transition(task_id, "ACCEPTED", f"attempt {attempt} failed ({body['summary']}); "
                                                                 "running it once more")
            self._retry.add(task_id)                 # queued again once the runner has let go of it
            return
        await self.hub.finish(task_id, body, refs or [])

    async def _notify(self, agent: AgentConfig, sender: str, task_id: str, text: str) -> None:
        """Copy a node's lead (agent.notify) on work its workers take on. Best effort, informational only."""
        for target in agent.notify:
            try:
                await self.hub.send(Envelope(type="UPDATE", sender=sender, to=target, task_id=task_id,
                                             body={"message": text, "fyi": True}))
            except Exception as e:
                self._failed("notify", e, address=target, task_id=task_id)

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
                await self._check_jobs()
                await self._fire_reminders()
                await self._expire_brains()
                for task_id in list(self._recheck):         # old workers still running at the last look
                    task = self.hub.ledger.task(task_id, "owner")
                    if task and task["status"] not in TERMINAL_STATES:
                        await self._recover_auto(task)
                    else:
                        self._recheck.discard(task_id)
                if asyncio.get_running_loop().time() - last_follow_up >= FOLLOW_UP_EVERY_S:
                    last_follow_up = asyncio.get_running_loop().time()
                    await self._follow_ups()
                    await self._chase_etas()
                await self._release_held()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._failed("heartbeat", e)

    async def _settle_stopped(self, task_id: str, why: str) -> None:
        """A run stopped on purpose (D-089): paused, or laid out again with the message for the next run. The run
        does not count as an attempt. A pause resumed while the run was still stopping goes on as an interrupt
."""
        hub = self.hub
        task = hub.ledger.task(task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES:
            return
        hub.ledger.update_task(task_id, "owner", attempts=max(0, task["attempts"] - 1))
        if why == "pause" and task.get("paused"):
            await hub.owner_transition(task_id, "WAITING", "paused: the run was stopped; waits for resume")
        else:
            await hub.owner_transition(task_id, "ACCEPTED", "interrupted: the run was stopped and is laid "
                                                            "out again with the new message")
            self._retry.add(task_id)

    def _worker_alive(self, task: dict[str, Any]) -> bool:
        """An earlier run of this task still runs: its leader (pid and start time), or its process group when a
        stop of it failed."""
        if same_process(task.get("runner_pid"), task.get("runner_start")):
            return True
        return bool(task.get("stuck_pgid")) and group_alive(task["stuck_pgid"])

    def _old_worker(self, task: dict[str, Any] | None) -> bool:
        """A worker from before a restart (or left by a failed stop) still runs this task, not one of our runs."""
        return bool(task) and task["task_id"] not in self._running and self._worker_alive(task)

    def _mark_unstopped(self, task_id: str, spawned: list[int]) -> None:
        """A run of ours was stopped: if its process group still lives (the stop failed), remember the group, so
        nothing runs beside it and the heartbeat watches it."""
        if spawned and group_alive(spawned[-1]):
            self.hub.ledger.update_task(task_id, "owner", stuck_pgid=spawned[-1])
            self._recheck.add(task_id)
            if self.hub.ledger.notice_once(task_id, f"stop-failed:{spawned[-1]}"):
                self._failed("interrupt", f"could not stop the run's process group {spawned[-1]}; nothing runs "
                                          "beside it, checked again each heartbeat", task_id=task_id)

    async def _stop_old_worker(self, task: dict[str, Any]) -> str | None:
        """Stop the process group of a worker from before a restart (not one of ours, so it cannot be cancelled):
        None if there is none, "stopped", or "failed": then it stays on the heartbeat's recheck and nothing runs
        next to it."""
        if not self._old_worker(task):
            return None
        task_id = task["task_id"]
        pid = task.get("stuck_pgid") or task["runner_pid"]     # started in its own session: group id = pid
        if await stop_group(pid):
            self.hub.ledger.update_task(task_id, "owner", stuck_pgid=None)
            self._recheck.discard(task_id)
            return "stopped"
        self.hub.ledger.update_task(task_id, "owner", stuck_pgid=pid)   # watched as a group, beyond its leader
        self._recheck.add(task_id)
        if self.hub.ledger.notice_once(task_id, f"stop-failed:{pid}"):
            self._failed("interrupt", f"could not stop the old worker's process group {pid}; checked again each "
                                      "heartbeat", address=task["owner"], task_id=task_id)
        return "failed"

    async def _check_jobs(self) -> None:
        """Background jobs (D-050): one has ended when its process is gone or its done-file is there. A task whose
        last job ended is woken: a worker's is queued again (a fresh run, attempts reset), a session is told."""
        hub = self.hub
        for job in hub.ledger.jobs():
            done = Path(job["done_file"]) if job["done_file"] else None
            if job["children"]:
                if (ended := self._children_ended(job["task_id"])) is None:
                    continue
            elif done and done.exists():
                ended = f"done-file {done} appeared: {done.read_text(errors='replace')[:300].strip()!r}"
            elif job["pid"] and not same_process(job["pid"], job["pid_start"]):
                ended = f"process {job['pid']} ended (exit code unknown: write it to a done-file to pass it on)"
            else:
                continue
            hub.ledger.end_job(job["job_id"], ended)
            task = hub.ledger.task(job["task_id"], "owner")
            if task["status"] in TERMINAL_STATES or hub.ledger.jobs(job["task_id"]):
                continue            # closed meanwhile, or still waiting on another job
            if task.get("paused"):
                # paused: stays WAITING; resume runs it with this news
                self._note_interrupt(job["task_id"], f"background job ended while paused: {ended}; "
                                                     f"log {job['log'] or '-'}; note {job['note'] or '-'}")
                continue
            await self._wake_for_job(task, job, ended)

    def _children_ended(self, task_id: str) -> str | None:
        """D-066: a wait on the direct child tasks ends once each has a result, was refused or cancelled, or is past
        its deadline (then it is reported overdue and left running: the parent decides). None while one is open.
        A child is reported overdue once: a parent that waits again afterwards waits for its real end, or every
        heartbeat would wake it again. There is no way to extend a child's
        deadline, and a worker cannot be relied on to cancel, so the node keeps this rule itself."""
        now = datetime.now(timezone.utc)
        lines, overdue = [], []
        ledger = self.hub.ledger
        for child in ledger.children(task_id):
            head = f"{child['task_id']} ({child['owner']})"
            if child["status"] in TERMINAL_STATES:
                result = child.get("result") or {}
                lines.append(f"{head}: {child['status']} {result.get('status') or ''} - "
                             f"{short(result.get('summary') or '', 200)}")
                continue
            deadline = (child.get("request") or {}).get("deadline")
            with contextlib.suppress(TypeError, ValueError):
                if (deadline and parse_iso(deadline) < now
                        and not ledger.noticed(child["task_id"], "overdue_wake")):
                    lines.append(f"{head}: overdue (deadline {deadline}, still {child['status']}; not cancelled)")
                    overdue.append(child["task_id"])
                    continue
            return None
        for child_id in overdue:
            ledger.notice_once(child_id, "overdue_wake")
        return "child tasks done: " + "; ".join(lines)

    def _stop_jobs(self, task_id: str) -> None:
        """A cancelled task's background jobs: a job whose process still runs gets SIGTERM (that pid only, R4.1);
        a done-file-only job has no process the node knows and is not stopped. Either way it is closed, so it
        wakes nobody."""
        for job in self.hub.ledger.jobs(task_id):
            ended = "cancelled with its task"
            if job["pid"] and same_process(job["pid"], job["pid_start"]):
                os.kill(job["pid"], signal.SIGTERM)
                ended += f"; SIGTERM sent to pid {job['pid']}"
            elif job["children"]:
                ended += "; its open child tasks are cancelled"
            elif not job["pid"]:
                ended += "; a done-file-only job is not stopped (the node knows no process for it)"
            self.hub.ledger.end_job(job["job_id"], ended)

    async def _wake_for_job(self, task: dict[str, Any], job: dict[str, Any], ended: str) -> None:
        hub, task_id, owner = self.hub, task["task_id"], task["owner"]
        text = f"background job ended: {ended}; log {job['log'] or '-'}; note {job['note'] or '-'}"
        agent = self._agent_cfg(owner)
        if agent.mode == "worker" or (agent.auto_worker and task.get("runner") != "session"):
            hub.ledger.update_task(task_id, "owner", attempts=0)
            await hub.owner_transition(task_id, "ACCEPTED", text)
            self._enqueue(owner, task_id)
            return
        # A session: a note in its own inbox that hands it the baton (next), which wakes it like a REQUEST. The task
        # leaves WAITING first, so recover() cannot wake it a second time.
        await hub.owner_transition(task_id, "RUNNING", text)
        wake = Envelope(type="UPDATE", sender=owner, to=owner, task_id=task_id, body={"message": text, "next": owner})
        hub.ledger.ingest(wake)
        hub.ledger.mark_handled(wake.message_id)

    def _interrupt_kind(self, env: Envelope) -> str | None:
        """D-089: a trusted controller (node.yaml trusted_controllers; the leader's word comes relayed by the
        secretary) interrupts a worker's run: a message about the task it runs, or any marked interrupt; it may pause
        and resume any task here. Anyone else may pause / resume only a task it requested from this address.
        priority and body.leader are not permissions: they only order what is allowed."""
        trusted = env.sender in self.cfg.trusted_controllers
        task = self.hub.ledger.task(env.task_id, "owner")
        if env.body.get("pause") or env.body.get("resume"):
            if task and task["owner"] == env.to and (trusted or env.sender == task["requester"]):
                return "pause" if env.body.get("pause") else "resume"
            return None
        if trusted and (env.body.get("interrupt") or env.task_id in self._running
                        or (task and task["owner"] == env.to and self._old_worker(task))):
            return "interrupt"
        return None

    async def _interrupt(self, agent: AgentConfig, env: Envelope, kind: str) -> None:
        hub, addr = self.hub, env.to
        text = f"{env.type} from {env.sender} on {env.task_id}: {short(env.body.get('message') or env.body.get('answer') or '', 400)}"
        if kind == "interrupt":
            targets = [t for t, r in self._running.items() if not r.done()
                       and (hub.ledger.task(t, "owner") or {}).get("owner") == addr
                       and (t == env.task_id or env.body.get("interrupt"))]
            for task_id in targets:
                self._note_interrupt(task_id, text)
                self._interrupted[task_id] = "interrupt"
                self._running[task_id].cancel()
            # workers from before a restart: of this task, or of every task of the post (interrupt: true)
            olds = (hub.ledger.tasks(role="owner", local_agent=addr, statuses=("ACCEPTED", "RUNNING"), limit=None)
                    if env.body.get("interrupt") else [hub.ledger.task(env.task_id, "owner")])
            for task in olds:
                if not (task and task["owner"] == addr and self._old_worker(task)):
                    continue
                self._note_interrupt(task["task_id"], text)
                if await self._stop_old_worker(task) != "stopped":
                    continue                                            # rechecked, nothing runs beside it
                if hub.ledger.task(task["task_id"], "owner").get("paused"):
                    # paused: stopped, and still waits for resume; the message waits on the task (Codex fourth
                    # review: it was laid out without a resume)
                    if task["status"] != "WAITING":
                        await hub.owner_transition(task["task_id"], "WAITING", f"paused; interrupted by "
                                                                               f"{env.sender}: its old run stopped")
                    continue
                hub.ledger.update_task(task["task_id"], "owner", attempts=0)
                await hub.owner_transition(task["task_id"], "ACCEPTED", f"interrupted by {env.sender}: the run "
                                                                       "from before the restart was stopped")
                self._enqueue(addr, task["task_id"])
            return
        task = hub.ledger.task(env.task_id, "owner")
        if task is None or task["status"] in TERMINAL_STATES:
            return
        if kind == "pause":
            hub.ledger.update_task(env.task_id, "owner", paused=1)
            self._note_interrupt(env.task_id, "paused by " + text)
            runner = self._running.get(env.task_id)
            if runner and not runner.done():
                self._interrupted[env.task_id] = "pause"
                runner.cancel()
            elif await self._stop_old_worker(task) != "failed":
                await hub.owner_transition(env.task_id, "WAITING", f"paused by {env.sender}")
        else:
            hub.ledger.update_task(env.task_id, "owner", paused=0)
            self._note_interrupt(env.task_id, "resumed by " + text)
            if env.task_id in self._running:
                # its stopped run is still settling (and still queued): laid out once it has left (Codex re-review
                # of cb77a33: a resume during the pause's WAITING notice was lost)
                self._retry.add(env.task_id)
            if hub.ledger.jobs(env.task_id):
                pass        # still waits on a background job: its end wakes the task
            elif (agent.mode == "worker" or agent.auto_worker) and task.get("runner") != "session":
                await hub.owner_transition(env.task_id, "ACCEPTED", f"resumed by {env.sender}")
                self._enqueue(addr, env.task_id)
            else:
                await hub.owner_transition(env.task_id, "RUNNING", f"resumed by {env.sender}")
        await self._cascade(env, kind)

    def _note_interrupt(self, task_id: str, text: str) -> None:
        task = self.hub.ledger.task(task_id, "owner")
        self.hub.ledger.update_task(task_id, "owner", interrupts=[*(task.get("interrupts") or []), text])

    async def _cascade(self, env: Envelope, kind: str) -> None:
        """Pause and resume travel down parent_task to the open child tasks, on whatever node they run (D-089)."""
        held = self.hub.ledger.held_ids()
        for child in self.hub.ledger.children(env.task_id):
            if child["status"] in TERMINAL_STATES:
                continue
            if child["task_id"] in held:
                # not sent yet (depends_on): it waits here, paused, until resumed
                self.hub.ledger.update_task(child["task_id"], "requester", paused=int(kind == "pause"))
                continue
            await self.hub.send(Envelope(
                type="UPDATE", sender=child["local_agent"], to=child["owner"], task_id=child["task_id"],
                priority=env.priority, body={"message": f"{kind} (from parent {env.task_id}): "
                                                        f"{env.body.get('message') or ''}",
                                             kind: True, "leader": bool(env.body.get("leader")),
                                             "next": child["owner"]}))

    def _on_nudge(self, agent: AgentConfig, env: Envelope) -> None:
        """A nudge from the requester (or an eta chase): a worker's task that is neither running nor queued is
        laid out again (a stalled worker); a session is woken by the message itself (next)."""
        task = self.hub.ledger.task(env.task_id, "owner")
        if (not task or env.sender != task["requester"] or task["status"] not in ("ACCEPTED", "RUNNING")
                or not (agent.mode == "worker" or agent.auto_worker) or task.get("runner") == "session"):
            return
        if env.task_id in self._queued.get(task["owner"], set()) or same_process(task.get("runner_pid"),
                                                                                   task.get("runner_start")):
            return
        self._enqueue(task["owner"], env.task_id)

    async def _chase_etas(self) -> None:
        """催办 (D-076): a request of ours whose owner's eta has passed gets one reminder that asks for a new eta;
        no answer within ETA_REPLY_GRACE_S: the requester is told (and the escalation addresses, e.g. the
        secretary). A new eta starts over. Work that waits on a job or on subtasks (WAITING) or on the requester
        (BLOCKED) is not chased; an owner whose node is offline is not chased either (the clock waits); a session
        that looks stuck (gone, and no worker to take over) is reported, not chased."""
        hub = self.hub
        now = datetime.now(timezone.utc)
        for t in hub.ledger.tasks(role="requester", statuses=OPEN_STATES, limit=None):
            eta = t.get("eta")
            if not eta or t["status"] in ("WAITING", "BLOCKED"):
                continue
            with contextlib.suppress(TypeError, ValueError):
                if parse_iso(eta) > now:
                    continue
            card = await hub.card_or_none(t["owner"])
            if not (card and card.get("online")):
                continue
            requester = t["local_agent"]
            stuck = card.get("mode") == "interactive" and not card.get("auto_worker") and card.get("session") != "online"
            if stuck:
                if hub.ledger.notice_once(t["task_id"], f"stuck:{eta}"):
                    await self._follow_up(t, "stuck", f"{t['owner']}'s session is gone and nothing else takes "
                                                      f"{t['task_id']} (eta {eta} passed)")
                continue
            if hub.ledger.notice_once(t["task_id"], f"eta:{eta}"):
                await hub.send(Envelope(type="UPDATE", sender=requester, to=t["owner"], task_id=t["task_id"], body={
                    "message": f"eta {eta} has passed for {t['task_id']}: reply with a new eta (report_progress "
                               "eta=…) and what you are doing or where you are stuck", "next": t["owner"],
                    "nudge": True, "chase": eta}))
                continue
            sent = hub.ledger.db.execute("SELECT created_at FROM notices WHERE task_id=? AND reason=?",
                                         (t["task_id"], f"eta:{eta}")).fetchone()
            if (sent and (now - parse_iso(sent["created_at"])).total_seconds() >= ETA_REPLY_GRACE_S
                    and hub.ledger.notice_once(t["task_id"], f"eta-silent:{eta}")):
                await self._follow_up(t, "eta_silent", f"{t['owner']} gave no new eta after its eta {eta} passed "
                                                       f"for {t['task_id']}: wait, or hand it to someone else")

    async def _release_held(self) -> None:
        """Requests held until the tasks they depend on are done (depends_on): sent with those results and their
        artifacts once all are done; if one failed, was refused or withdrawn, not sent: the requester is told."""
        hub = self.hub
        for held in hub.ledger.held():
            if (hub.ledger.task(held["task_id"], "requester") or {}).get("paused"):
                continue                                      # its parent is paused (D-089)
            deps = {d: hub.ledger.task(d) for d in held["depends_on"]}
            failed = [d for d, t in deps.items() if t and t["status"] in TERMINAL_STATES
                      and (t["status"] != "COMPLETED" or t.get("result_status") == "failed")]
            env = held["envelope"]
            if failed:
                hub.ledger.drop_held(env.task_id)
                text = f"{env.task_id} was not sent: it depends on {', '.join(failed)}, which did not complete"
                hub.ledger.update_task(env.task_id, "requester", status="FAILED", result_status="failed",
                                       result=result_body("failed", text))
                note = Envelope(type="UPDATE", sender=env.sender, to=env.sender, task_id=env.task_id,
                                body={"message": text, "next": env.sender})
                hub.ledger.ingest(note)
                hub.ledger.mark_handled(note.message_id)
                continue
            if not all(t and t["status"] == "COMPLETED" for t in deps.values()):
                continue
            # what travels with it is checked again for its sender now: only tasks it takes part in, only artifacts it
            # may see itself
            sender = env.sender
            deps = {d: t for d, t in deps.items() if is_participant(hub.ledger, sender, d)}
            env.body["dependencies"] = [{"task_id": d, "owner": t["owner"], "status": t.get("result_status"),
                                         "summary": short((t.get("result") or {}).get("summary") or "", 300)}
                                        for d, t in deps.items()]
            env.artifacts = [*env.artifacts, *(ArtifactRef.from_dict(a) for t in deps.values()
                                               for a in t.get("output_refs") or []
                                               if artifact_visible(hub.ledger, sender, a.get("uri", "")))]
            if (after := env.body.pop("deadline_after_s", None)) and not env.body.get("deadline"):
                # a relative deadline (+2h) given with depends_on counts from now, its release (D-098)
                env.body["deadline"] = (datetime.now(timezone.utc) + timedelta(seconds=after)
                                        ).isoformat(timespec="seconds")
            if not env.body.get("deadline") and env.body.get("reply") != "none":
                from .tools import default_reply_deadline     # its reply clock starts now that it is sent
                if (due := default_reply_deadline(hub, env.body.get("timeout_s"), env.body.get("kind"))):
                    env.body.update(deadline=due, deadline_default=True)
            if not hub.ledger.release_held(env):
                continue                                      # withdrawn meanwhile: never sent
            await hub.try_publish(env)
            await hub.copy_to_observers(sender, env, env.body.get("observers") or [])

    async def _expire_brains(self) -> None:
        """End a brain batch (D-073) after brain_batch_idle_s without a brain run, unless one of the post's subs
        still runs or one of its tasks waits on a job: the next run then starts a new conversation and picks up
        from HANDOFF/PLAN. The brain updates those every run, so nothing is lost by forgetting the id."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.cfg.brain_batch_idle_s)
        ledger = self.hub.ledger
        for row in ledger.brains():
            agent = self._agent_cfg(row["local_agent"])
            if agent is None or parse_iso(row["updated_at"]) > cutoff:
                continue
            # busy, per project: a sub of it still open, a task waiting on a job, or a brain run under way or queued
            busy = [t for t in ledger.tasks(role="owner", local_agent=row["local_agent"], statuses=OPEN_STATES,
                                            limit=None)
                    if (agent.project_of(t.get("request")) or "") == row["project"]
                    and ((t.get("request") or {}).get("internal") or ledger.jobs(t["task_id"])
                         or t["status"] in ("RUNNING", "ACCEPTED"))]
            if not busy:
                ledger.forget_brain(row["local_agent"], row["project"])

    async def _fire_reminders(self) -> None:
        """Due reminders (D-066) become a note in the agent's own inbox that hands it the baton: it wakes a session
        like a REQUEST does and waits there while none runs. No session lease involved. A repeating one comes back
        one interval after now (a node that was down does not fire the missed ones in a burst)."""
        now = datetime.now(timezone.utc)
        for r in self.hub.ledger.due_reminders(now.isoformat(timespec="milliseconds")):
            if self._agent_cfg(r["local_agent"]) is None:
                continue                                    # not an agent of this node any more
            again = (now + timedelta(seconds=r["every_s"])).isoformat(timespec="milliseconds") if r["every_s"] \
                else None
            if not self.hub.ledger.fire_reminder(r, again):
                continue
            text = f"reminder {r['id']} (set {r['created_at']}): {r['text']}"
            if again:
                text += f" [repeats every {r['every_s']:.0f} s; next {again}; cancel_reminder({r['id']}) stops it]"
            note = Envelope(type="UPDATE", sender=r["local_agent"], to=r["local_agent"],
                            task_id=f"reminder-{r['id']}", body={"message": text, "next": r["local_agent"]})
            self.hub.ledger.ingest(note)
            self.hub.ledger.mark_handled(note.message_id)
            await self._wake_named(r["local_agent"], r.get("task_id"), text)     # a post with no session (D-098)

    async def _follow_ups(self) -> None:
        """Chase replies this node is owed (like an email client's follow-up flag), each once:
        - overdue: reply required, deadline passed, no RESULT yet;
        - session_offline: the owner is an interactive agent whose node is up but whose session is gone,
          so the request sits unread (leader's rule: no session found = that terminal is offline).
        Nothing is chased while the owner's node itself is offline (a closed laptop): the clock waits."""
        hub = self.hub
        now = datetime.now(timezone.utc)
        held = hub.ledger.held_ids()            # not sent yet: nobody owes us a reply
        for t in hub.ledger.tasks(role="requester", statuses=OPEN_STATES, limit=None):
            if t["task_id"] in held:
                continue
            request = t.get("request") or {}
            if not reply_required(request):
                continue
            card = await hub.card_or_none(t["owner"])
            if not (card and card.get("online")):
                continue
            deadline = request.get("deadline")
            with contextlib.suppress(TypeError, ValueError):
                # an owner that gave an eta is chased by its eta (_chase_etas) while that eta is ahead and the task is
                # one _chase_etas chases (not WAITING / BLOCKED), not as overdue
                eta_chased = (t.get("eta") and parse_iso(t["eta"]) > now and t["status"] not in ("WAITING", "BLOCKED"))
                if (deadline and not eta_chased and parse_iso(deadline) < now
                        and hub.ledger.notice_once(t["task_id"], "overdue")):
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
                self._failed("follow-up", e, address=target, task_id=task["task_id"])

    def _record_worker(self, task_id: str, pid: int) -> None:
        self.hub.ledger.set_runner_pid(task_id, pid, proc_start(pid))

    async def _recover_auto(self, task: dict[str, Any]) -> None:
        """auto_worker after a restart: deliver the draft its worker submitted, or run the task again. A worker
        from before the restart that still runs is not stopped: skip it this round, record it once, look again at
        the next heartbeat (D-040). A PENDING task still claimed by the worker is released for _auto_dispatch; a
        session's task stays the session's."""
        hub, task_id = self.hub, task["task_id"]
        if task.get("runner") == "session":
            return
        if task.get("paused"):
            # paused: its old worker is stopped (again, while it runs); then it waits for resume
            if await self._stop_old_worker(task) == "failed":
                return
            task = hub.ledger.task(task_id, "owner")    # as it is now: a resume may have come while it stopped
            if task.get("paused"):
                self._recheck.discard(task_id)
                if task["status"] != "WAITING":
                    await hub.owner_transition(task_id, "WAITING", "paused: its old worker has stopped")
                return
        if task["status"] == "PENDING":
            if task.get("runner") == "worker":
                hub.ledger.release_task(task_id, "worker")
            return
        if self._worker_alive(task):
            if hub.ledger.notice_once(task_id, "old-worker-running"):
                self._failed("recover", "a worker from before the restart still runs; checked again each heartbeat",
                             address=task["owner"], task_id=task_id)
            self._recheck.add(task_id)
            return
        self._recheck.discard(task_id)
        if task.get("stuck_pgid"):
            hub.ledger.update_task(task_id, "owner", stuck_pgid=None)          # its group has gone
        if await self._deliver_draft(task_id):
            return
        hub.ledger.set_runner_pid(task_id, None)
        if task["status"] != "ACCEPTED":
            await hub.owner_transition(task_id, "ACCEPTED", f"node {self.cfg.node} restarted; task will be run again")
        self._enqueue(task["owner"], task_id)

    async def _deliver_draft(self, task_id: str) -> bool:
        """A worker that submitted its result and ended while the daemon was down: deliver that result rather
        than running the task again."""
        current = self.hub.ledger.task(task_id, "owner")
        draft = (current or {}).get("result_draft")
        if not draft or current["status"] in TERMINAL_STATES:
            return False
        draft = dict(draft)
        refs = [ArtifactRef.from_dict(a) for a in draft.pop("artifacts", None) or []]
        await self.hub.finish(task_id, draft, refs)
        return True

    async def _auto_dispatch(self) -> None:
        """auto_worker agents: run the waiting tasks no session takes (none there past the grace period, or one
        working on another project)."""
        hub = self.hub
        for agent in self.cfg.agents:
            addr = str(Address(self.cfg.node, agent.id))
            if not agent.auto_worker:
                continue
            for task in hub.ledger.tasks(role="owner", local_agent=addr, statuses=("PENDING",), limit=None):
                if (task.get("runner") is None and task["task_id"] not in self._queued[addr]
                        and not self._session_takes(agent, addr, task["task_id"])):
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
                **(public_session(session_fields(hub.ledger.session_of(addr)))
                   if agent.mode == "interactive" else {}),
                "heartbeat_s": self.cfg.heartbeat_s, "last_heartbeat": now}
            await bus.kv_put(bus.names.agents_kv, f"{self.cfg.node}.{agent.id}",
                             {k: v for k, v in card.items() if k in CARD_KEYS})

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
                self._failed("outbox", e)

    def _agent_cfg(self, address: str) -> AgentConfig | None:
        addr = Address.parse(address)
        return next((a for a in self.cfg.agents if a.id == addr.agent and addr.node == self.cfg.node), None)


def _structured(candidate: dict[str, Any] | None) -> bool:
    return bool(candidate and candidate.get("status") in ("complete", "partial", "failed") and candidate.get("summary"))


def _result_from(draft: dict[str, Any] | None, outcome) -> tuple[dict[str, Any], list[ArtifactRef]]:
    """Pick the agent's structured result. Never upgrade an unstructured finish to 'complete'."""
    candidate = draft or outcome.result
    if _structured(candidate):
        refs = [ArtifactRef.from_dict(a) for a in candidate.get("artifacts", [])]
        body = {k: v for k, v in candidate.items() if k != "artifacts"}
        if outcome.exit_code != 0 and body["status"] == "complete":
            body["status"] = "partial"
            body.setdefault("limitations", []).append(f"agent process exited with code {outcome.exit_code}")
        return body, refs
    status = "failed" if outcome.exit_code != 0 else "partial"
    # The raw output and run log are private: the requester gets the kind of error only (e.g. "quota"), the text stays
    # in the log on the owner's node.
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
