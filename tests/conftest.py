"""Shared fixtures: a throwaway NATS server and in-process nodes A/B on localhost.

Each node gets its own data dir (own SQLite ledger), exactly like separate
machines; they only share the NATS server.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from mutmuas.config import load_config
from mutmuas.hub import Hub
from mutmuas.node import NodeDaemon

ROOT = Path(__file__).resolve().parents[1]
HANDLERS = Path(__file__).parent / "handlers"


def nats_binary() -> str:
    for candidate in (os.environ.get("NATS_SERVER_BIN"), str(ROOT / ".local/bin/nats-server"),
                      shutil.which("nats-server")):
        if candidate and Path(candidate).exists():
            return candidate
    pytest.skip("nats-server not found (run scripts/install.sh or set NATS_SERVER_BIN)")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class NatsServer:
    """A nats-server process that can be stopped and restarted on the same port/store."""

    def __init__(self, store: Path, conf: Path | None = None):
        self.port = free_port()
        self.store = store
        self.conf = conf
        self.proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def start(self) -> None:
        if self.conf:   # the config carries jetstream/store_dir/auth itself
            argv = [nats_binary(), "-c", str(self.conf), "-a", "127.0.0.1", "-p", str(self.port)]
        else:
            argv = [nats_binary(), "-js", "-a", "127.0.0.1", "-p", str(self.port), "-sd", str(self.store)]
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("nats-server did not start")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(10)
        self.proc = None


@pytest.fixture
def nats(tmp_path):
    server = NatsServer(tmp_path / "jetstream")
    server.start()
    yield server
    server.stop()


def worker(id: str, script: str, **extra) -> dict:
    return {"id": id, "mode": "worker", "runtime": "script", "command": ["{python}", str(HANDLERS / script)],
            "permissions": ["READ", "PUBLISH_ARTIFACT", "RUN_EXPERIMENT", "REQUEST_TASK"], **extra}


def interactive(id: str, **extra) -> dict:
    return {"id": id, "mode": "interactive",
            "permissions": ["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"], **extra}


@pytest.fixture
def make_config(tmp_path, nats):
    def _make(node: str, agents: list[dict], **extra):
        path = tmp_path / f"node-{node}" / "node.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        for a in agents:
            a.setdefault("workdir", str(path.parent / "work" / a["id"]))
        data = {"project": "testproj", "node": node, "data_dir": str(path.parent / "data"),
                "heartbeat_s": 0.5, "nats": {"servers": [nats.url]}, "agents": agents, **extra}
        path.write_text(yaml.safe_dump(data))
        return load_config(path)
    return _make


class Cluster:
    """Starts/stops NodeDaemons and opens client Hubs (what the CLI/MCP use)."""

    def __init__(self):
        self.daemons: dict[str, NodeDaemon] = {}
        self.hubs: list[Hub] = []

    async def start(self, cfg) -> NodeDaemon:
        daemon = NodeDaemon(cfg)
        await asyncio.wait_for(daemon.start(), 15)
        self.daemons[cfg.node] = daemon
        return daemon

    async def stop(self, node: str) -> None:
        daemon = self.daemons.pop(node, None)
        if daemon:
            await daemon.stop()

    async def client(self, cfg, **kw) -> Hub:
        hub = await Hub.open(cfg, "test-client", **kw)
        self.hubs.append(hub)
        return hub

    async def close(self) -> None:
        for node in list(self.daemons):
            await self.stop(node)
        for hub in self.hubs:
            try:
                await hub.close()
            except Exception:
                pass


@pytest.fixture
async def cluster():
    c = Cluster()
    yield c
    await c.close()


async def eventually(predicate, timeout: float = 20, interval: float = 0.1, what: str = "condition"):
    """Poll an async-or-sync predicate until it returns truthy."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(interval)


def auto_worker_node(tmp_path, **agent_extra):
    """An auto_worker agent B:desk on a Hub without a bus, and its daemon (not started; queue ready), for the v4
    recovery and actor tests. Returns (agent, cfg, ledger, hub, daemon)."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.ledger import Ledger
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script", command=["true"],
                        workdir=str(tmp_path / "work"), **agent_extra)
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    return agent, cfg, ledger, hub, daemon


def owned_task(ledger, task_id: str, status: str | None = None, claim: str | None = None, ingest: bool = False):
    """A REQUEST from A:sender that B:desk owns; optionally in `status` and claimed by `claim` (worker|session)."""
    from mutmuas.protocol import Envelope, request_body
    request = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                       body=request_body("test task", "test", kind="query"))
    if ingest:
        ledger.ingest(request)
    ledger.create_owned_task(request)
    if status:
        ledger.update_task(task_id, "owner", status=status)
    if claim:
        assert ledger.claim_task(task_id, claim, (status or "PENDING",)) is None
    return request


@contextlib.asynccontextmanager
async def group_child_survives(ledger, task_id: str, tmp_path):
    """A worker process group whose leader has exited while a child keeps running in the group, recorded as
    task_id's worker (pid and start time). Kills the whole group afterwards."""
    from mutmuas.node import proc_start
    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    code = ("import pathlib, subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
            f"pathlib.Path({str(ready)!r}).write_text('ready'); "
            f"p = pathlib.Path({str(release)!r})\nwhile not p.exists(): time.sleep(0.01)")
    leader = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    try:
        for _ in range(200):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(task_id, leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)                                         # the child still runs in the group
        yield leader
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(leader.pid, signal.SIGKILL)
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)


class Orphan:
    """A process in its own group whose parent has already exited, as a daemon's workers are after the daemon
    crashed: init/launchd reaps it when it ends, so no zombie stays behind (a zombie child of the test process
    would still count as a group member). The Popen-like part of the interface the recovery tests use."""

    def __init__(self, code: str):
        launcher = ("import subprocess, sys; p = subprocess.Popen([sys.executable, '-c', sys.argv[1]], "
                    "start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
                    "stderr=subprocess.DEVNULL); print(p.pid, flush=True)")
        out = subprocess.run([sys.executable, "-c", launcher, code], capture_output=True, text=True, check=True)
        self.pid = int(out.stdout)

    def poll(self):
        try:
            os.kill(self.pid, 0)
        except ProcessLookupError:
            return 0
        except PermissionError:
            pass
        return None

    def wait(self, timeout: float = 5):
        deadline = time.time() + timeout
        while self.poll() is None:
            if time.time() > deadline:
                raise subprocess.TimeoutExpired(str(self.pid), timeout)
            time.sleep(0.02)
        return 0

    def _signal(self, sig) -> None:
        try:
            os.killpg(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def terminate(self) -> None:
        self._signal(signal.SIGTERM)

    def kill(self) -> None:
        self._signal(signal.SIGKILL)


def thread_types(hub: Hub, task_id: str) -> list[str]:
    return [m["type"] for m in hub.ledger.thread(task_id)]


sys.path.insert(0, str(Path(__file__).parent))
