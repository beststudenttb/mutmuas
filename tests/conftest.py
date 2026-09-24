"""Shared fixtures: a throwaway NATS server and in-process nodes A/B on localhost.

Each node gets its own data dir (own SQLite ledger), exactly like separate
machines; they only share the NATS server.
"""

from __future__ import annotations

import asyncio
import os
import shutil
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


def thread_types(hub: Hub, task_id: str) -> list[str]:
    return [m["type"] for m in hub.ledger.thread(task_id)]


sys.path.insert(0, str(Path(__file__).parent))
