"""D-104 item 2: during a deploy the previous version's runs and this version's daemon share a ledger and
exchange messages, and a rollback runs the previous version on a ledger this one wrote. Both ways must work:
this code reads the previous version's ledger and messages (fixtures written by it, tests/compat/<sha>/), and the
previous version reads this code's (its source from git; skipped where the history is not at hand)."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import Envelope, request_body

PREVIOUS = "8bc01bb"                                   # the version exp/flow builds on (deployed as claude)
FIXTURES = Path(__file__).parent / "compat" / PREVIOUS
REPO = Path(__file__).parents[1]


def _node(tmp_path):
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"),
                     agents=[AgentConfig(id="desk", mode="interactive")]).validate()
    cfg.data_path.mkdir(parents=True, exist_ok=True)
    return cfg


async def test_this_code_works_on_the_previous_versions_ledger(tmp_path):
    cfg = _node(tmp_path)
    with sqlite3.connect(cfg.db_path) as db:
        db.executescript((FIXTURES / "ledger.sql").read_text())
    ledger = Ledger(cfg.db_path)                       # gains the columns added since
    try:
        task = ledger.task("T-compat-1", "owner")
        assert task["status"] == "WAITING" and task["paused"] and task["interrupts"] == ["hold on"]
        assert task["priority"] == "normal" and task["wait_reason"] is None and task["run_log"] is None
        assert task["request"]["leader"] is True       # still urgent: the leader's work counts as high
        assert [j["note"] for j in ledger.jobs("T-compat-1")] == ["training"]
        assert [e.task_id for e in ledger.outbox()] == ["T-compat-3"]
        hub = Hub(cfg, None, ledger)
        rows = await tools.inbox(hub, "B:desk", include_seen=True, peek=True, types=None)
        assert sorted(r["type"] for r in rows) == ["ANSWER", "REQUEST"]
        daemon = NodeDaemon(cfg)                       # the new daemon after a deploy
        daemon.hub = hub
        daemon._queues["B:desk"], daemon._queued["B:desk"] = asyncio.PriorityQueue(), set()
        await daemon.recover()
        assert ledger.task("T-compat-1", "owner")["status"] == "WAITING"      # paused stays paused
    finally:
        ledger.close()


def test_this_code_reads_the_previous_versions_messages(tmp_path):
    for raw in json.loads((FIXTURES / "envelopes.json").read_text()):
        env = Envelope.from_json(json.dumps(raw))
        env.validate()
        assert env.to_dict()["body"] == raw["body"]


OLD_READER = """
import asyncio, json, sys
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import Envelope
cfg = NodeConfig(project="p", node="B", data_dir=sys.argv[1], agents=[AgentConfig(id="desk", mode="interactive")])
ledger = Ledger(cfg.db_path)
task = ledger.task("T-new", "owner")
assert task["status"] == "WAITING" and task["paused"] == 1, task
assert ledger.tasks(role="owner", limit=None) and ledger.jobs("T-new") and ledger.outbox()
for raw in json.load(open(sys.argv[2])):
    Envelope.from_json(json.dumps(raw)).validate()
daemon = NodeDaemon(cfg)                               # a rollback: the previous daemon on this ledger
daemon.hub = Hub(cfg, None, ledger)
daemon._queues["B:desk"], daemon._queued["B:desk"] = asyncio.PriorityQueue(), set()
asyncio.run(daemon.recover())
print("OK")
"""


def test_the_previous_version_works_on_this_codes_ledger_and_messages(tmp_path):
    if not shutil.which("git") or subprocess.run(["git", "-C", str(REPO), "cat-file", "-e", f"{PREVIOUS}^{{commit}}"],
                                                 capture_output=True).returncode:
        pytest.skip(f"the source of {PREVIOUS} is not at hand (no git history here)")
    old = tmp_path / "old"
    old.mkdir()
    archive = subprocess.run(["git", "-C", str(REPO), "archive", PREVIOUS, "src"], capture_output=True, check=True)
    subprocess.run(["tar", "x", "-C", str(old)], input=archive.stdout, check=True)
    cfg = _node(tmp_path)
    ledger = Ledger(cfg.db_path)
    req = Envelope(type="REQUEST", sender="A:main", to="B:desk", task_id="T-new", priority="high",
                   body=request_body("train it", "compat", kind="experiment"))
    ledger.ingest(req)
    ledger.create_owned_task(req)
    ledger.update_task("T-new", "owner", status="WAITING", paused=1, wait_reason="quota", run_log="/tmp/run.log",
                       interrupts=["your previous run stopped at the account's usage limit"])
    ledger.add_job("T-new", "B:desk", 99999, "start", "/tmp/done", "/tmp/log", "training")
    ledger.queue_outgoing(Envelope(type="RESULT", sender="B:desk", to="A:main", task_id="T-new",
                                   body={"status": "complete", "summary": "delivered", "next": "A:main"}))
    ledger.close()
    messages = tmp_path / "messages.json"
    messages.write_text(json.dumps([json.loads(e.to_json()) for e in (
        req, Envelope(type="UPDATE", sender="B:secretary", to="B:desk", task_id="T-new",
                      body={"message": "the usage limit is back", "resume": True}))]))
    out = subprocess.run([sys.executable, "-c", OLD_READER, str(cfg.data_path), str(messages)], capture_output=True,
                         text=True, env={**os.environ, "PYTHONPATH": str(old / "src")})
    assert out.returncode == 0 and out.stdout.strip() == "OK", out.stderr[-2000:]
