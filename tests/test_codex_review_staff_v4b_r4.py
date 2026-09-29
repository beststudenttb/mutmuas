"""Round-four regression: ps STAT validation must reject invalid codes and accept documented macOS flags.

Round 5 (kernel query, no ps parsing): no STAT is parsed any more. Both tests keep their intent with the new
semantics: an unrelated process's flags cannot poison the query of our group (it is still "members"), and an
invalid listing cannot prove the old group empty while its child runs ("members", quarantined). The original is
kept in tests/reviews/codex_review_staff_v4b_r4_original.py."""

import asyncio
import contextlib
import os
import subprocess
import sys

import pytest

from mutmuas import node as node_mod
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, proc_start
from mutmuas.protocol import Envelope, request_body


@pytest.mark.parametrize("stat", ["SX", "S>", "SA", "SS"])
def test_documented_macos_stat_suffix_does_not_poison_listing(stat, monkeypatch):
    # Apple ps(1) documents >, A, S and X; current Apple ps source emits X for P_TRACED.
    # The flagged process is unrelated: ps -A includes it anyway, so rejecting its
    # valid state would poison inspection of every process group on the machine.
    listing = f"{os.getpid()} {os.getpgrp()} S\n987654 987654 {stat}\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k:
                        subprocess.CompletedProcess(a[0], 0, listing, ""))
    assert node_mod.group_state(os.getpgrp()) == "members"


async def test_invalid_stat_cannot_prove_old_group_empty_while_child_runs(tmp_path, monkeypatch):
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    request = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-invalid-stat",
                       body=request_body("test task", "test", kind="query"))
    ledger.ingest(request)
    ledger.create_owned_task(request)
    ledger.update_task(request.task_id, "owner", status="RUNNING")
    assert ledger.claim_task(request.task_id, "worker", ("RUNNING",)) is None
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()

    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    leader = subprocess.Popen([sys.executable, "-c",
                               "import pathlib, subprocess, sys, time; "
                               "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
                               f"pathlib.Path({str(ready)!r}).write_text('ready'); "
                               f"p = pathlib.Path({str(release)!r})\nwhile not p.exists(): time.sleep(0.01)"],
                              start_new_session=True)
    try:
        for _ in range(200):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(request.task_id, leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)  # the old child is still alive

        # L is a suffix flag, not a leading run state, in both Apple and
        # procps-ng ps manuals. This malformed listing omits the old child.
        bogus = f"{os.getpid()} {os.getpgrp()} L\n"
        monkeypatch.setattr(subprocess, "run", lambda *a, **k:
                            subprocess.CompletedProcess(a[0], 0, bogus, ""))
        state = node_mod.group_state(leader.pid)
        await daemon.recover()
        task = ledger.task(request.task_id, "owner")
        assert (state, task["status"], request.task_id in daemon._queued["B:desk"]) == (
            "members", "FAILED", False)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(leader.pid, 9)
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)
        ledger.close()
