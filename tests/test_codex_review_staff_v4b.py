"""Option-B restart regressions: do not run twice or strand a task."""

import asyncio
import os
import signal
import subprocess
import sys

import pytest

from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, proc_start
from mutmuas.protocol import Envelope, request_body


def _daemon(tmp_path, status="RUNNING"):
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    task_id = "T-review-b"
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                                      body=request_body("test task", "test", kind="query")))
    ledger.update_task(task_id, "owner", status=status)
    assert ledger.claim_task(task_id, "worker", (status,)) is None
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    return ledger, daemon, task_id


@pytest.mark.asyncio
async def test_recover_does_not_requeue_when_group_leader_exited_but_child_runs(tmp_path):
    """The worker's process group, not merely its original PID, must be stopped before retry."""
    ledger, daemon, task_id = _daemon(tmp_path)
    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    code = ("import pathlib, subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
            f"pathlib.Path({str(ready)!r}).write_text('ready'); "
            f"p=pathlib.Path({str(release)!r}); "
            "\nwhile not p.exists(): time.sleep(0.01)")
    leader = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    try:
        for _ in range(100):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(task_id, leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)  # the child still runs in that process group
        await daemon.recover()
        if task_id in daemon._queued["B:desk"]:
            os.killpg(leader.pid, 0)  # still-running child means requeue would double-run
            pytest.fail("task requeued while its old worker's process group still runs")
    finally:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)
        ledger.close()


@pytest.mark.asyncio
async def test_unknown_worker_start_does_not_requeue_a_live_old_process(tmp_path):
    """An unverified legacy PID must be quarantined, not run a second time."""
    ledger, daemon, task_id = _daemon(tmp_path)
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    ledger.set_runner_pid(task_id, worker.pid, None)
    try:
        await daemon.recover()
        assert worker.poll() is None
        assert task_id not in daemon._queued["B:desk"]
    finally:
        worker.kill()
        worker.wait(5)
        ledger.close()


@pytest.mark.asyncio
async def test_recover_resolves_pending_task_still_claimed_by_worker(tmp_path):
    """A crash between the PENDING transition and release_task must not strand the task."""
    ledger, daemon, task_id = _daemon(tmp_path, status="PENDING")
    try:
        await daemon.recover()
        await daemon._auto_dispatch()
        assert task_id in daemon._queued["B:desk"]
    finally:
        ledger.close()
