"""Regression checks for auto-worker actor identity after ba28e70.

Option B (exp/staff-v4-b) changes two things here; the original file is kept in tests/reviews/:
- the worker is recorded with its start time: Codex's round 4 (f8c105e) requires pid and start time (a record
  without a start time proves nothing);
- recovery stops a surviving worker instead of adopting it, so the task may go to the session, but only after
  the old worker is gone: never two actors."""

import asyncio
import subprocess
import sys

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas.node import NodeDaemon


def _setup(tmp_path):
    agent, cfg, ledger, hub, _ = auto_worker_node(tmp_path)
    return agent, cfg, ledger, hub


def _holder():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])


@pytest.mark.asyncio
async def test_recovery_does_not_release_a_still_running_worker_to_the_session(tmp_path):
    """An unclean daemon restart must not hand off a task while its detached worker process is alive.
    D-040: the old worker is not stopped; the task is skipped while it runs (never two actors)."""
    agent, cfg, ledger, hub = _setup(tmp_path)
    task_id = "T-worker-survives"
    owned_task(ledger, task_id)
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    worker, session = _holder(), _holder()
    from mutmuas.node import proc_start
    ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        assert worker.poll() is None                # D-040: not stopped, skipped and looked at again
        await daemon._execute(agent, task_id)  # dequeued after restart while the session is present
        task = ledger.task(task_id, "owner")
        assert task["runner"] == "worker" and task["status"] != "PENDING"
    finally:
        for proc in (worker, session):
            proc.terminate()
            proc.wait(5)
        ledger.close()
