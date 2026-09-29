"""Regression checks for auto-worker actor identity after ba28e70.

Option B (exp/staff-v4-b) changes two things here; the original file is kept in tests/reviews/:
- the worker is recorded with its start time: Codex's round 4 (f8c105e) requires pid and start time (a record
  without a start time proves nothing);
- recovery stops a surviving worker instead of adopting it, so the task may go to the session, but only after
  the old worker is gone: never two actors."""

import asyncio
import os
import subprocess
import sys

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.node import NodeDaemon, lease_refusal


def _setup(tmp_path):
    agent, cfg, ledger, hub, _ = auto_worker_node(tmp_path)
    return agent, cfg, ledger, hub


def _request(ledger, task_id):
    owned_task(ledger, task_id)


def _holder():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])


@pytest.mark.asyncio
async def test_worker_without_task_env_still_cannot_finish_session_task(tmp_path, monkeypatch):
    """A child of a worker can lose its environment but is still the worker, not the interactive session."""
    agent, _, ledger, hub = _setup(tmp_path)
    worker_task, session_task = "T-worker", "T-session"
    _request(ledger, worker_task)
    _request(ledger, session_task)
    ledger.update_task(worker_task, "owner", status="RUNNING")
    assert ledger.claim_task(worker_task, "worker", ("RUNNING",)) is None
    from mutmuas.node import proc_start
    ledger.set_runner_pid(worker_task, os.getpid(), proc_start(os.getpid()))  # models the daemon-started worker
    assert ledger.claim_task(session_task, "session", ("PENDING",)) is None
    ledger.update_task(session_task, "owner", status="RUNNING")
    session = _holder()
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
        assert lease_refusal(ledger, "B:desk") is None  # admitted via worker PID, not session PID
        monkeypatch.delenv("MUTMUAS_TASK_ID", raising=False)
        with pytest.raises(PermissionError, match="session|worker"):
            await tools.submit_result(hub, "B:desk", "complete", "wrong actor", task_id=session_task)
        assert ledger.task(session_task, "owner")["status"] == "RUNNING"
    finally:
        session.terminate()
        session.wait(5)
        ledger.close()


@pytest.mark.asyncio
async def test_recovery_does_not_release_a_still_running_worker_to_the_session(tmp_path):
    """An unclean daemon restart must not hand off a task while its detached worker process is alive.
    Option B: the restart stops it first; only then may the session get the task (no two actors)."""
    agent, cfg, ledger, hub = _setup(tmp_path)
    task_id = "T-worker-survives"
    _request(ledger, task_id)
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
        worker.wait(5)                              # option B: the surviving worker is stopped at the restart
        await daemon._execute(agent, task_id)  # dequeued after restart while the session is present
        task = ledger.task(task_id, "owner")
        assert task["status"] == "PENDING" and task["runner"] is None     # the session's to decide, worker gone
    finally:
        for proc in (worker, session):
            proc.terminate()
            proc.wait(5)
        ledger.close()
