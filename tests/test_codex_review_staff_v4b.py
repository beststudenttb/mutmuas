"""Option-B restart regressions: do not run twice or strand a task."""

import os
import subprocess
import sys

import pytest
from conftest import auto_worker_node, group_child_survives, owned_task



def _daemon(tmp_path, status="RUNNING"):
    _, _, ledger, _, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-review-b", status, claim="worker")
    return ledger, daemon, "T-review-b"


@pytest.mark.asyncio
async def test_recover_does_not_requeue_when_group_leader_exited_but_child_runs(tmp_path):
    """The worker's process group, not merely its original PID, must be stopped before retry."""
    ledger, daemon, task_id = _daemon(tmp_path)
    try:
        async with group_child_survives(ledger, task_id, tmp_path) as leader:
            await daemon.recover()
            if task_id in daemon._queued["B:desk"]:
                os.killpg(leader.pid, 0)  # still-running child means requeue would double-run
                pytest.fail("task requeued while its old worker's process group still runs")
    finally:
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
