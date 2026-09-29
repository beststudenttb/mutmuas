"""Round-two regression: process-enumeration failure must not authorize a retry."""

import subprocess
import sys

import pytest

from conftest import group_child_survives
from test_codex_review_staff_v4b import _daemon
from mutmuas.node import proc_start


@pytest.mark.asyncio
async def test_cleanly_exited_old_worker_is_requeued_not_quarantined(tmp_path):
    ledger, daemon, task_id = _daemon(tmp_path)
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"],
                              start_new_session=True)
    try:
        ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
        worker.wait(5)
        await daemon.recover()
        assert ledger.task(task_id, "owner")["status"] == "ACCEPTED"
        assert task_id in daemon._queued["B:desk"]
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


@pytest.mark.asyncio
async def test_failed_ps_does_not_requeue_when_group_child_survives(tmp_path, monkeypatch):
    ledger, daemon, task_id = _daemon(tmp_path)
    real_run = subprocess.run
    try:
        async with group_child_survives(ledger, task_id, tmp_path):
            def failed_ps(args, *other, **kwargs):
                if "-A" in args:
                    return subprocess.CompletedProcess(args, 1, "", "ps failed")
                return real_run(args, *other, **kwargs)

            monkeypatch.setattr(subprocess, "run", failed_ps)
            await daemon.recover()
            assert task_id not in daemon._queued["B:desk"], "unknown group state must not authorize retry"
    finally:
        ledger.close()
