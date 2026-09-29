"""Round-two regression: process-enumeration failure must not authorize a retry."""

import asyncio
import os
import signal
import subprocess
import sys

import pytest

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
    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    code = ("import pathlib, subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
            f"pathlib.Path({str(ready)!r}).write_text('ready'); "
            f"p=pathlib.Path({str(release)!r}); "
            "\nwhile not p.exists(): time.sleep(0.01)")
    leader = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    real_run = subprocess.run
    try:
        for _ in range(100):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(task_id, leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)  # a child still runs in this group

        def failed_ps(args, *other, **kwargs):
            if "-A" in args:
                return subprocess.CompletedProcess(args, 1, "", "ps failed")
            return real_run(args, *other, **kwargs)

        monkeypatch.setattr(subprocess, "run", failed_ps)
        await daemon.recover()
        assert task_id not in daemon._queued["B:desk"], "unknown group state must not authorize retry"
    finally:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)
        ledger.close()
