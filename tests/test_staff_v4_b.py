"""Staff v4, option B after D-040: at a restart an auto_worker task's draft is delivered or the task is run
again; an old worker that still runs is skipped, recorded and looked at again each heartbeat (not stopped, not
quarantined). Worker identity is pid plus start time."""

from __future__ import annotations

import os
import subprocess
import sys

from conftest import Orphan, auto_worker_node, owned_task

from mutmuas import node as node_mod
from mutmuas.node import proc_start, same_process


def _setup(tmp_path, status, **agent_extra):
    agent, _, ledger, _, daemon = auto_worker_node(tmp_path, **agent_extra)
    owned_task(ledger, "T-b", status, claim="worker", ingest=True)
    return agent, ledger, daemon


def test_unknown_start_time_never_authenticates_a_worker():
    """A missing start timestamp (migration or read failure) must not make a reused PID authoritative."""
    assert not same_process(os.getpid(), None)


async def test_recovery_delivers_a_draft_from_a_worker_that_already_exited(tmp_path):
    """A completed draft must win over retry even if its process died while the daemon was offline."""
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = Orphan("import time; time.sleep(30)")
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    ledger.update_task("T-b", "owner", result_draft={"status": "complete", "summary": "worker finished"})
    worker.terminate()
    worker.wait(5)
    try:
        await daemon.recover()
        task = ledger.task("T-b", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "worker finished"
        assert "T-b" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_an_old_worker_that_still_runs_is_skipped_recorded_and_looked_at_again(tmp_path):
    """D-040: not stopped, not quarantined. This round it is skipped and recorded (once); after it ends, the
    next look queues the task again."""
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = Orphan("import time; time.sleep(30)")
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    try:
        await daemon.recover()
        assert worker.poll() is None and "T-b" not in daemon._queued["B:desk"]
        assert "T-b" in daemon._recheck
        await daemon._recover_auto(ledger.task("T-b", "owner"))       # still running: recorded only once
        records = [r for r in ledger.failures() if r["task_id"] == "T-b"]
        assert [r["stage"] for r in records] == ["recover"]
        worker.terminate()
        worker.wait(5)
        await daemon._recover_auto(ledger.task("T-b", "owner"))       # what the heartbeat does
        assert "T-b" in daemon._queued["B:desk"] and "T-b" not in daemon._recheck
    finally:
        worker.kill()
        ledger.close()


async def test_a_record_without_start_time_does_not_admit_a_worker(tmp_path):
    from mutmuas.node import lease_refusal
    agent, ledger, _ = _setup(tmp_path, "RUNNING")
    ledger.set_runner_pid("T-b", os.getpid(), None)           # a record from before start times were kept
    session = Orphan("import time; time.sleep(30)")
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
        assert lease_refusal(ledger, "B:desk") is not None       # this process is not proven to be the worker
    finally:
        session.kill()
        ledger.close()


async def test_a_reused_pid_with_another_start_time_does_not_block_the_retry(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    ledger.set_runner_pid("T-b", other.pid, "a start time of the old worker")
    try:
        await daemon.recover()
        assert other.poll() is None and "T-b" in daemon._queued["B:desk"]
    finally:
        other.kill()
        other.wait(5)
        ledger.close()


def test_process_probes_contract(monkeypatch):
    """proc_start and _zombie read /proc or an absolute-path ps; on failure they say "not known" (None or
    False), never a guess (pinned before cleanup #1 merged their shared reading)."""
    me = os.getpid()
    assert node_mod.proc_start(me) and node_mod.proc_start(me) == node_mod.proc_start(me)
    assert node_mod._zombie(me) is False
    gone = 2 ** 22 + 54321                                             # no such process
    assert node_mod.proc_start(gone) is None and node_mod._zombie(gone) is False
    monkeypatch.setattr(node_mod, "_PS", None)
    if not os.path.exists(f"/proc/{me}/stat"):                         # macOS: no /proc, and now no ps either
        assert node_mod.proc_start(me) is None and node_mod._zombie(me) is False
