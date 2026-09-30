"""R20 items 3 and 4 (D-051): a wake-up lost to a crash between 'job ended' and 'wake' is made up at restart,
exactly once; every run keeps its own log file."""

from __future__ import annotations

import sys

from conftest import auto_worker_node, owned_task
from test_job_wake import _node

from mutmuas import tools
from mutmuas.runtime import ScriptRuntime, TaskContext


def _crashed_after_job_end(ledger, task_id="T-j", claim=None):
    """The state a crash leaves between _check_jobs' end_job and _wake_for_job: every job ended, task WAITING."""
    owned_task(ledger, task_id, "WAITING", claim=claim, ingest=True)
    job_id = ledger.add_job(task_id, "B:desk", None, None, "/tmp/train.done", "/tmp/train.log", "training")
    ledger.end_job(job_id, "done-file /tmp/train.done appeared: '0'")


def _wake_notes(ledger, task_id="T-j"):
    return [m for m in ledger.thread(task_id) if "background job ended" in ((m.get("body") or {}).get("message") or "")]


async def test_a_workers_lost_wake_up_is_made_up_at_restart_once(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    _crashed_after_job_end(ledger)
    try:
        await daemon.recover()
        await daemon.recover()                               # a second restart, or the scan run again
        await daemon._check_jobs()
        task = ledger.task("T-j", "owner")
        assert task["status"] == "ACCEPTED" and "T-j" in daemon._queued["B:desk"]
        assert daemon._queues["B:desk"].qsize() == 1 and len(_wake_notes(ledger)) == 1
    finally:
        ledger.close()


async def test_a_sessions_lost_wake_up_is_made_up_at_restart_once(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _crashed_after_job_end(ledger)
    try:
        await daemon.recover()
        await daemon.recover()
        notes = await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE)
        assert [n["task_id"] for n in notes] == ["T-j"] and "background job ended" in notes[0]["body"]["message"]
        assert ledger.task("T-j", "owner")["status"] == "RUNNING"
    finally:
        ledger.close()


async def test_an_auto_worker_is_woken_not_handed_its_in_progress_draft(tmp_path):
    """Before: recover() took the WAITING task for an ordinary crashed run and delivered the worker's draft."""
    _, _, ledger, _, daemon = auto_worker_node(tmp_path)
    _crashed_after_job_end(ledger, claim="worker")
    ledger.update_task("T-j", "owner", result_draft={"status": "partial", "summary": "training runs"})
    try:
        await daemon.recover()
        task = ledger.task("T-j", "owner")
        assert task["status"] == "ACCEPTED" and task["result"] is None and "T-j" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_every_run_keeps_its_own_log(tmp_path):
    """r19 F1: a woken task starts again at attempt 1, and its log overwrote the first run's."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    agent = AgentConfig(id="desk", mode="worker", runtime="script", workdir=str(tmp_path / "work"),
                        command=[sys.executable, "-c", "print('run')"])
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    req = Envelope(type="REQUEST", sender="A:s", to="B:desk", task_id="T-j", body=request_body("x", "y"))
    runtime = ScriptRuntime(agent, node)
    first = await runtime.run(TaskContext("T-j", req, agent, node, 1))
    second = await runtime.run(TaskContext("T-j", req, agent, node, 1))
    assert first.log_path != second.log_path
    assert "run" in open(first.log_path).read() and "run" in open(second.log_path).read()
