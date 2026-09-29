"""Long jobs (D-050): a task registers a background job, waits (WAITING) without being treated as failed, and the
node's heartbeat wakes the post when the job ends (process gone, or done-file there)."""

from __future__ import annotations

import asyncio
import sys

from conftest import Orphan, auto_worker_node, owned_task

from mutmuas import cli, tools
from mutmuas.hub import Hub
from mutmuas.node import NodeDaemon, proc_start


def _node(tmp_path, mode="worker", **extra):
    """B:desk as a plain worker (a script that exits without a result) or a plain interactive agent."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.ledger import Ledger
    agent = AgentConfig(id="desk", mode=mode, workdir=str(tmp_path / "work"),
                        **({"runtime": "script", "command": [sys.executable, "-c", "pass"]} if mode == "worker"
                           else {}), **extra)
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    daemon = NodeDaemon(cfg)
    daemon.hub = Hub(cfg, None, ledger)
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    return agent, ledger, daemon


def _sleeper():
    return Orphan("import time; time.sleep(60)")


async def test_registering_a_job_makes_the_task_wait_and_shows_it(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-j", "RUNNING", ingest=True)
    job = _sleeper()
    try:
        out = await tools.add_job(daemon.hub, "B:desk", "T-j", pid=job.pid, log="/tmp/train.log", note="training")
        assert out["state"] == "WAITING" and ledger.task("T-j", "owner")["status"] == "WAITING"
        [row] = ledger.jobs("T-j")
        assert row["pid"] == job.pid and row["pid_start"] == proc_start(job.pid)
        waiting = (await tools.whoami(daemon.hub, "B:desk"))["jobs_waiting"]
        assert [(j["task_id"], j["note"]) for j in waiting] == [("T-j", "training")]
    finally:
        job.terminate()
        ledger.close()


async def test_a_run_that_ends_while_its_job_runs_is_neither_finished_nor_failed(tmp_path):
    """The worker starts the job, registers it and exits without a result."""
    agent, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-j", "ACCEPTED", ingest=True)
    job = _sleeper()
    ledger.add_job("T-j", "B:desk", job.pid, proc_start(job.pid), None, None, "training")
    try:
        await daemon._execute(agent, "T-j")
        task = ledger.task("T-j", "owner")
        assert task["status"] not in ("COMPLETED", "FAILED") and task["result"] is None
        assert "T-j" not in daemon._retry and ledger.failures() == []
    finally:
        job.terminate()
        ledger.close()


async def test_when_the_process_ends_the_workers_task_runs_again_with_a_fresh_count(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-j", "RUNNING", ingest=True)
    ledger.bump_attempts("T-j")
    job = _sleeper()
    await tools.add_job(daemon.hub, "B:desk", "T-j", pid=job.pid, log="/tmp/train.log", note="training")
    try:
        await daemon._check_jobs()
        assert ledger.jobs("T-j") and "T-j" not in daemon._queued["B:desk"]       # still running: nothing
        job.terminate()
        job.wait(5)
        await daemon._check_jobs()
        assert ledger.jobs("T-j") == []
        [ended] = ledger.jobs("T-j", open_only=False)
        assert f"process {job.pid} ended" in ended["ended"]
        task = ledger.task("T-j", "owner")
        assert task["status"] == "ACCEPTED" and task["attempts"] == 0 and "T-j" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_done_file_ends_the_job_and_wakes_a_session(tmp_path):
    """A session's task: the node puts a note that hands it the baton in its inbox (wakes its watcher)."""
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    owned_task(ledger, "T-j", "RUNNING", ingest=True)
    done = tmp_path / "train.done"
    await tools.add_job(daemon.hub, "B:desk", "T-j", done_file=str(done), note="training")
    try:
        await daemon._check_jobs()
        assert ledger.jobs("T-j")
        done.write_text("exit 0\n")
        await daemon._check_jobs()
        [ended] = ledger.jobs("T-j", open_only=False)
        assert "exit 0" in ended["ended"]
        [note] = await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE)
        assert note["task_id"] == "T-j" and "background job ended" in note["body"]["message"]
    finally:
        ledger.close()


async def test_a_restart_leaves_a_waiting_task_alone(tmp_path):
    """An auto_worker task with an 'in progress' draft and a running job: not delivered, not run again."""
    _, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-j", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-j", "owner", result_draft={"status": "partial", "summary": "training runs"})
    job = _sleeper()
    ledger.add_job("T-j", "B:desk", job.pid, proc_start(job.pid), None, None, "training")
    try:
        await daemon.recover()
        task = ledger.task("T-j", "owner")
        assert task["status"] == "WAITING" and "T-j" not in daemon._queued["B:desk"]
    finally:
        job.terminate()
        ledger.close()


def test_the_resumed_worker_is_told_how_its_jobs_ended(tmp_path):
    from test_staff_v4 import _claude_ctx

    from mutmuas.runtime import worker_prompt
    _, ctx, _ = _claude_ctx(tmp_path)
    assert "add_job" in worker_prompt(ctx) and "nohup" in worker_prompt(ctx)
    ctx.jobs = [{"job_id": 1, "note": "training", "ended": "process 42 ended", "log": "/tmp/train.log"}]
    prompt = worker_prompt(ctx)
    assert "job 1 (training): process 42 ended; log /tmp/train.log" in prompt and "PLAN.md" in prompt


async def test_agentctl_job_add(monkeypatch):
    calls = []

    async def fake_add_job(hub, me, task_id, **kw):
        calls.append((task_id, kw))
        return {}

    monkeypatch.setattr(tools, "add_job", fake_add_job)
    args = cli.agentctl_parser().parse_args(["job", "add", "--task", "T-j", "--pid", "42", "--log", "l",
                                             "--note", "n", "--as", "B:desk"])
    await cli.cmd_job(args, None)
    assert calls == [("T-j", {"pid": 42, "done_file": None, "log": "l", "note": "n"})]
