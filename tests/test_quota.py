"""D-104 item 1: a run that ends because the vendor account's usage limit was reached does not fail its task: the
task waits (paused, wait_reason "quota"), the run is not counted as an attempt, and its jobs run on. The secretary
lists every such task at once and resumes them when the account is back (one account: back for everyone)."""

from __future__ import annotations

import asyncio
import sys

import pytest

from conftest import Orphan, owned_task
from test_interrupt import _deliver, _update
from test_job_wake import _node

from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.node import NodeDaemon, proc_start
from mutmuas.runtime import ClaudeCodeRuntime, CodexRuntime, RunOutcome


def _script(agent, text):
    agent.command = [sys.executable, "-c", f"print({text!r}); raise SystemExit(1)"]


async def test_a_run_stopped_by_the_usage_limit_leaves_its_task_waiting_and_its_job_running(tmp_path):
    agent, ledger, daemon = _node(tmp_path)
    _script(agent, "MUTMUAS_QUOTA: usage limit reached, resets 5pm")
    owned_task(ledger, "T-q", "ACCEPTED", ingest=True)
    job = Orphan("import time; time.sleep(30)")
    ledger.add_job("T-q", "B:desk", job.pid, proc_start(job.pid), None, None, "training")
    daemon._enqueue("B:desk", "T-q")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))    # as a run goes
    try:
        for _ in range(100):
            if ledger.task("T-q", "owner")["status"] == "WAITING":
                break
            await asyncio.sleep(0.05)
        task = ledger.task("T-q", "owner")
        assert task["status"] == "WAITING" and task["paused"] and task["wait_reason"] == "quota"
        assert task["attempts"] == 0 and task["result"] is None                   # not a failed run
        assert any("usage limit" in note for note in task["interrupts"])          # the next run is told
        assert job.poll() is None and ledger.jobs("T-q")                          # its job runs on
        assert not daemon._retry and "T-q" not in daemon._queued["B:desk"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        job.kill()
        ledger.close()


def test_only_the_vendor_clis_own_words_count_as_the_usage_limit(tmp_path):
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path))
    claude = ClaudeCodeRuntime(AgentConfig(id="c", runtime="claude-code"), node)
    codex = CodexRuntime(AgentConfig(id="x", runtime="codex"), node)
    assert claude.quota(RunOutcome(1, '{"is_error":true,"result":"Claude AI usage limit reached|1759"}'))
    assert claude.quota(RunOutcome(1, "You've hit your session limit · resets 5pm"))
    assert codex.quota(RunOutcome(1, "ERROR: usage_limit_reached"))
    for text in ("OSError: Disk quota exceeded", "HTTP 429 from api.example.com", "rate limit exceeded on S3"):
        assert claude.quota(RunOutcome(1, text)) is None and codex.quota(RunOutcome(1, text)) is None
    assert claude.quota(RunOutcome(0, "Done: the paper says a usage limit reached 90% of runs")) is None  # it worked


@pytest.mark.parametrize("text", [
    "You\u2019ve hit your limit \u00b7 resets 11:20pm (Europe/Paris)",          # newer wording, curly apostrophe
    "You've hit your weekly limit · resets Mon 9am",
    "Claude AI usage limit reached|1753783200",
    "You've reached your usage limit for this period. Resets in: 4 hours 23 minutes",
    "Weekly limit reached · Retrying in 3h",
    '{"type":"error","error":{"type":"grace_daily_limit_reached","message":"daily limit"}}'])
@pytest.mark.parametrize("exit_code", [0, 1])
def test_claudes_limit_notices_are_recognised_even_when_it_exits_0(tmp_path, text, exit_code):
    """B:ops (F5): claude -p may end at the limit with exit 0, and newer versions say "You've hit your limit"
    without "session" or "usage". With exit 0 the notice must be what the run's (short) output starts with."""
    claude = ClaudeCodeRuntime(AgentConfig(id="c", runtime="claude-code"),
                               NodeConfig(project="p", node="B", data_dir=str(tmp_path)))
    assert claude.quota(RunOutcome(exit_code, text))


async def test_a_resume_clears_the_quota_wait_and_lays_the_task_out(tmp_path):
    agent, ledger, daemon = _node(tmp_path)
    daemon.cfg.trusted_controllers = ["B:secretary"]
    _script(agent, "MUTMUAS_QUOTA: usage limit reached")
    owned_task(ledger, "T-q", "ACCEPTED", ingest=True)
    try:
        await daemon._execute(agent, "T-q")
        await _deliver(daemon, agent, _update("T-q", sender="B:secretary", resume=True, message="limit is back"))
        task = ledger.task("T-q", "owner")
        assert not task["paused"] and task["wait_reason"] is None and "T-q" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_the_secretary_lists_and_resumes_every_quota_wait(tmp_path, monkeypatch):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    records = [{"task_id": "T-1", "owner": "B:rl", "status": "WAITING", "wait_reason": "quota"},
               {"task_id": "T-2", "owner": "C:vision", "status": "WAITING", "wait_reason": "quota"},
               {"task_id": "T-3", "owner": "C:vision", "status": "WAITING"}]               # waiting on something else

    async def all_tasks(limit=100, viewer=None):
        return records
    sent = []

    async def control(hub, me, task_id, action, message):
        sent.append((task_id, action))
        return {"task_id": task_id, "action": action, "delivery": "sent"}
    monkeypatch.setattr(daemon.hub, "all_tasks", all_tasks)
    monkeypatch.setattr(tools, "control_task", control)
    try:
        assert [t["task_id"] for t in await tools.quota_waits(daemon.hub, "B:desk")] == ["T-1", "T-2"]
        out = await tools.resume_quota_waits(daemon.hub, "B:desk")
        assert sent == [("T-1", "resume"), ("T-2", "resume")] and out["resumed"] == ["T-1", "T-2"]
    finally:
        ledger.close()


async def test_a_quota_task_resumed_then_a_deploy_before_its_next_run_is_not_held_again(tmp_path):
    agent, ledger, daemon = _node(tmp_path)
    daemon.cfg.trusted_controllers = ["B:secretary"]
    agent.command = [sys.executable, "-c", "print('MUTMUAS_QUOTA: usage limit reached'); raise SystemExit(1)"]
    owned_task(ledger, "T-q", "ACCEPTED", ingest=True)
    try:
        await daemon._execute(agent, "T-q")
        assert ledger.task("T-q", "owner")["wait_reason"] == "quota"
        await _deliver(daemon, agent, _update("T-q", sender="B:secretary", resume=True, message="limit is back"))
        assert not ledger.task("T-q", "owner")["paused"]
        # the daemon is replaced (deploy) while T-q still sits in the queue, before its next run starts
        new = NodeDaemon(daemon.cfg)
        new.hub = daemon.hub
        new._queues["B:desk"], new._queued["B:desk"] = asyncio.PriorityQueue(), set()
        await new.recover()
        task = ledger.task("T-q", "owner")
        assert not task["paused"] and task["wait_reason"] is None, task
        assert "T-q" in new._queued["B:desk"]
    finally:
        ledger.close()
