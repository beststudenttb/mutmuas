"""D-089: the leader's word (or priority=high) interrupts a worker in the middle of a run: the node stops the run and
lays the task out again with the message, which the next run (the same conversation for a brain) is given first.
Pause and resume stop and restart a worker's task and travel down to its child tasks."""

from __future__ import annotations

import asyncio

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import node as node_module
from mutmuas import tools
from mutmuas.protocol import Envelope, request_body
from mutmuas.runtime import RunOutcome, worker_prompt


class Blocking:
    """A runtime whose run never returns until cancelled; records each run's context."""
    runs: list = []
    started = None

    def __init__(self, *_):
        pass

    async def run(self, ctx):
        Blocking.runs.append(ctx)
        Blocking.started.set()
        await asyncio.Event().wait()
        return RunOutcome(0, "")


@pytest.fixture
def blocking(monkeypatch):
    Blocking.runs, Blocking.started = [], asyncio.Event()
    monkeypatch.setattr(node_module, "make_runtime", Blocking)
    return Blocking


async def _running(daemon, agent, task_id):
    """Queue the task and start the post's runner; return once the run is under way."""
    daemon._enqueue("B:desk", task_id)
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    await asyncio.wait_for(Blocking.started.wait(), 5)
    Blocking.started.clear()
    return runner


def _update(task_id, sender="A:sender", priority="normal", **body):
    return Envelope(type="UPDATE", sender=sender, to="B:desk", task_id=task_id, priority=priority,
                    body={"message": "stop using the old environment; use env v2", **body})


async def _deliver(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    await daemon._handle(agent, env)


async def _settle(cond, timeout=5):
    for _ in range(int(timeout * 50)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached")


async def test_the_leaders_word_interrupts_a_running_worker_and_is_given_to_the_next_run(tmp_path, blocking):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", leader=True))
        await asyncio.wait_for(blocking.started.wait(), 5)             # stopped, laid out again, running again
        first, second = blocking.runs
        assert "stop using the old environment" not in worker_prompt(first)
        assert "interrupted" in worker_prompt(second) and "stop using the old environment" in worker_prompt(second)
        notes = [e.body.get("message", "") for e in ledger.outbox() if e.task_id == "T-r" and e.type == "UPDATE"]
        assert any("interrupted" in n for n in notes)                      # the requester is told
        assert ledger.task("T-r", "owner")["interrupts"] in (None, "[]", [])  # handed over to the run
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_ordinary_mail_does_not_interrupt(tmp_path, blocking):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r"))
        await asyncio.sleep(0.3)
        assert len(blocking.runs) == 1 and "T-r" in daemon._running
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_high_priority_with_interrupt_stops_whatever_the_post_runs(tmp_path, blocking):
    """A new instruction (not about the running task) marked interrupt: the running task is laid out again."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-other", sender="B:secretary", priority="high", interrupt=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert "stop using the old environment" in worker_prompt(blocking.runs[1])
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_pause_stops_a_running_worker_until_resume(tmp_path, blocking):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", leader=True, pause=True))
        await _settle(lambda: "T-r" not in daemon._running)
        await asyncio.sleep(0.2)
        task = ledger.task("T-r", "owner")
        assert task["status"] == "WAITING" and task["paused"] and len(blocking.runs) == 1
        await daemon._auto_dispatch()
        await daemon.recover()
        await asyncio.sleep(0.2)
        assert len(blocking.runs) == 1                                   # nothing restarts a paused task
        await _deliver(daemon, agent, _update("T-r", leader=True, resume=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert not ledger.task("T-r", "owner")["paused"]
        assert "resume" in worker_prompt(blocking.runs[1]).lower()
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_pause_and_resume_travel_down_to_child_tasks(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    child = await tools.send_request(hub, "B:desk", "C:rl", "train", "for T-p", parent_task="T-p")
    try:
        await _deliver(daemon, agent, _update("T-p", leader=True, pause=True))
        [down] = [e for e in ledger.outbox() if e.task_id == child["task_id"] and e.type == "UPDATE"]
        assert down.to == "C:rl" and down.body["pause"] is True and down.body["leader"] is True
        await _deliver(daemon, agent, _update("T-p", leader=True, resume=True))
        ups = [e for e in ledger.outbox() if e.task_id == child["task_id"] and e.body.get("resume")]
        assert len(ups) == 1
    finally:
        ledger.close()


async def test_only_the_requester_or_the_leader_can_pause(tmp_path, blocking):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", sender="C:stranger", pause=True))   # not leader, not requester
        await asyncio.sleep(0.3)
        assert "T-r" in daemon._running and not ledger.task("T-r", "owner")["paused"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_a_restart_leaves_a_paused_task_paused(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1)
    try:
        await daemon.recover()
        assert ledger.task("T-r", "owner")["status"] == "WAITING" and "T-r" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_an_interrupted_brain_run_is_resumed_in_the_same_conversation(tmp_path, blocking):
    """Sandbox finding: a brain's session id came only from the JSON a run prints when it ends, so a run stopped
    midway left none and the next run started afresh. The node now names a new conversation itself
    (--session-id) and records it before the run starts."""
    from mutmuas.runtime import ClaudeCodeRuntime
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    agent.runtime = "claude-code"
    owned_task(ledger, "T-b", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-b")
    try:
        first = blocking.runs[0]
        assert first.resume is None and first.session_id
        argv, _ = ClaudeCodeRuntime(agent, cfg).command(first)
        assert argv[argv.index("--session-id") + 1] == first.session_id
        assert ledger.brain_session("B:desk", None) == first.session_id         # known before the run ends
        await _deliver(daemon, agent, _update("T-b", leader=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        second = blocking.runs[1]
        assert second.resume == first.session_id
        argv, _ = ClaudeCodeRuntime(agent, cfg).command(second)
        assert "--session-id" not in argv and argv[argv.index("--resume") + 1] == first.session_id
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()
