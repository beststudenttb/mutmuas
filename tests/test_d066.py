"""D-066: a task waits on the child tasks it delegated and is woken once when they are all done; cancelling a
task cancels its open children; the node delivers (repeating) reminders itself; workers have turn/cost limits."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from conftest import backdate_deadline, owned_task
from test_job_wake import _node

from mutmuas import tools
from mutmuas.protocol import Envelope, result_body


def _iso(delta_s: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_s)).isoformat()


async def _child(daemon, parent: str, to: str, deadline: str | None = None) -> str:
    """A child task; a deadline already past is sent as a future one and moved back (D-098 refuses a past one)."""
    past = deadline and deadline < _iso(0)
    sent = await tools.send_request(daemon.hub, "B:desk", to, f"part of {parent}", "split", parent_task=parent,
                                    deadline="+1h" if past else deadline)
    if past:
        backdate_deadline(daemon.hub.ledger, sent["task_id"], deadline)
    return sent["task_id"]


async def _reply(daemon, task_id: str, owner: str, type_: str = "RESULT", **body) -> None:
    await daemon._on_reply(Envelope(type=type_, sender=owner, to="B:desk", task_id=task_id, body=body))


# --------------------------------------------------------------------------- 1 child tasks wake their parent


async def test_a_worker_waiting_on_its_children_is_woken_once_with_their_results(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    first, second = await _child(daemon, "T-p", "C:vision"), await _child(daemon, "T-p", "C:control")
    try:
        out = await tools.add_job(daemon.hub, "B:desk", "T-p", children=True, note="vision and control")
        assert out["state"] == "WAITING" and ledger.task("T-p", "owner")["status"] == "WAITING"
        await daemon._check_jobs()
        assert ledger.jobs("T-p") and "T-p" not in daemon._queued["B:desk"]
        await _reply(daemon, first, "C:vision", **result_body("complete", "detector at 0.91 mAP"))
        await daemon._check_jobs()
        assert ledger.jobs("T-p") and "T-p" not in daemon._queued["B:desk"]           # one child still open
        await _reply(daemon, second, "C:control", "REJECT", reason="not my function")
        await daemon._check_jobs()
        [ended] = ledger.jobs("T-p", open_only=False)
        assert first in ended["ended"] and "detector at 0.91 mAP" in ended["ended"]
        assert second in ended["ended"] and "not my function" in ended["ended"]
        task = ledger.task("T-p", "owner")
        assert task["status"] == "ACCEPTED" and task["attempts"] == 0 and "T-p" in daemon._queued["B:desk"]
        daemon._queued["B:desk"].clear()
        await daemon._check_jobs()                                                     # woken once only
        assert "T-p" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_child_past_its_deadline_wakes_the_parent_but_is_not_closed(tmp_path):
    """The deadline is when a reply was due: the parent decides whether to wait longer or cancel (D-068 no. 9)."""
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    late = await _child(daemon, "T-p", "C:train", deadline=_iso(-60))
    try:
        await tools.add_job(daemon.hub, "B:desk", "T-p", children=True)
        await daemon._check_jobs()
        [note] = await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE)
        assert note["task_id"] == "T-p" and late in note["body"]["message"] and "overdue" in note["body"]["message"]
        assert ledger.task(late, "requester")["status"] == "PENDING"
    finally:
        ledger.close()


async def test_an_overdue_child_wakes_the_parent_only_once(tmp_path):
    """Secretary's recheck of ad60ea4: after the overdue wake-up the parent waits again (rule 8, or WAITING); the
    same overdue child must not end that wait at once, or a worker is started over and over. It ends when the
    child really finishes."""
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    late = await _child(daemon, "T-p", "C:train", deadline=_iso(-60))
    try:
        await tools.add_job(daemon.hub, "B:desk", "T-p", children=True)
        await daemon._check_jobs()
        assert "T-p" in daemon._queued["B:desk"]                                       # the overdue wake-up
        daemon._queued["B:desk"].clear()
        await tools.report_progress(daemon.hub, "B:desk", "training runs late; waiting", task_id="T-p",
                                    state="WAITING")
        await daemon._check_jobs()
        await daemon._check_jobs()
        assert "T-p" not in daemon._queued["B:desk"] and ledger.jobs("T-p")            # not again
        await _reply(daemon, late, "C:train", **result_body("complete", "trained"))
        await daemon._check_jobs()
        assert "T-p" in daemon._queued["B:desk"]
        assert "trained" in ledger.jobs("T-p", open_only=False)[-1]["ended"]
    finally:
        ledger.close()


async def test_reporting_waiting_with_open_children_registers_the_wait(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    await _child(daemon, "T-p", "C:vision")
    try:
        await tools.report_progress(daemon.hub, "B:desk", "delegated the vision part", task_id="T-p",
                                    state="WAITING")
        [job] = ledger.jobs("T-p")
        assert job["children"] == 1
        await tools.report_progress(daemon.hub, "B:desk", "still waiting", task_id="T-p", state="WAITING")
        assert len(ledger.jobs("T-p")) == 1                                            # not registered twice
    finally:
        ledger.close()


async def test_waiting_on_children_needs_a_child(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    try:
        with pytest.raises(ValueError, match="child"):
            await tools.add_job(daemon.hub, "B:desk", "T-p", children=True)
    finally:
        ledger.close()


async def test_a_restarted_node_still_wakes_the_parent(tmp_path):
    """The wait lives in the ledger: a new daemon on the same ledger picks it up."""
    from mutmuas.node import NodeDaemon
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    child = await _child(daemon, "T-p", "C:vision")
    try:
        await tools.add_job(daemon.hub, "B:desk", "T-p", children=True)
        await _reply(daemon, child, "C:vision", **result_body("complete", "done"))
        again = NodeDaemon(daemon.cfg)
        again.hub, again._queues, again._queued = daemon.hub, daemon._queues, {"B:desk": set()}
        await again._check_jobs()
        assert "T-p" in again._queued["B:desk"]
    finally:
        ledger.close()


# --------------------------------------------------------------------------- 2 cancelling cascades to children


async def test_cancelling_a_task_cancels_its_open_children(tmp_path):
    _, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    done, open_ = await _child(daemon, "T-p", "C:vision"), await _child(daemon, "T-p", "C:control")
    await _reply(daemon, done, "C:vision", **result_body("complete", "done"))
    try:
        await daemon._on_cancel(Envelope(type="CANCEL", sender="A:sender", to="B:desk", task_id="T-p",
                                         body={"reason": "leader changed the plan"}))
        assert ledger.task("T-p", "owner")["status"] == "CANCELLED"
        assert ledger.task(open_, "requester")["status"] == "CANCELLED"
        assert ledger.task(done, "requester")["status"] == "COMPLETED"
        cancels = [e for e in ledger.outbox() if e.type == "CANCEL"]
        assert [(e.task_id, e.to) for e in cancels] == [(open_, "C:control")]
        assert "T-p" in cancels[0].body["reason"] and "leader changed the plan" in cancels[0].body["reason"]
    finally:
        ledger.close()


async def test_withdrawing_a_pending_task_cancels_its_children_too(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    owned_task(ledger, "T-p", "PENDING", ingest=True)
    child = await _child(daemon, "T-p", "C:vision")
    try:
        await daemon._on_cancel(Envelope(type="CANCEL", sender="A:sender", to="B:desk", task_id="T-p", body={}))
        assert ledger.task(child, "requester")["status"] == "CANCELLED"
    finally:
        ledger.close()


# --------------------------------------------------------------------------- 3 the node delivers reminders


async def test_the_node_delivers_a_due_reminder_into_the_inbox(tmp_path):
    """No session and no lease needed: the reminder lands in the inbox and hands the post the baton."""
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    try:
        out = await tools.remind_me(daemon.hub, "B:desk", "+1s", "report progress to the secretary")
        ledger.db.execute("UPDATE reminders SET due=? WHERE id=?", (_iso(-1), out["reminder"]))      # it is due
        await daemon._fire_reminders()
        [note] = await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE)
        assert "report progress to the secretary" in note["body"]["message"] and note["from"] == "B:desk"
        await daemon._fire_reminders()
        assert await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE) == []           # once
        assert ledger.db.execute("SELECT fired_at FROM reminders WHERE id=?", (out["reminder"],)).fetchone()[0]
    finally:
        ledger.close()


async def test_a_repeating_reminder_comes_back_after_its_interval(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    try:
        out = await tools.remind_me(daemon.hub, "B:desk", "+1s", "progress ping", every="5h")
        ledger.db.execute("UPDATE reminders SET due=? WHERE id=?", (_iso(-1), out["reminder"]))      # it is due
        assert out["every_s"] == 5 * 3600
        await daemon._fire_reminders()
        await daemon._fire_reminders()
        assert len(await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE)) == 1
        due = datetime.fromisoformat(ledger.db.execute("SELECT due FROM reminders WHERE id=?",
                                                       (out["reminder"],)).fetchone()[0])
        assert timedelta(hours=4.9) < due - datetime.now(timezone.utc) <= timedelta(hours=5)
        await tools.cancel_reminder(daemon.hub, "B:desk", out["reminder"])
        ledger.db.execute("UPDATE reminders SET due=? WHERE id=?", (_iso(-1), out["reminder"]))
        await daemon._fire_reminders()
        assert await tools.inbox(daemon.hub, "B:desk", types=tools.WAKE) == []
    finally:
        ledger.close()


def test_the_session_mcp_no_longer_fires_reminders():
    """Only the node fires reminders, so a reminder is never taken by a session that then drops it."""
    import inspect

    from mutmuas import mcp_server
    assert "due_reminders" not in inspect.getsource(mcp_server)


# --------------------------------------------------------------------------- 4 worker turn and cost limits


def _claude(tmp_path, agent_extra=None, node_extra=None):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import request_body
    from mutmuas.runtime import ClaudeCodeRuntime, TaskContext
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path / "data"), **(node_extra or {}))
    agent = AgentConfig(id="w", runtime="claude-code", workdir=str(tmp_path), **(agent_extra or {}))
    req = Envelope(type="REQUEST", sender="A:m", to="C:w", task_id="T-1", body=request_body("x", "y"))
    return ClaudeCodeRuntime(agent, node), TaskContext("T-1", req, agent, node)


def _flag(argv, name):
    return argv[argv.index(name) + 1] if name in argv else None


def test_worker_limits_come_from_the_node_default_or_the_agent(tmp_path):
    runtime, ctx = _claude(tmp_path)
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") is None and _flag(argv, "--max-budget-usd") is None
    runtime, ctx = _claude(tmp_path, node_extra={"worker_max_turns": 150, "worker_max_cost_usd": 15})
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") == "150" and _flag(argv, "--max-budget-usd") == "15"
    runtime, ctx = _claude(tmp_path, {"max_turns": 40, "max_cost_usd": 2.5}, {"worker_max_turns": 150})
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") == "40" and _flag(argv, "--max-budget-usd") == "2.5"


@pytest.mark.parametrize("subtype", ["error_max_turns", "error_max_budget_usd"])
def test_the_claude_result_names_the_limit_it_hit(tmp_path, subtype):
    runtime, ctx = _claude(tmp_path)
    tail = json.dumps({"type": "result", "subtype": subtype, "is_error": True, "num_turns": 41,
                       "total_cost_usd": 3.2, "session_id": "s"})
    outcome = runtime.parse(ctx, 0, tail)
    assert outcome.limit == subtype


async def test_a_run_stopped_at_a_limit_is_a_failed_run_laid_out_once_more(tmp_path, monkeypatch):
    """R5.4: recorded in the failures, run once more; the second time the task fails."""
    from mutmuas import node as node_module
    from mutmuas.runtime import RunOutcome
    agent, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-l", "ACCEPTED", ingest=True)

    class Limited:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            return RunOutcome(0, "stopped", limit="error_max_turns")
    monkeypatch.setattr(node_module, "make_runtime", Limited)
    try:
        await daemon._execute(agent, "T-l")
        assert ledger.task("T-l", "owner")["status"] == "ACCEPTED" and "T-l" in daemon._retry
        [failure] = ledger.failures()
        assert "error_max_turns" in failure["error"]
        daemon._retry.clear()
        await daemon._execute(agent, "T-l")
        task = ledger.task("T-l", "owner")
        assert task["status"] == "FAILED" and "error_max_turns" in task["result"]["summary"]
    finally:
        ledger.close()
