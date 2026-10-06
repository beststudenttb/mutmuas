"""Child tasks (D-066): a task waits on the child tasks it delegated (add_job children=True) and is woken once
when each has a result, was refused or cancelled, or is past its deadline, also after a restart. Cancelling or
withdrawing a task cancels its open children."""

from __future__ import annotations

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
