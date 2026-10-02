"""D-073 batch 2 (spec v1.1 §3.1/3.4/3.5/3.6, §5.1; D-076 §6): the node writes arriving work into the plan and
tells the sender its place in the queue; push state per message and a push of all unread mail when the MCP server
starts; an eta on accepting, chased once it passes; requests held until the tasks they depend on are done; nudge;
a session that takes no work."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from conftest import Orphan, auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.protocol import Envelope, request_body, result_body


def _iso(delta_s: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_s)).isoformat()


def _request(task_id: str, objective: str = "label the desk images", **extra) -> Envelope:
    return Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                    body={**request_body(objective, "reason"), **extra})


async def _arrive(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    state = await daemon._on_request(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")


@pytest.fixture
def session():
    proc = Orphan("import time; time.sleep(60)")
    yield proc
    proc.kill()


def _out(ledger, type_, task_id):
    return [e for e in ledger.outbox() if e.type == type_ and e.task_id == task_id]


# --------------------------------------------------------------------------- arriving work goes on the plan


async def test_arriving_work_is_written_into_the_plan_and_the_sender_told_its_place(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    plan = agent.workdir_path / "PLAN.md"
    try:
        await _arrive(daemon, agent, _request("T-1", "first job\nwith details"))
        await _arrive(daemon, agent, _request("T-2", "second job"))
        text = plan.read_text()
        assert "## 收件" in text and "- [ ] T-1 from A:sender: first job" in text and "with details" not in text
        assert "- [ ] T-2 from A:sender: second job" in text
        [receipt] = [e for e in _out(ledger, "UPDATE", "T-2") if e.body.get("state") == "PENDING"]
        assert "2 in the queue" in receipt.body["message"] and receipt.body.get("position") == 2
        await hub.finish("T-1", result_body("complete", "done"))
        assert "T-1" not in plan.read_text() and "T-2" in plan.read_text()
    finally:
        ledger.close()


async def test_a_workers_acknowledgement_carries_its_place_too(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        await _arrive(daemon, agent, _request("T-1"))
        [ack] = _out(ledger, "ACK", "T-1")
        assert ack.body.get("position") == 1 and "1 in the queue" in ack.body["message"]
        assert "T-1" in (agent.workdir_path / "PLAN.md").read_text()
    finally:
        ledger.close()


# --------------------------------------------------------------------------- push state


def _wake_mail(ledger, n):
    for i in range(n):
        env = Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id=f"T-{i}",
                       body={"message": f"note {i}", "next": "B:desk"})
        ledger.ingest(env)
        ledger.mark_handled(env.message_id)


async def test_the_mcp_start_pushes_every_unread_message_once_more(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _wake_mail(ledger, 3)
    try:
        first, cursor = await tools.push_due(hub, "B:desk", None)          # the MCP server starts: all unread
        assert [m["task_id"] for m in first] == ["T-0", "T-1", "T-2"]
        again, cursor = await tools.push_due(hub, "B:desk", cursor)        # later: only what is new
        assert again == []
        _wake_mail(ledger, 4)                                             # four new messages arrive
        new, cursor = await tools.push_due(hub, "B:desk", cursor)
        assert len(new) == 4                                              # each new one once, the old ones not
        restart, _ = await tools.push_due(hub, "B:desk", None)            # a reconnect: all unread again
        assert len(restart) >= 4
        row = ledger.db.execute("SELECT pushed, pushed_at FROM messages WHERE task_id='T-0' AND direction='in'"
                                " ORDER BY rowid LIMIT 1").fetchone()
        assert row["pushed"] == 2 and row["pushed_at"]
        me = await tools.whoami(hub, "B:desk")
        assert me["inbox_unread"] >= 4 and me["oldest_unread_s"] >= 0 and me["last_push_at"]
    finally:
        ledger.close()


# --------------------------------------------------------------------------- eta and 催办


async def test_an_eta_given_on_accepting_reaches_the_requester(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    agent.auto_worker = False
    owned_task(ledger, "T-e", ingest=True)
    eta = _iso(3600)
    try:
        await tools.accept_task(hub, "B:desk", "T-e", eta=eta)
        assert ledger.task("T-e", "owner")["eta"] == eta
        [ack] = _out(ledger, "ACK", "T-e")
        assert ack.body["eta"] == eta
        with pytest.raises(ValueError):
            await tools.report_progress(hub, "B:desk", "x", task_id="T-e", eta="tomorrow")
    finally:
        ledger.close()


def _requested(ledger, task_id, owner="C:far", **body):
    env = Envelope(type="REQUEST", sender="B:desk", to=owner, task_id=task_id,
                   body={**request_body("train it", "need it"), **body})
    ledger.queue_outgoing(env)
    return env


async def test_a_passed_eta_is_chased_once_then_the_requester_is_told(tmp_path, monkeypatch):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    cfg.escalate_to = ["B:secretary"]

    async def card(_):
        return {"online": True, "mode": "worker", "session": "offline"}
    monkeypatch.setattr(hub, "card_or_none", card)
    _requested(ledger, "T-r")
    ledger.update_task("T-r", "requester", status="RUNNING", eta=_iso(-60))
    try:
        await daemon._chase_etas()
        await daemon._chase_etas()
        nudges = [e for e in _out(ledger, "UPDATE", "T-r") if e.to == "C:far"]
        assert len(nudges) == 1 and nudges[0].body["next"] == "C:far" and "new eta" in nudges[0].body["message"]
        ledger.db.execute("UPDATE notices SET created_at=? WHERE task_id='T-r'", (_iso(-7200),))
        await daemon._chase_etas()                                    # no new eta since: tell the requester
        told = [e for e in _out(ledger, "UPDATE", "T-r") if e.to in ("B:desk", "B:secretary")]
        assert {e.to for e in told} == {"B:desk", "B:secretary"}
        ledger.update_task("T-r", "requester", eta=_iso(-30))         # a new eta came, and passed: chased again
        await daemon._chase_etas()
        assert len([e for e in _out(ledger, "UPDATE", "T-r") if e.to == "C:far"]) == 2
    finally:
        ledger.close()


async def test_work_waiting_on_a_job_or_subtasks_is_not_chased(tmp_path, monkeypatch):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)

    async def card(_):
        return {"online": True, "mode": "worker"}
    monkeypatch.setattr(hub, "card_or_none", card)
    _requested(ledger, "T-w")
    ledger.update_task("T-w", "requester", status="WAITING", eta=_iso(-60))
    try:
        await daemon._chase_etas()
        assert not _out(ledger, "UPDATE", "T-w")
    finally:
        ledger.close()


# --------------------------------------------------------------------------- depends_on


async def test_a_request_is_held_until_what_it_depends_on_is_done(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _requested(ledger, "T-dep")
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "evaluate the model", "after training",
                                        depends_on=["T-dep"])
        assert sent["delivery"] == "held" and not _out(ledger, "REQUEST", sent["task_id"])
        await daemon._release_held()
        assert not _out(ledger, "REQUEST", sent["task_id"])
        await daemon._on_reply(Envelope(type="RESULT", sender="C:far", to="B:desk", task_id="T-dep",
                                        body=result_body("complete", "weights at runs/x")))
        await daemon._release_held()
        [req] = _out(ledger, "REQUEST", sent["task_id"])
        assert req.body["depends_on"] == ["T-dep"] and "weights at runs/x" in str(req.body["dependencies"])
    finally:
        ledger.close()


async def test_a_failed_dependency_stops_the_request_and_tells_its_sender(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _requested(ledger, "T-dep")
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "evaluate", "after training", depends_on=["T-dep"])
        await daemon._on_reply(Envelope(type="REJECT", sender="C:far", to="B:desk", task_id="T-dep",
                                        body={"reason": "no GPU"}))
        await daemon._release_held()
        assert not _out(ledger, "REQUEST", sent["task_id"])
        assert ledger.task(sent["task_id"], "requester")["status"] == "FAILED"
        notes = await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)
        assert any(n["task_id"] == sent["task_id"] and "T-dep" in n["body"]["message"] for n in notes)
    finally:
        ledger.close()


async def test_depends_on_names_a_task_this_node_knows(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        with pytest.raises(ValueError, match="T-nowhere"):
            await tools.send_request(hub, "B:desk", "C:far", "x", "y", depends_on=["T-nowhere"])
    finally:
        ledger.close()


# --------------------------------------------------------------------------- nudge


async def test_a_nudge_wakes_the_owner_once_in_a_while(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _requested(ledger, "T-n")
    try:
        await tools.nudge(hub, "B:desk", "T-n", "is it running?")
        [n] = _out(ledger, "UPDATE", "T-n")
        assert n.body["nudge"] is True and n.body["next"] == "C:far" and "is it running?" in n.body["message"]
        with pytest.raises(PermissionError, match="nudged"):
            await tools.nudge(hub, "B:desk", "T-n")
    finally:
        ledger.close()


async def test_a_nudge_requeues_a_workers_stalled_task(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-s", "ACCEPTED", ingest=True)
    try:
        env = Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id="T-s",
                       body={"message": "nudge", "nudge": True, "next": "B:desk"})
        ledger.ingest(env)
        await daemon._handle(agent, env)
        assert "T-s" in daemon._queued["B:desk"]
    finally:
        ledger.close()


# --------------------------------------------------------------------------- a session that takes no work


async def test_a_session_switched_off_leaves_the_work_to_the_worker(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        ledger.set_session_accepting("B:desk", False)
        await _arrive(daemon, agent, _request("T-o"))
        assert ledger.task("T-o", "owner")["status"] == "ACCEPTED" and "T-o" in daemon._queued["B:desk"]
        assert "T-o" not in [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)]
        ledger.set_session_accepting("B:desk", True)
        await _arrive(daemon, agent, _request("T-on"))
        assert ledger.task("T-on", "owner")["status"] == "PENDING"
    finally:
        ledger.close()
