"""Deadlines and etas (D-076, D-098): given only from now (at least a second) and defaulted by kind. An eta
given on accepting reaches the requester and is chased once it passes; an overdue task is chased by its eta while
one is still to come; work waiting on a job or on child tasks is not chased."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.protocol import Envelope, request_body


def _iso(delta_s: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_s)).isoformat()


def _out(ledger, type_, task_id):
    return [e for e in ledger.outbox() if e.type == type_ and e.task_id == task_id]


async def test_an_eta_given_on_accepting_reaches_the_requester(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    agent.auto_worker = False
    owned_task(ledger, "T-e", ingest=True)
    try:
        await tools.accept_task(hub, "B:desk", "T-e", eta="+1h")
        eta = ledger.task("T-e", "owner")["eta"]
        assert timedelta(minutes=59) < datetime.fromisoformat(eta) - datetime.now(timezone.utc) <= timedelta(hours=1)
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


def _due_in_s(sent, ledger):
    from datetime import datetime, timezone
    from mutmuas.ids import parse_iso
    deadline = ledger.task(sent["task_id"], "requester")["request"]["deadline"]
    return (parse_iso(deadline) - datetime.now(timezone.utc)).total_seconds()


async def test_a_deadline_is_given_from_now(tmp_path):
    """D-102: only +N[smhd], so one in the past or without a timezone cannot be written."""
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        for bad in ("2020-01-01T00:00:00+00:00", "2099-01-01T00:00:00+00:00", "2099-01-01T00:00:00", "tomorrow"):
            with pytest.raises(ValueError, match="from now"):
                await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline=bad)
        assert ledger.tasks(role="requester") == []
    finally:
        ledger.close()


async def test_a_deadline_can_be_relative(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="+2h")
        assert 7100 < _due_in_s(sent, ledger) <= 7200
        assert not ledger.task(sent["task_id"], "requester")["request"].get("deadline_default")
    finally:
        ledger.close()


@pytest.mark.parametrize("kind,hours", [("query", 4), ("experiment", 24), ("code", 24)])
async def test_the_default_deadline_depends_on_the_kind(tmp_path, kind, hours):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "x", "y", kind=kind)
        assert hours * 3600 - 60 < _due_in_s(sent, ledger) <= hours * 3600
    finally:
        ledger.close()


@pytest.mark.parametrize("eta,chased", [(None, True), ("+1h", False)])
async def test_an_overdue_task_whose_owner_gave_an_eta_is_chased_by_the_eta(tmp_path, monkeypatch, eta, chased):
    from datetime import datetime, timedelta, timezone
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="+1h")
        task = ledger.task(sent["task_id"], "requester")
        request = {**task["request"], "deadline": "2020-01-01T00:00:00+00:00"}       # it has passed
        fields = {"request": request}
        if eta:
            fields["eta"] = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(timespec="seconds")
        ledger.update_task(sent["task_id"], "requester", **fields)

        async def online(addr):
            return {"online": True, "mode": "interactive", "auto_worker": True}
        monkeypatch.setattr(hub, "card_or_none", online)
        await daemon._follow_ups()
        follow_ups = [e for e in ledger.outbox() if e.body.get("follow_up") == "overdue"]
        assert bool(follow_ups) is chased
    finally:
        ledger.close()


@pytest.mark.parametrize("deadline", ["+0m", "+0s", "-5m"])
async def test_a_relative_deadline_must_be_ahead(tmp_path, deadline):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        with pytest.raises(ValueError):
            await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline=deadline)
    finally:
        ledger.close()


async def test_an_eta_after_the_deadline_is_told_to_the_requester_once(tmp_path):
    from datetime import datetime, timedelta, timezone
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="+1h")
        late = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat(timespec="seconds")
        for _ in range(2):
            await daemon._on_reply(Envelope(type="UPDATE", sender="C:far", to="B:desk", task_id=sent["task_id"],
                                            body={"state": "RUNNING", "message": "on it", "eta": late}))
        told = [e for e in ledger.outbox() if e.body.get("follow_up") == "eta_after_deadline"]
        assert len([e for e in told if e.to == "B:desk"]) == 1
    finally:
        ledger.close()


@pytest.mark.parametrize("status,eta_h,chased", [("RUNNING", 1, False), ("WAITING", 1, True),
                                                 ("BLOCKED", 1, True), ("RUNNING", -1, True)])
async def test_overdue_is_held_back_only_while_the_eta_will_still_be_chased(tmp_path, monkeypatch, status, eta_h,
                                                                           chased):
    from datetime import datetime, timedelta, timezone
    from conftest import backdate_deadline
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="+1h")
        backdate_deadline(ledger, sent["task_id"], "2020-01-01T00:00:00+00:00")
        eta = (datetime.now(timezone.utc) + timedelta(hours=eta_h)).isoformat(timespec="seconds")
        ledger.update_task(sent["task_id"], "requester", status=status, eta=eta)

        async def online(addr):
            return {"online": True, "mode": "interactive", "auto_worker": True}
        monkeypatch.setattr(hub, "card_or_none", online)
        await daemon._follow_ups()
        assert bool([e for e in ledger.outbox() if e.body.get("follow_up") == "overdue"]) is chased
    finally:
        ledger.close()


@pytest.mark.parametrize("text", ["+0.001s", "+0s", "+0.5s", "-1m", "10m"])
def test_a_time_from_now_is_at_least_a_second(text):
    with pytest.raises(ValueError, match="at least a second"):
        tools.from_now(text, "deadline")
    assert tools.from_now("+1s", "deadline")
