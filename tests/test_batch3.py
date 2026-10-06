"""D-098 batch 3: a post's brain with no session is woken by what names it (next, its reminders, a child's question
or block); ACKs and progress no longer pile up as unread; deadlines are checked, relative, defaulted by kind and
chased by eta."""

from __future__ import annotations

import os

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.protocol import Envelope


async def _deliver(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    await daemon._handle(agent, env)


async def _waiting_brain(ledger, hub):
    """B:desk's task T-p waits (children job) on its child C1, which B:desk asked of C:far."""
    owned_task(ledger, "T-p", "WAITING", claim="worker", ingest=True)
    child = await tools.send_request(hub, "B:desk", "C:far", "train it", "part of T-p", parent_task="T-p")
    ledger.add_job("T-p", "B:desk", None, None, None, None, "children of T-p", children=True)
    return child["task_id"]


# --------------------------------------------------------------------------- 1. waking a brain with no session


@pytest.mark.parametrize("kind", ["QUESTION", "BLOCKED"])
async def test_a_child_asking_or_blocked_wakes_the_waiting_brain(tmp_path, kind):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        body = {"question": "which camera?"} if kind == "QUESTION" else {"reason": "need the calibration"}
        await _deliver(daemon, agent, Envelope(type=kind, sender="C:far", to="B:desk", task_id=child,
                                               body={**body, "next": "B:desk"}))
        task = ledger.task("T-p", "owner")
        assert task["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
        assert ledger.jobs("T-p") == []                                   # the wait ended, saying why
        [job] = ledger.jobs("T-p", open_only=False)
        assert child in job["ended"] and kind in job["ended"]
    finally:
        ledger.close()


async def test_a_brain_whose_session_is_online_is_left_to_the_session(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        ledger.session_beat("B:desk", os.getpid(), str(tmp_path), session_pid=os.getpid())
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
    finally:
        ledger.close()


async def test_an_update_naming_the_brain_next_wakes_its_blocked_task(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-b", "BLOCKED", claim="worker", ingest=True)
        await _deliver(daemon, agent, Envelope(type="ANSWER", sender="A:sender", to="B:desk", task_id="T-b",
                                               body={"answer": "use camera 2", "next": "B:desk"}))
        assert ledger.task("T-b", "owner")["status"] == "ACCEPTED" and "T-b" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_paused_brain_is_not_woken(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        ledger.update_task("T-p", "owner", paused=1)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
    finally:
        ledger.close()


async def test_a_reminder_set_in_a_run_wakes_that_task(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        await _waiting_brain(ledger, hub)
        await tools.remind_me(hub, "B:desk", "+1s", "hourly self-check", task_id="T-p")
        ledger.db.execute("UPDATE reminders SET due='2000-01-01T00:00:00.000+00:00'")
        await daemon._fire_reminders()
        assert ledger.task("T-p", "owner")["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_reminder_without_a_task_wakes_the_brains_waiting_tasks(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        await _waiting_brain(ledger, hub)
        await tools.remind_me(hub, "B:desk", "+1s", "hourly self-check")
        ledger.db.execute("UPDATE reminders SET due='2000-01-01T00:00:00.000+00:00'")
        await daemon._fire_reminders()
        assert ledger.task("T-p", "owner")["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
    finally:
        ledger.close()


def test_the_brain_is_told_to_wait_on_children_not_on_a_done_file(tmp_path):
    from mutmuas.config import NodeConfig
    from mutmuas.protocol import request_body
    from mutmuas.runtime import TaskContext, worker_prompt
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        req = Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-1", body=request_body("x", "y"))
        text = worker_prompt(TaskContext("T-1", req, agent, cfg))
        assert "children=True" in text and "not a done_file" in text and "reminder" in text
    finally:
        ledger.close()


# --------------------------------------------------------------------------- 2. ACKs and progress are not unread


async def _handled(daemon, agent, env):
    """As the dispatcher does it: handle, then mark handled."""
    daemon.hub.ledger.ingest(env)
    state = await daemon._handle(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")
    daemon._read_if_info(env)


async def test_acks_and_progress_are_not_unread_but_mail_that_names_me_is(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "train it", "r")
        tid = sent["task_id"]
        for body, kind in (({"state": "RUNNING", "message": "accepted by C:far"}, "ACK"),
                           ({"state": "RUNNING", "message": "epoch 3 of 10"}, "UPDATE"),
                           ({"message": "half done"}, "UPDATE"),
                           ({"message": "your turn: pick a camera", "next": "B:desk"}, "UPDATE"),
                           ({"message": "C:far took a task (FYI to its lead)", "fyi": True}, "UPDATE")):
            await _handled(daemon, agent, Envelope(type=kind, sender="C:far", to="B:desk", task_id=tid, body=body))
        me = await tools.whoami(hub, "B:desk")
        assert me["inbox_unread"] == 2                                   # the one that names me, and the FYI
        page = await tools.inbox_page(hub, "B:desk", peek=True)          # only="all"
        assert sorted(m["body"]["message"] for m in page["messages"]) == [
            "C:far took a task (FYI to its lead)", "your turn: pick a camera"]
        seen = await tools.inbox(hub, "B:desk", include_seen=True, peek=True)
        assert len(seen) == 5                                            # all still there to look back on
    finally:
        ledger.close()


async def test_clear_inbox_clears_an_old_backlog_of_acks_and_progress_never_listed(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "train it", "r")
        for i in range(30):                                  # a backlog from before this change: still unread
            env = Envelope(type="ACK" if i % 2 else "UPDATE", sender="C:far", to="B:desk", task_id=sent["task_id"],
                           body={"state": "RUNNING", "message": f"progress {i}"})
            ledger.ingest(env)
            ledger.mark_handled(env.message_id)
        question = Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=sent["task_id"],
                            body={"question": "which camera?"})
        ledger.ingest(question)
        ledger.mark_handled(question.message_id)
        out = await tools.clear_inbox(hub, "B:desk", ledger.last_rowid())
        assert out["marked_read"] == 30
        assert "1 message(s) never listed" in out["left_unread"]        # the question still has to be read
    finally:
        ledger.close()


# --------------------------------------------------------------------------- 3. deadlines


def _due_in_s(sent, ledger):
    from datetime import datetime, timezone
    from mutmuas.ids import parse_iso
    deadline = ledger.task(sent["task_id"], "requester")["request"]["deadline"]
    return (parse_iso(deadline) - datetime.now(timezone.utc)).total_seconds()


async def test_a_deadline_in_the_past_is_refused(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        with pytest.raises(ValueError, match="past"):
            await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="2020-01-01T00:00:00+00:00")
        with pytest.raises(ValueError, match="timezone"):
            await tools.send_request(hub, "B:desk", "C:far", "x", "y", deadline="2099-01-01T00:00:00")
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
