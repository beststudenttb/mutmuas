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


async def test_a_reminder_without_a_task_only_goes_to_the_inbox(tmp_path):
    """B:ops review of d385143: a reminder a session set (no task), e.g. every 5 h, woke every waiting task of the
    post each time. It only lands in the inbox; one set in a worker run wakes its task (above)."""
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-a", "WAITING", claim="worker", ingest=True)
        ledger.add_job("T-a", "B:desk", 999999, None, "/tmp/log", None, "training a")
        owned_task(ledger, "T-b", "WAITING", claim="worker", ingest=True)
        ledger.add_job("T-b", "B:desk", 999998, None, "/tmp/log", None, "training b")
        await tools.remind_me(hub, "B:desk", "+1s", "leader: look at the paper draft", every="5h")
        ledger.db.execute("UPDATE reminders SET due='2000-01-01T00:00:00.000+00:00'")
        await daemon._fire_reminders()
        assert [ledger.task(t, "owner")["status"] for t in ("T-a", "T-b")] == ["WAITING", "WAITING"]
        assert ledger.jobs("T-a") and ledger.jobs("T-b") and not daemon._queued["B:desk"]
        assert any("paper draft" in (m["body"].get("message") or "")
                   for m in await tools.inbox(hub, "B:desk", peek=True))
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


# --------------------------------------------------------------------------- secretary/C review of d385143


@pytest.mark.parametrize("session", ["off", "other project"])
async def test_a_session_that_leaves_the_work_to_the_worker_does_not_stop_the_wake(tmp_path, session):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        cwd = agent.workdir_path
        if session == "other project":
            cwd = agent.workdir_path / "other"
            cwd.mkdir(parents=True)
        ledger.session_beat("B:desk", os.getpid(), str(cwd), session_pid=os.getpid())
        if session == "off":
            ledger.set_session_accepting("B:desk", False)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
    finally:
        ledger.close()


@pytest.mark.parametrize("body,info", [({"next": ""}, True), ({"fyi": False}, True), ({"next": None}, True),
                                       ({"next": "B:desk"}, False), ({"fyi": True}, False)])
def test_info_is_the_same_in_python_and_in_sql(tmp_path, body, info):
    from mutmuas.ledger import INFO_SQL, is_info
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        env = Envelope(type="UPDATE", sender="C:far", to="B:desk", task_id="T-x", body={"message": "m", **body})
        ledger.ingest(env)
        in_sql = ledger.db.execute(f"SELECT {INFO_SQL} FROM messages WHERE message_id=?",
                                   (env.message_id,)).fetchone()[0]
        assert is_info(env) is info and bool(in_sql) is info
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


async def test_a_held_requests_relative_deadline_counts_from_its_release(tmp_path):
    from mutmuas.protocol import result_body
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        dep = await tools.send_request(hub, "B:desk", "C:far", "train", "dep")
        held = await tools.send_request(hub, "B:desk", "C:rl", "evaluate", "after", depends_on=[dep["task_id"]],
                                        deadline="+2h")
        assert "deadline" not in ledger.task(held["task_id"], "requester")["request"]     # not started yet
        await daemon._on_reply(Envelope(type="RESULT", sender="C:far", to="B:desk", task_id=dep["task_id"],
                                        body=result_body("complete", "trained")))
        await daemon._release_held()
        assert 7100 < _due_in_s(held, ledger) <= 7200
        assert not ledger.task(held["task_id"], "requester")["request"].get("deadline_default")
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


async def test_the_wake_tells_the_brain_its_wait_has_ended(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        [job] = ledger.jobs("T-p", open_only=False)
        assert "add_job again" in job["ended"]
    finally:
        ledger.close()


async def test_a_resume_of_a_paused_task_waiting_on_a_job_keeps_it_waiting(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-j", "WAITING", claim="worker", ingest=True)
        ledger.add_job("T-j", "B:desk", 999999, None, "/tmp/log", None, "training")
        ledger.update_task("T-j", "owner", paused=1)
        await _deliver(daemon, agent, Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id="T-j",
                                               body={"message": "resume", "resume": True, "next": "B:desk"}))
        task = ledger.task("T-j", "owner")
        assert not task["paused"] and task["status"] == "WAITING" and ledger.jobs("T-j")
        assert "T-j" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_childs_result_leaves_the_parent_waiting_for_its_other_children(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        c1 = await _waiting_brain(ledger, hub)
        await tools.send_request(hub, "B:desk", "C:far", "eval", "part of T-p", parent_task="T-p")
        await _deliver(daemon, agent, Envelope(type="RESULT", sender="C:far", to="B:desk", task_id=c1,
                                               body={"status": "complete", "summary": "ok", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
        assert "T-p" not in daemon._queued["B:desk"]
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


@pytest.mark.parametrize("control", ["pause", "resume", "interrupt"])
def test_a_control_message_is_not_informational(tmp_path, control):
    from mutmuas.ledger import INFO_SQL, is_info
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        env = Envelope(type="UPDATE", sender="B:secretary", to="B:desk", task_id="T-x",
                       body={"message": "m", control: True})
        ledger.ingest(env)
        in_sql = ledger.db.execute(f"SELECT {INFO_SQL} FROM messages WHERE message_id=?",
                                   (env.message_id,)).fetchone()[0]
        assert not is_info(env) and not in_sql
    finally:
        ledger.close()
