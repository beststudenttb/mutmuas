"""D-073 batch 2 (spec v1.1 §3.1/3.4/3.5/3.6, §5.1; D-076 §6): the node writes arriving work into the plan and
tells the sender its place in the queue; push state per message and a push of all unread mail when the MCP server
starts; an eta on accepting, chased once it passes; a session that takes no work."""

from __future__ import annotations

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


# --------------------------------------------------------------------------- nudge


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


# --------------------------------------------------------------------------- Codex full review of b442f2c


async def test_finish_does_not_overwrite_a_line_the_node_adds_meanwhile(tmp_path, monkeypatch):
    """Another node writer (a new 收件 line) arrives while finish rewrites PLAN.md: it must not be lost."""
    import threading
    from pathlib import Path
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    board = agent.workdir_path / "PLAN.md"
    owned_task(ledger, "T-done", ingest=True)
    owned_task(ledger, "T-new", ingest=True)
    board.parent.mkdir(parents=True, exist_ok=True)
    board.write_text("# PLAN\n## [>] T-done test\n- [x] done\n\n## 收件\n")
    other = []

    def meanwhile():                                   # the moment finish writes the board back
        if not other:
            other.append(threading.Thread(target=hub.add_inbox_line, args=(ledger.task("T-new", "owner"),)))
            other[0].start()
            other[0].join(0.5)
    write_text, replace = Path.write_text, Path.replace

    def patched_write(path, *a, **k):
        if path == board:
            meanwhile()
        return write_text(path, *a, **k)

    def patched_replace(path, target):
        if target == board:
            meanwhile()
        return replace(path, target)
    monkeypatch.setattr(Path, "write_text", patched_write)
    monkeypatch.setattr(Path, "replace", patched_replace)
    try:
        await hub.finish("T-done", result_body("complete", "done"))
        other[0].join(5)
        text = board.read_text()
        assert "T-new" in text and "T-done test" not in text
        assert "- [x] done" in ledger.task("T-done", "owner")["result"]["outputs"]["plan"]
    finally:
        ledger.close()


async def test_the_start_push_covers_every_unread_message_oldest_first(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _wake_mail(ledger, 75)
    try:
        first, cursor = await tools.push_due(hub, "B:desk", None)
        assert len(first) == 75 and [m["seq"] for m in first] == sorted(m["seq"] for m in first)
        assert (await tools.push_due(hub, "B:desk", cursor))[0] == []
        assert ledger.db.execute("SELECT count(*) FROM messages WHERE direction='in' AND pushed=0").fetchone()[0] == 0
    finally:
        ledger.close()


async def test_off_is_refused_without_a_worker_to_take_the_work(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        agent.auto_worker = False
        with pytest.raises(PermissionError, match="worker"):
            await tools.set_session_taking_work(hub, "B:desk", False)
        agent.auto_worker = True
        assert (await tools.set_session_taking_work(hub, "B:desk", False))["session_takes_work"] is False
    finally:
        ledger.close()


async def test_a_refused_result_leaves_the_plan_section_alone(tmp_path):
    """Codex review of ea40b88: an invalid RESULT (empty summary) was refused after the task's PLAN section had
    already been taken off the board."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-s", "RUNNING", ingest=True)
    board = agent.workdir_path / "PLAN.md"
    board.parent.mkdir(parents=True, exist_ok=True)
    board.write_text("# PLAN\n## [>] T-s\n- [ ] important unsaved work\n")
    try:
        with pytest.raises(Exception):
            await tools.submit_result(hub, "B:desk", "complete", "", task_id="T-s")
        task = ledger.task("T-s", "owner")
        assert task["status"] == "RUNNING" and task["result"] is None
        assert "important unsaved work" in board.read_text()
    finally:
        ledger.close()
