"""Reminders (D-066, D-098): the node delivers a due reminder into the post's inbox itself (no session needed),
repeats it at its interval, and a reminder set inside a run wakes that task."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from conftest import auto_worker_node, owned_task
from test_job_wake import _node

from mutmuas import tools


def _iso(delta_s: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_s)).isoformat()


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


async def _waiting_brain(ledger, hub):
    """B:desk's task T-p waits (children job) on its child C1, which B:desk asked of C:far."""
    owned_task(ledger, "T-p", "WAITING", claim="worker", ingest=True)
    child = await tools.send_request(hub, "B:desk", "C:far", "train it", "part of T-p", parent_task="T-p")
    ledger.add_job("T-p", "B:desk", None, None, None, None, "children of T-p", children=True)
    return child["task_id"]


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
