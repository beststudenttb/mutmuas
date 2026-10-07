"""D-108: a post's card says whether its session is at work (activity, reported by the session's Claude Code hooks:
busy when it starts on a prompt, idle when it stops) and which project it is in. busy shows the post working
even with no mutmuas task; a report not renewed for an hour counts as idle (a lost Stop hook)."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import cli, tools
from mutmuas.protocol import Envelope, request_body


class CardBus:
    """Stands in for the bus: keeps what the daemon publishes."""
    def __init__(self):
        self.names = SimpleNamespace(nodes_kv="nodes", agents_kv="agents")
        self.kv = {}

    async def kv_put(self, bucket, key, value):
        self.kv[(bucket, key)] = value


async def _card(daemon):
    daemon.hub.bus = CardBus()
    await daemon._publish_cards()
    return daemon.hub.bus.kv[("agents", "B.desk")]


def _session(ledger, agent, cwd=None):
    ledger.session_beat("B:desk", os.getpid(), str(cwd or agent.workdir_path), session_pid=os.getppid())


async def test_a_busy_session_shows_the_post_working_without_a_task(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _session(ledger, agent)
    try:
        assert (await _card(daemon))["state"] == "idle"
        args = cli.agentctl_parser().parse_args(["activity", "busy", "--as", "B:desk"])
        await cli.cmd_activity(args, hub)
        card = await _card(daemon)
        assert card["state"] == "working" and card["activity"] == "busy"
        assert datetime.fromisoformat(card["activity_at"]) > datetime.now(timezone.utc) - timedelta(minutes=1)
        await tools.report_activity(hub, "B:desk", "idle")
        card = await _card(daemon)
        assert card["state"] == "idle" and card["activity"] == "idle"
    finally:
        ledger.close()


async def test_a_busy_report_not_renewed_for_an_hour_counts_as_idle(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _session(ledger, agent)
    try:
        await tools.report_activity(hub, "B:desk", "busy")
        ledger.db.execute("UPDATE sessions SET activity_at=?",
                          ((datetime.now(timezone.utc) - timedelta(minutes=61)).isoformat(),))
        card = await _card(daemon)
        assert card["state"] == "idle" and card["activity"] == "idle"
    finally:
        ledger.close()


async def test_activity_is_reported_beside_the_session_and_needs_one(tmp_path):
    """The hook is a child of the session: no lease stands in its way. Without a session there is nothing to mark."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    try:
        assert "cmd_activity" in cli.LEASE_FREE
        with pytest.raises(KeyError, match="no session"):
            await tools.report_activity(hub, "B:desk", "busy")
        with pytest.raises(ValueError):
            await tools.report_activity(hub, "B:desk", "lunch")
    finally:
        ledger.close()


async def test_the_card_names_the_project_the_post_is_in(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path, default_project="robo")
    (agent.workdir_path / "paper").mkdir(parents=True)
    try:
        assert (await _card(daemon))["project"] == "robo"                      # nothing going on: its default
        _session(ledger, agent, agent.workdir_path / "paper")
        assert (await _card(daemon))["project"] == "paper"                     # the session's project directory
        req = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-p",
                       body={**request_body("x", "y"), "project": "vision"})
        ledger.ingest(req)
        ledger.create_owned_task(req)
        ledger.update_task("T-p", "owner", status="RUNNING")
        assert (await _card(daemon))["project"] == "vision"                    # the task in hand
    finally:
        ledger.close()


async def test_a_post_with_no_project_shows_none(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-x", "PENDING", ingest=True)
    try:
        assert "project" not in await _card(daemon)
    finally:
        ledger.close()


# --------------------------------------------------------------------------- stuck (D-108 addition)


async def test_a_post_that_is_really_held_up_shows_stuck_with_the_kind_only(tmp_path):
    """stuck on the public card: a blocked task, waiting for quota, a send that keeps failing, or a worker that
    keeps failing. stuck_reason names the kinds only, never a task or its content."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        card = await _card(daemon)
        assert "stuck" not in card and "stuck_reason" not in card
        owned_task(ledger, "T-b", "BLOCKED", ingest=True)
        card = await _card(daemon)
        assert card["stuck"] is True and card["stuck_reason"] == "blocked"
        owned_task(ledger, "T-q", "WAITING", ingest=True)
        ledger.update_task("T-q", "owner", paused=1, wait_reason="quota")
        assert (await _card(daemon))["stuck_reason"] == "blocked,quota"
        out = Envelope(type="UPDATE", sender="B:desk", to="A:sender", task_id="T-b", body={"message": "x"})
        ledger.queue_outgoing(out)
        ledger.mark_send_error(out.message_id, "BusUnavailable: no servers")
        _failing_since(ledger, out.message_id, minutes=6)
        assert (await _card(daemon))["stuck_reason"] == "blocked,quota,delivery"
        for task_id in ("T-q", "T-b"):                                         # two tasks: the worker itself
            ledger.record_failure("run", "runtime error: claude not found", address="B:desk", task_id=task_id)
        card = await _card(daemon)
        assert card["stuck_reason"] == "blocked,quota,delivery,worker" and "T-" not in card["stuck_reason"]
    finally:
        ledger.close()


async def test_stuck_clears_once_the_trouble_is_over(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        out = Envelope(type="UPDATE", sender="B:desk", to="A:sender", task_id="T-1", body={"message": "x"})
        ledger.queue_outgoing(out)
        ledger.mark_send_error(out.message_id, "BusUnavailable")
        _failing_since(ledger, out.message_id, minutes=6)
        ledger.record_failure("run", "runtime error", address="B:desk", task_id="T-1")
        ledger.record_failure("run", "runtime error", address="B:desk", task_id="T-2")
        assert (await _card(daemon))["stuck"] is True
        ledger.mark_sent(out.message_id)                                       # the send went through after all
        owned_task(ledger, "T-ok", "ACCEPTED", ingest=True)
        ledger.update_task("T-ok", "owner", status="COMPLETED")                # a run worked since the failures
        assert "stuck" not in await _card(daemon)
        ledger.record_failure("run", "runtime error", address="B:desk", task_id="T-3")   # one is not "keeps failing"
        assert "stuck" not in await _card(daemon)
    finally:
        ledger.close()


def _failing_since(ledger, message_id, minutes):
    then = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(timespec="milliseconds")
    ledger.db.execute("UPDATE messages SET created_at=? WHERE message_id=?", (then, message_id))


async def test_one_task_failing_both_attempts_does_not_leave_its_idle_post_stuck(tmp_path):
    """B:ops S1 (probe p1): a task that failed twice is FAILED; the post has nothing open and is not held up."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-x", "ACCEPTED", ingest=True)
        ledger.record_failure("run", "timed out after 3600s", address="B:desk", task_id="T-x", attempt=1)
        ledger.record_failure("run", "timed out after 3600s", address="B:desk", task_id="T-x", attempt=2)
        ledger.update_task("T-x", "owner", status="FAILED")
        card = await _card(daemon)
        assert card["availability"] == "available" and "stuck" not in card
    finally:
        ledger.close()


async def test_a_worker_failing_on_two_tasks_is_stuck_for_an_hour_at_most(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        for task_id in ("T-1", "T-2"):
            ledger.record_failure("run", "runtime error: claude not found", address="B:desk", task_id=task_id)
        assert (await _card(daemon))["stuck_reason"] == "worker"
        ledger.db.execute("UPDATE failures SET at=?",
                          ((datetime.now(timezone.utc) - timedelta(minutes=61)).isoformat(),))
        assert "stuck" not in await _card(daemon)
    finally:
        ledger.close()


async def test_a_send_that_failed_once_is_not_stuck_yet(tmp_path):
    """B:ops S3: a blip of the bus is not being held up; failing for 5 minutes is."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        out = Envelope(type="UPDATE", sender="B:desk", to="A:sender", task_id="T-1", body={"message": "x"})
        ledger.queue_outgoing(out)
        ledger.mark_send_error(out.message_id, "BusUnavailable")
        assert "stuck" not in await _card(daemon)
        _failing_since(ledger, out.message_id, minutes=6)
        assert (await _card(daemon))["stuck_reason"] == "delivery"
    finally:
        ledger.close()


async def test_a_new_session_does_not_inherit_the_old_ones_busy(tmp_path):
    """B:ops S5: a session that starts anew (another MCP process) starts idle."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _session(ledger, agent)
    try:
        await tools.report_activity(hub, "B:desk", "busy")
        ledger.session_beat("B:desk", os.getppid(), str(agent.workdir_path), session_pid=os.getppid())   # a new one
        assert ledger.session_of("B:desk")["activity"] is None
    finally:
        ledger.close()


async def test_a_broken_worker_two_tasks_each_failing_twice_shows_stuck(tmp_path):
    """B:ops S1b (probe p3): a failed run is tried again before new work, so a broken worker's last two failures are
    one task's; two different tasks failing within the hour, nothing finished, is the worker itself."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        for task_id in ("T-a", "T-b"):
            owned_task(ledger, task_id, "ACCEPTED", ingest=True)
            for attempt in (1, 2):
                ledger.record_failure("run", "runtime error: claude not found", address="B:desk", task_id=task_id,
                                      attempt=attempt)
            ledger.update_task(task_id, "owner", status="FAILED")
        assert (await _card(daemon)).get("stuck_reason") == "worker"
    finally:
        ledger.close()

