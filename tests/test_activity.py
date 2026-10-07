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
        assert (await _card(daemon))["stuck_reason"] == "blocked,quota,delivery"
        for _ in range(2):
            ledger.record_failure("run", "runtime error: claude not found", address="B:desk", task_id="T-q")
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
        ledger.record_failure("run", "runtime error", address="B:desk")
        ledger.record_failure("run", "runtime error", address="B:desk")
        assert (await _card(daemon))["stuck"] is True
        ledger.mark_sent(out.message_id)                                       # the send went through after all
        owned_task(ledger, "T-ok", "ACCEPTED", ingest=True)
        ledger.update_task("T-ok", "owner", status="COMPLETED")                # a run worked since the failures
        assert "stuck" not in await _card(daemon)
        ledger.record_failure("run", "runtime error", address="B:desk")       # one failure is not "keeps failing"
        assert "stuck" not in await _card(daemon)
    finally:
        ledger.close()
