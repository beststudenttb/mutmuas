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
