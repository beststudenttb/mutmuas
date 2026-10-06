"""Remaining regressions found while independently reviewing ``d13ffc8``."""

from __future__ import annotations

import asyncio

from conftest import eventually, interactive
from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import Envelope
from mutmuas.visibility import is_participant


def _local_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[AgentConfig(id=agent_id, mode="interactive") for agent_id in agent_ids],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def _request(task_id: str) -> Envelope:
    return Envelope(
        type="REQUEST",
        sender="A:main",
        to="B:desk",
        task_id=task_id,
        body={"objective": "review", "reason": "regression test"},
    )


def test_existing_observer_cannot_send_task_content_copy_directly(tmp_path):
    """Observer grants are owner-relayed; observers do not get to author task copies."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main", "peer")
    request = _request("T-observer-content")
    ledger.queue_outgoing(request)
    ledger.add_observers(request.task_id, ["C:other"])
    direct_copy = Envelope(
        type="UPDATE",
        sender="C:other",
        to="A:peer",
        task_id=request.task_id,
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:main", "A:peer", "B:desk", "C:other"],
            "copy_of": {
                "type": "REQUEST",
                "from": "A:main",
                "to": "B:desk",
                "body": {"objective": "content authored by an observer"},
            },
        },
    )
    try:
        assert asyncio.run(daemon._on_observer_copy(direct_copy)) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


async def test_requester_adds_observer_without_owner_sending_duplicate_copy(make_config, cluster):
    """The owner relay is for observer-originated grants, not grants already copied by a party."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("peer")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = [await cluster.client(cfg) for cfg in (a, b, c)]
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "duplicate copy")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner task")

    await tools.add_observer(hub_a, "A:main", sent["task_id"], "C:peer")
    await eventually(
        lambda: hub_c.ledger.task(sent["task_id"], "observer:C:peer"),
        what="observer copy",
    )
    await asyncio.sleep(1)

    copies = hub_c.ledger.db.execute(
        "SELECT COUNT(*) FROM messages WHERE direction='in' AND task_id=?"
        " AND json_extract(envelope, '$.body.copy_of') IS NOT NULL",
        (sent["task_id"],),
    ).fetchone()[0]
    assert copies == 1


async def test_inbox_all_counts_as_foreground_display_for_clear(tmp_path):
    """The CLI documents --all as a way to look before using --clear-before."""
    _, ledger, hub, _ = _local_stack(tmp_path, "main")
    request = Envelope(
        type="REQUEST",
        sender="B:desk",
        to="A:main",
        task_id="T-inbox-all",
        body={"objective": "shown by inbox --all", "reason": "display test"},
    )
    ledger.ingest(request)
    ledger.mark_handled(request.message_id)
    try:
        listed = await tools.inbox(hub, "A:main", include_seen=True)
        assert [row["task_id"] for row in listed] == [request.task_id]

        cleared = await tools.clear_inbox(hub, "A:main", ledger.last_rowid())
        assert cleared["marked_read"] == 1
    finally:
        ledger.close()
