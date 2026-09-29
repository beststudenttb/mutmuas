"""Regressions found while independently reviewing ``d98413f``."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import Envelope


async def test_inbox_all_does_not_show_an_unverified_observer_copy(tmp_path, monkeypatch):
    """`--all` means read + unread mail, not copies still awaiting authorization."""
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[AgentConfig(id="peer", mode="interactive")],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    hub.bus = SimpleNamespace()
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon.VERIFY_BACKOFF_S = (60,)

    async def no_record(task_id, owner):
        return None

    monkeypatch.setattr(hub, "_remote_task", no_record)
    copy = Envelope(
        type="UPDATE",
        sender="B:desk",
        to="A:peer",
        task_id="T-not-verified",
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:peer", "B:desk"],
            "copy_of": {
                "type": "REQUEST",
                "from": "C:requester",
                "to": "B:desk",
                "body": {"objective": "must stay hidden", "reason": "not authorized yet"},
            },
        },
    )
    ledger.ingest(copy)
    assert daemon._on_observer_copy(copy) == "unverified"
    ledger.mark_handled(copy.message_id, "unverified")
    await asyncio.sleep(0)

    try:
        assert await tools.inbox(hub, "A:peer", include_seen=True, peek=True) == []
    finally:
        background = list(daemon._background)
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        ledger.close()
