"""Destructive-path regressions found while reviewing ``exp/retire-agent-v4``."""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from conftest import interactive
from mutmuas import cli
from mutmuas.bus import Bus, Names
from mutmuas.ids import Address, InvalidAddress
from mutmuas.protocol import Envelope


def test_retire_agent_refuses_while_the_seat_process_is_still_running(make_config, monkeypatch):
    """The seat checklist requires a zero process count before its mailbox can be retired."""
    cfg = make_config("C", [interactive("main")])  # guest was removed from node.yaml already
    monkeypatch.setattr(cli, "_session_processes", lambda address: [27191])
    opened = False

    class FakeBus:
        names = SimpleNamespace(nodes_kv="nodes", agents_kv="agents")

        async def kv_get(self, bucket, key):
            return None if bucket == "nodes" else {"address": "C:guest"}

        async def inbox_pending(self, agent):
            return None

        async def remove_agent(self, agent, force=False):
            return "card removed"

        async def close(self):
            pass

    async def fake_open(*args, **kwargs):
        nonlocal opened
        opened = True
        return FakeBus()

    monkeypatch.setattr(cli.Bus, "open", fake_open)
    args = argparse.Namespace(config=str(cfg.path), id="guest", ignore_open_tasks=False, drop_mail=False)

    with pytest.raises(SystemExit):
        cli.node_retire_agent(args)
    assert not opened, "retirement reached the destructive bus path despite a live seat process"


async def test_remove_agent_does_not_report_success_when_card_deletion_fails():
    """A failed registry deletion must not be suppressed as the success text 'card removed'."""
    bus = object.__new__(Bus)
    bus.names = Names("testproj")
    bus.kv_delete = AsyncMock(side_effect=RuntimeError("registry unavailable"))
    bus.inbox_pending = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="registry unavailable"):
        await bus.remove_agent(Address("C", "guest"))


def test_drop_mail_does_not_silently_delete_mail_that_arrives_after_the_listing(
        make_config, monkeypatch, capsys):
    """Every permanently dropped message must be listed, including one racing the preview."""
    cfg = make_config("C", [interactive("main")])
    monkeypatch.setattr(cli, "_session_processes", lambda address: [])
    messages = [Envelope(type="REQUEST", sender="A:main", to="C:guest", task_id="T-listed",
                         body={"objective": "first", "reason": "race test"})]
    removed = False
    list_calls = 0

    class FakeBus:
        names = SimpleNamespace(nodes_kv="nodes", agents_kv="agents")

        async def kv_get(self, bucket, key):
            return None if bucket == "nodes" else {"address": "C:guest"}

        async def inbox_pending(self, agent):
            return len(messages)

        async def pending_messages(self, agent, limit=200):
            nonlocal list_calls
            list_calls += 1
            visible = list(messages)
            if list_calls == 1:
                messages.append(Envelope(type="REQUEST", sender="B:desk", to="C:guest", task_id="T-raced",
                                         body={"objective": "second", "reason": "arrived after preview"}))
            return visible

        async def remove_agent(self, agent, force=False):
            nonlocal removed
            removed = True
            return "card and mailbox removed"

        async def close(self):
            pass

    async def fake_open(*args, **kwargs):
        return FakeBus()

    monkeypatch.setattr(cli.Bus, "open", fake_open)
    args = argparse.Namespace(config=str(cfg.path), id="guest", ignore_open_tasks=False, drop_mail=True)
    try:
        cli.node_retire_agent(args)
    except SystemExit:
        pass
    output = capsys.readouterr().out

    assert not removed or "T-raced" in output, "a racing message was deleted without being listed"


def test_address_parser_rejects_ambiguous_node_ids_with_underscores():
    """The node-id rule must cover wire/CLI addresses, not only node.yaml generation."""
    with pytest.raises(InvalidAddress):
        Address.parse("C_a:b")
