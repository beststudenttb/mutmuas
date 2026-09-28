"""Remaining destructive race found while reviewing ``18509e2``."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

from conftest import interactive
from mutmuas import cli
from mutmuas.protocol import Envelope


PROTOCOL_DOC = (Path(__file__).parents[1] / "docs" / "MESSAGE_PROTOCOL.md").read_text()


def test_drop_mail_rechecks_after_output_at_the_delete_boundary(make_config, monkeypatch, capsys):
    """Mail arriving just after the final preview check must not be silently deleted."""
    cfg = make_config("C", [interactive("main")])
    monkeypatch.setattr(cli, "_session_processes", lambda address: [])
    messages = [Envelope(type="REQUEST", sender="A:main", to="C:guest", task_id="T-listed",
                         body={"objective": "first", "reason": "delete-boundary race"})]
    pending_checks = 0
    removed = False

    class FakeBus:
        names = SimpleNamespace(nodes_kv="nodes", agents_kv="agents")

        async def kv_get(self, bucket, key):
            return None if bucket == "nodes" else {"address": "C:guest"}

        async def inbox_pending(self, agent):
            nonlocal pending_checks
            pending_checks += 1
            count = len(messages)
            if pending_checks == 2:  # immediately after the loop's final equality check
                messages.append(Envelope(type="REQUEST", sender="B:desk", to="C:guest", task_id="T-after-check",
                                         body={"objective": "second", "reason": "arrived after final check"}))
            return count

        async def pending_messages(self, agent, limit=200):
            return list(messages)[:limit]

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

    assert not removed or "T-after-check" in output, "mail arriving after the final check was deleted unlisted"


def test_protocol_documents_that_node_ids_exclude_underscores():
    """The public address grammar must not advertise the consumer-colliding node form."""
    address_rule = next(line for line in PROTOCOL_DOC.splitlines() if "Addresses and ids used in subjects" in line)
    assert "node" in address_rule.lower() and "no `_`" in address_rule.lower()
