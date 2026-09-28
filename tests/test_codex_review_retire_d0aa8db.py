"""Remaining destructive race found while reviewing ``d0aa8db``: an initially empty mailbox."""

from __future__ import annotations

import argparse
from types import SimpleNamespace

from conftest import interactive
from mutmuas import cli
from mutmuas.protocol import Envelope


def test_drop_mail_rechecks_when_the_mailbox_was_empty_at_first(make_config, monkeypatch, capsys):
    """Mail arriving after an initial count of 0 must not be deleted unlisted by --drop-mail."""
    cfg = make_config("C", [interactive("main")])
    monkeypatch.setattr(cli, "_session_processes", lambda address: [])
    messages: list[Envelope] = []
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
            if pending_checks == 1:  # right after the initial read, which saw an empty mailbox
                messages.append(Envelope(type="REQUEST", sender="B:desk", to="C:guest", task_id="T-late",
                                         body={"objective": "late", "reason": "arrived after the first count"}))
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

    assert not removed or "T-late" in output, "mail arriving after an initial count of 0 was deleted unlisted"
