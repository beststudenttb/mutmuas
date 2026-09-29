"""agentctl's short task commands: which tool each calls, with which arguments, under which function name.

The name matters: the lease check exempts commands by fn.__name__ (cli.LEASE_FREE), so a refactoring of these
commands must keep both the call and the name."""

from __future__ import annotations

import asyncio

import pytest

from mutmuas import cli, tools

CASES = [
    (["cancel", "T-1", "--reason", "why"], "cmd_cancel", "cancel_task", ("T-1", "why"), {}),
    (["cancel", "T-1"], "cmd_cancel", "cancel_task", ("T-1", ""), {}),
    (["accept", "T-1"], "cmd_accept", "accept_task", ("T-1",), {}),
    (["reject", "T-1", "no time"], "cmd_reject", "reject_task", ("T-1", "no time"), {}),
    (["question", "T-1", "which one?", "--next", "B:x"], "cmd_question", "ask_question", ("T-1", "which one?"),
     {"next": "B:x"}),
    (["question", "T-1", "which one?"], "cmd_question", "ask_question", ("T-1", "which one?"), {"next": None}),
    (["answer", "T-1", "this one", "--next", "B:x"], "cmd_answer", "answer", ("T-1", "this one"), {"next": "B:x"}),
]


@pytest.mark.parametrize("argv, name, tool, targs, tkwargs", CASES)
def test_short_command_calls_its_tool(argv, name, tool, targs, tkwargs, monkeypatch):
    calls, printed = [], []

    async def fake(hub, me, *args, **kwargs):
        calls.append((me, args, kwargs))
        return {"ok": True}

    monkeypatch.setattr(tools, tool, fake)
    monkeypatch.setattr(cli, "_print", lambda value, as_json: printed.append((value, as_json)))
    args = cli.agentctl_parser().parse_args([*argv, "--as", "A:me", "--json"])
    assert args.fn.__name__ == name and args.bus is False
    asyncio.run(args.fn(args, object()))
    assert calls == [("A:me", targs, tkwargs)]
    assert printed == [({"ok": True}, True)]


def test_short_commands_hold_the_lease():
    """None of them is lease-free: they act on tasks, so a second session may not use them."""
    for argv, name, *_ in CASES:
        assert name not in cli.LEASE_FREE
