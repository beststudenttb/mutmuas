"""agentctl's short task commands: which tool each calls, with which arguments, under which function name.

The name matters: the lease check exempts commands by fn.__name__ (cli.LEASE_FREE), so a refactoring of these
commands must keep both the call and the name."""

from __future__ import annotations

import asyncio

import pytest

from mutmuas import cli, tools

CASES = [
    (["session", "off"], "cmd_session", "set_session_taking_work", (False,), {}),
    (["session", "on"], "cmd_session", "set_session_taking_work", (True,), {}),
    (["update", "half done", "--task", "T-1", "--state", "WAITING", "--next", "B:x"], "cmd_update",
     "report_progress", ("half done", "T-1", "WAITING"), {"next": "B:x", "eta": None}),
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
    """Those that act on tasks are not lease-free (a second session may not use them); `session` is, on purpose:
    it is run from a shell beside the session."""
    for argv, name, *_ in CASES:
        assert (name in cli.LEASE_FREE) == (name == "cmd_session")
