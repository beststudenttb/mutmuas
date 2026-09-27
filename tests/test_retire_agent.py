"""agent-node retire-agent: take ONE agent (e.g. a seat) off the network without touching the node's other
agents (seat design v1, retire step 4; the node-wide `retire` could only remove everything)."""

from __future__ import annotations

import asyncio

import yaml

from conftest import eventually, interactive
from mutmuas import cli, tools
from mutmuas.ids import Address


async def _setup(make_config, cluster):
    a = make_config("A", [interactive("main")])
    c = make_config("C", [interactive("main"), interactive("guest")])
    await cluster.start(a)
    await cluster.start(c)
    hub = await cluster.client(a)
    for_guest = await tools.send_request(hub, "A:main", "C:guest", "draft", "retire test")
    for_main = await tools.send_request(hub, "A:main", "C:main", "keep me", "retire test")
    await eventually(lambda: cluster.daemons["C"].hub.ledger.task(for_guest["task_id"], "owner"), what="guest task")
    await cluster.stop("C")
    raw = yaml.safe_load(c.path.read_text())
    raw["agents"] = [x for x in raw["agents"] if x["id"] != "guest"]      # HR removes the seat first
    c.path.write_text(yaml.safe_dump(raw))
    return a, c, hub, for_guest, for_main


def _run(*argv):
    try:
        cli.agent_node(list(argv))
        return 0
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1


async def test_retire_agent_removes_one_agent_and_leaves_the_others(make_config, cluster, capsys):
    a, c, hub, for_guest, for_main = await _setup(make_config, cluster)
    main_pending = await hub.bus.inbox_pending(Address("C", "main"))

    # an open task on the seat: refused unless --force
    assert await asyncio.to_thread(_run, "retire-agent", "--config", str(c.path), "--id", "guest") != 0
    assert "guest" in {x["address"].split(":")[1] for x in await tools.list_agents(hub) if x["address"].startswith("C:")}

    assert await asyncio.to_thread(_run, "retire-agent", "--config", str(c.path), "--id", "guest", "--force") == 0
    names = {x["address"] for x in await tools.list_agents(hub)}
    assert "C:guest" not in names and "C:main" in names                 # the other agent keeps its card
    assert await hub.bus.inbox_pending(Address("C", "guest")) is None   # the seat's mailbox is gone
    assert await hub.bus.inbox_pending(Address("C", "main")) == main_pending   # the other mailbox untouched


async def test_retire_agent_refuses_an_agent_that_is_still_configured(make_config, cluster):
    a = make_config("A", [interactive("main")])
    c = make_config("C", [interactive("main"), interactive("guest")])
    await cluster.start(a)
    await cluster.start(c)
    hub = await cluster.client(a)
    code = await asyncio.to_thread(_run, "retire-agent", "--config", str(c.path), "--id", "guest", "--force")
    assert code != 0                                  # the daemon would publish its card again at once
    assert "C:guest" in {x["address"] for x in await tools.list_agents(hub)}
