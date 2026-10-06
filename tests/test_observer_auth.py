"""D-102 re-review probe (positive): observer copies across three nodes under the generated per-node NATS
permissions. Requester A, owner B, observer C (C never saw the task, so it must check the copy against B's
shared task record). Put under tests/ and run."""
import asyncio
import json

import yaml
from conftest import NatsServer, eventually, free_port, interactive

from mutmuas import tools
from mutmuas.config import NatsConfig
from mutmuas.server_config import generate


async def _has(hub, me, secret):
    rows = await tools.inbox(hub, me, peek=True, include_seen=True)
    return rows if secret in json.dumps(rows) else None


async def _auth_cluster(tmp_path, make_config, cluster, late=()):
    written = generate("testproj", ["A", "B", "C"], tmp_path / "server", listen_host="127.0.0.1",
                       store_dir=str(tmp_path / "js-auth"), monitor_port=free_port())
    server = NatsServer(tmp_path / "js-auth", conf=written["server"])
    server.start()
    cfgs = {}
    for node, agents in (("A", [interactive("main"), interactive("peer")]), ("B", [interactive("desk")]),
                         ("C", [interactive("other")])):
        c = make_config(node, agents)
        c.nats = NatsConfig(servers=[server.url], credentials_file=str(written[node]))
        raw = yaml.safe_load(c.path.read_text())
        raw["nats"] = {"servers": [server.url], "credentials_file": str(written[node])}
        c.path.write_text(yaml.safe_dump(raw))
        cfgs[node] = c
        if node not in late:
            await cluster.start(c)
    hubs = [await cluster.client(cfgs[n]) for n in "ABC"]
    return server, hubs, cfgs


def _no_failures(*hubs):
    for hub in hubs:
        rows = hub.ledger.db.execute("SELECT * FROM failures").fetchall()
        assert not [dict(r) for r in rows], [dict(r) for r in rows]


async def test_observer_copies_pass_under_real_node_permissions(tmp_path, make_config, cluster):
    server, (hub_a, hub_b, hub_c), _ = await _auth_cluster(tmp_path, make_config, cluster)
    try:
        sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "SECRET-REASON", observers=["C:other"])
        task_id = sent["task_id"]
        await eventually(lambda: _has(hub_c, "C:other", "SECRET-REASON"), timeout=20, what="REQUEST copy at C")
        row = hub_c.ledger.task(task_id, "observer:C:other")
        assert row and row["requester"] == "A:main" and row["owner"] == "B:desk"
        await eventually(lambda: tools.inbox(hub_b, "B:desk"), what="request at B")
        await tools.submit_result(hub_b, "B:desk", "complete", "SECRET-RESULT", task_id=task_id)
        await tools.wait_for_result(hub_a, task_id, 20, me="A:main")
        await eventually(lambda: _has(hub_c, "C:other", "SECRET-RESULT"), timeout=20, what="RESULT copy at C")
        assert "SECRET-RESULT" in json.dumps(await hub_c.task_view(task_id, "C:other"))
        # an observer adds another: the owner relays, the new observer checks against B's record
        await tools.add_observer(hub_c, "C:other", task_id, "A:peer")
        await eventually(lambda: _has(hub_a, "A:peer", "SECRET-RESULT"), timeout=20, what="relayed copies")
        await asyncio.sleep(1)
        for d in cluster.daemons.values():
            _no_failures(d.hub)
    finally:
        await cluster.close()
        server.stop()


async def test_observer_gets_the_request_copy_when_the_owner_node_comes_up_late(tmp_path, make_config, cluster, caplog):
    """The owner's node is down when the request is sent: nobody may write its record but itself, so the copy
    must wait for it (on adba72c the requester's write is refused and the copy arrives before any record)."""
    server, (hub_a, hub_b, hub_c), cfgs = await _auth_cluster(tmp_path, make_config, cluster, late=("B",))
    try:
        sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "SECRET-REASON", observers=["C:other"])
        task_id = sent["task_id"]
        await asyncio.sleep(3)
        assert "could not publish task record" not in caplog.text
        await cluster.start(cfgs["B"])
        await eventually(lambda: _has(hub_c, "C:other", "SECRET-REASON"), timeout=20, what="REQUEST copy at C")
        row = hub_c.ledger.task(task_id, "observer:C:other")
        assert row and row["owner"] == "B:desk"
    finally:
        await cluster.close()
        server.stop()
