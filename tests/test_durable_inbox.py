"""Mailboxes are durable: offline, busy, restarted or never-seen receivers lose nothing; duplicates run once."""

import asyncio

from conftest import eventually, interactive, worker

from mutmuas import tools
from mutmuas.protocol import Envelope, request_body


async def test_receiver_never_online_before(make_config, cluster):
    """B has never registered when A sends. The stream keeps it; B's first start processes it."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:lab", "echo", "offline receiver",
                                    inputs={"action": "echo", "text": "stored"})
    assert "not registered yet" in sent["note"]
    view = await hub.task_view(sent["task_id"])
    assert view["status"] == "PENDING"

    await cluster.start(b)
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    assert result["result"]["summary"] == "echo: stored"


async def test_receiver_restart_keeps_backlog(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    first = await tools.send_request(hub, "A:main", "B:lab", "echo", "r1", inputs={"action": "echo", "text": "1"})
    await tools.wait_for_result(hub, first["task_id"], 30)

    await cluster.stop("B")
    ids = []
    for i in range(5):
        sent = await tools.send_request(hub, "A:main", "B:lab", "echo", "backlog",
                                        inputs={"action": "echo", "text": str(i)})
        assert "offline" in sent["note"]
        ids.append(sent["task_id"])
    await cluster.start(b)
    for i, task_id in enumerate(ids):
        result = await tools.wait_for_result(hub, task_id, 30)
        assert result["result"]["summary"] == f"echo: {i}"


async def test_duplicate_delivery_executes_once(make_config, cluster, tmp_path):
    """Same message published twice (server dedup) and re-sent under a new message id (node dedup)."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    marker = tmp_path / "executions.log"
    env = Envelope(type="REQUEST", sender="A:main", to="B:lab", task_id="T-dup-1",
                   body=request_body("count me", "dedup test", inputs={"action": "echo", "marker": str(marker)}))
    await hub.send(env)
    assert await hub.bus.publish(env) is True                          # JetStream says: duplicate
    await hub.bus.js.publish(hub.bus.names.inbox_subject(env.to_addr, "A"), env.to_json())  # no dedup header

    result = await tools.wait_for_result(hub, "T-dup-1", 30)
    assert result["result_status"] == "complete"

    # Requester retries later under a new message id: answered from the stored result, not re-run.
    resend = Envelope(type="REQUEST", sender="A:main", to="B:lab", task_id="T-dup-1", body=env.body)
    await hub.bus.publish(resend)
    await eventually(lambda: len([m for m in hub.ledger.thread("T-dup-1") if m["type"] == "RESULT"]) == 2,
                     what="replayed RESULT")
    await asyncio.sleep(1.0)       # give any wrongly re-executed copy time to show up
    assert marker.read_text().count("T-dup-1") == 1
    results = [m for m in hub.ledger.thread("T-dup-1") if m["type"] == "RESULT"]
    assert results[0]["body"] == results[1]["body"]
