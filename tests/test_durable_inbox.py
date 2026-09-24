"""Mailboxes are durable: offline, busy, restarted or never-seen receivers lose nothing; duplicates run once."""

import asyncio

from conftest import eventually, interactive, worker

from mutmuas import tools
from mutmuas.ids import Address
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


def test_ledger_migrates_v1_schema(tmp_path):
    """v1 ledgers (messages keyed on message_id only) are re-keyed in place without losing rows."""
    import sqlite3

    from mutmuas.ledger import Ledger
    db = sqlite3.connect(tmp_path / "ledger.sqlite3")
    db.executescript("""
        CREATE TABLE messages (message_id TEXT PRIMARY KEY, direction TEXT NOT NULL, local_agent TEXT NOT NULL,
            peer TEXT NOT NULL, type TEXT NOT NULL, task_id TEXT, conversation_id TEXT, envelope TEXT NOT NULL,
            state TEXT NOT NULL, seen INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE INDEX messages_state ON messages(direction, state);
        CREATE INDEX messages_task ON messages(task_id);""")
    env = Envelope(type="REQUEST", sender="B:main", to="B:ops", task_id="T-1", body=request_body("x", "y"))
    db.execute("INSERT INTO messages VALUES (?, 'out', 'B:main', 'B:ops', 'REQUEST', 'T-1', 'c', ?, 'sent',"
               " 1, 1, NULL, 'now', 'now')", (env.message_id, env.to_json().decode()))
    db.commit()
    db.close()

    ledger = Ledger(tmp_path / "ledger.sqlite3")
    assert ledger.count("out", "sent") == 1
    assert ledger.ingest(env) is True            # the same-node inbound copy is no longer swallowed
    assert ledger.ingest(env) is False           # but real duplicates still are
    ledger.close()
    Ledger(tmp_path / "ledger.sqlite3").close()  # idempotent on the migrated schema


async def test_renamed_and_retired_agents_leave_no_ghosts(make_config, cluster, tmp_path):
    """Renaming an agent (A:main -> A:claude) or retiring a node removes stale cards and empty mailboxes."""
    import yaml as _yaml

    from mutmuas import cli
    a = make_config("A", [interactive("main")])
    c = make_config("C", [interactive("main"), interactive("load")])
    await cluster.start(a)
    await cluster.start(c)
    hub = await cluster.client(a)
    names = {x["address"] for x in await tools.list_agents(hub)}
    assert {"A:main", "C:main", "C:load"} <= names

    # rename A:main -> A:claude (keeping the old name as display alias)
    await cluster.stop("A")
    raw = _yaml.safe_load(a.path.read_text())
    raw["agents"][0].update(id="claude", display="A:main")
    a.path.write_text(_yaml.safe_dump(raw))
    from mutmuas.config import load_config
    await cluster.start(load_config(a.path))
    names = {x["address"] for x in await tools.list_agents(hub)}
    assert "A:main" not in names and "A:claude" in names
    assert await hub.resolve("A:main") == "A:claude"               # old name still reaches the agent
    assert await hub.bus.inbox_pending(Address("A", "main")) is None  # empty mailbox removed

    # retire node C entirely
    await cluster.stop("C")
    import asyncio
    await asyncio.to_thread(cli.agent_node, ["retire", "--config", str(c.path), "--force"])
    assert not [x for x in await tools.list_agents(hub) if x["address"].startswith("C:")]
    assert "C" not in {n["node"] for n in await hub.nodes()}
