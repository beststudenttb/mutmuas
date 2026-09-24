"""Failure modes: restarts, crashes, timeouts, bad input, permissions, network loss, cancel, dishonest results."""

import asyncio
import json

import pytest
import yaml
from conftest import NatsServer, eventually, interactive, worker

from mutmuas import tools
from mutmuas.bus import Bus, BusUnavailable
from mutmuas.config import NatsConfig
from mutmuas.hub import PermissionDenied
from mutmuas.protocol import Envelope
from mutmuas.server_config import generate


async def _request(hub, to, inputs, **kw):
    return (await tools.send_request(hub, "A:main", to, "do it", "failure test", inputs=inputs, **kw))["task_id"]


async def test_node_restart_resumes_running_task(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    marker = tmp_path / "runs.log"
    task_id = await _request(hub, "B:lab", {"action": "experiment", "steps": 6, "step_s": 0.4,
                                            "marker": str(marker)}, kind="experiment")
    await eventually(lambda: marker.exists(), what="task started")
    await asyncio.sleep(0.5)
    await cluster.stop("B")                               # daemon dies mid-run; process group is killed
    view = await hub.task_view(task_id)
    assert view["status"] not in ("COMPLETED", "FAILED")

    await cluster.start(b)
    result = await tools.wait_for_result(hub, task_id, 40)
    assert result["result_status"] == "complete" and result["attempts"] == 2
    assert marker.read_text().count("attempt=2") == 1
    assert any("restarted" in tools._note(m) for m in hub.ledger.thread(task_id))


async def test_agent_process_crash_is_reported_failed(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    result = await tools.wait_for_result(hub, await _request(hub, "B:lab", {"action": "crash"}), 30)
    assert result["status"] == "FAILED" and result["result_status"] == "failed"
    assert "exit code 3" in result["result"]["summary"]
    # the reason from the process's stderr reaches the requester (e.g. a CLI out of credits)
    assert "about to crash" in result["result"]["summary"]
    assert "about to crash" in " ".join(result["result"]["outputs"]["error_lines"])


async def test_unstructured_or_dishonest_results_are_not_complete(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    silent = await tools.wait_for_result(hub, await _request(hub, "B:lab", {"action": "noresult"}), 30)
    assert silent["result_status"] == "partial"
    assert any("no submit_result" in x for x in silent["result"]["limitations"])
    liar = await tools.wait_for_result(hub, await _request(hub, "B:lab", {"action": "lie"}), 30)
    assert liar["result_status"] == "partial"
    assert any("exited with code 1" in x for x in liar["result"]["limitations"])


async def test_task_timeout_kills_process(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    task_id = await _request(hub, "B:lab", {"action": "sleep", "seconds": 30}, timeout_s=1)
    result = await tools.wait_for_result(hub, task_id, 20)
    assert result["status"] == "FAILED" and "timed out" in result["result"]["summary"]


async def test_cancel_running_task(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    task_id = await _request(hub, "B:lab", {"action": "sleep", "seconds": 30})
    await eventually(lambda: _status(hub, task_id, "RUNNING"), what="running")
    await tools.cancel_task(hub, "A:main", task_id, "no longer needed")
    result = await tools.wait_for_result(hub, task_id, 20)
    assert result["status"] == "CANCELLED"


async def test_invalid_message_does_not_break_the_node(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    subject = hub.bus.names.inbox_subject(Envelope(type="ACK", sender="A:main", to="B:lab").to_addr, "A")
    await hub.bus.js.publish(subject, b"this is not json")
    bad = Envelope(type="REQUEST", sender="A:main", to="B:lab", task_id="T-bad", body={"objective": "x"}).to_dict()
    await hub.bus.js.publish(subject, json.dumps(bad).encode())        # missing 'reason'

    err = await eventually(lambda: [m for m in hub.ledger.unseen("A:main") if m.type == "ERROR"], what="ERROR")
    assert "reason" in err[0].body["message"]
    ok = await tools.wait_for_result(hub, await _request(hub, "B:lab", {"action": "echo", "text": "still alive"}), 30)
    assert ok["result"]["summary"] == "echo: still alive"


async def test_permission_denied(make_config, cluster):
    a = make_config("A", [interactive("main"), interactive("intern", permissions=["READ"])])
    b = make_config("B", [worker("viewer", "lab.py", permissions=["READ"]),
                          worker("private", "lab.py", accept_from=["B:*"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)

    r1 = await tools.wait_for_result(hub, await _request(hub, "B:viewer", {"action": "sleep"}, kind="experiment"), 20)
    assert r1["status"] == "FAILED" and "lacks RUN_EXPERIMENT" in r1["result"]["summary"]
    r2 = await tools.wait_for_result(hub, await _request(hub, "B:private", {"action": "echo"}), 20)
    assert r2["status"] == "FAILED" and "does not accept requests from A:main" in r2["result"]["summary"]
    with pytest.raises(PermissionDenied):
        await tools.send_request(hub, "A:intern", "B:viewer", "x", "y")
    with pytest.raises(PermissionDenied):                  # cannot act as an agent of another node
        await tools.send_request(hub, "B:viewer", "B:private", "x", "y")


async def test_network_interruption(make_config, cluster, nats):
    """NATS goes down: new requests and finished results wait in local outboxes, then flow on reconnect."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    running = await _request(hub, "B:lab", {"action": "sleep", "seconds": 2})
    await eventually(lambda: _status(hub, running, "RUNNING"), what="running")

    nats.stop()
    await asyncio.sleep(0.5)
    queued = await tools.send_request(hub, "A:main", "B:lab", "echo", "sent during outage",
                                      inputs={"action": "echo", "text": "late"})
    assert queued["delivery"] == "queued"
    await asyncio.sleep(2.5)            # B's task finishes during the outage; its RESULT sits in B's outbox
    b_ledger = cluster.daemons["B"].hub.ledger
    assert b_ledger.count("out", "queued") >= 1

    nats.start()
    for task_id, summary in ((running, "slept"), (queued["task_id"], "echo: late")):
        await eventually(lambda t=task_id: (hub.ledger.task(t, "requester") or {}).get("status") == "COMPLETED",
                         timeout=30, what=f"{task_id} completed after reconnect")
        assert hub.ledger.task(task_id, "requester")["result"]["summary"] == summary


async def test_generated_server_auth_enforces_node_identity(tmp_path, make_config, cluster):
    """With the generated per-node users, the full flow works and node A cannot publish as node B."""
    from conftest import free_port
    written = generate("testproj", ["A", "B"], tmp_path / "server", listen_host="127.0.0.1",
                       store_dir=str(tmp_path / "js-auth"), monitor_port=free_port())
    server = NatsServer(tmp_path / "js-auth", conf=written["server"])
    server.start()
    try:
        def cfg(node, agents):
            c = make_config(node, agents)
            c.nats = NatsConfig(servers=[server.url], credentials_file=str(written[node]))
            return c
        a = cfg("A", [interactive("main")])
        b = cfg("B", [worker("lab", "lab.py")])
        # The handler subprocess reloads config from disk, so persist the auth settings there too.
        for c, node in ((a, "A"), (b, "B")):
            raw = yaml.safe_load(c.path.read_text())
            raw["nats"] = {"servers": [server.url], "credentials_file": str(written[node])}
            c.path.write_text(yaml.safe_dump(raw))
        await cluster.start(a)
        await cluster.start(b)
        hub = await cluster.client(a)
        result = await tools.wait_for_result(
            hub, await _request(hub, "B:lab", {"action": "experiment", "steps": 2, "step_s": 0.05},
                                kind="experiment"), 30)
        assert result["result_status"] == "complete"
        fetched = await tools.fetch_artifact(hub, result["output_refs"][0]["uri"], str(tmp_path / "dl"))
        assert fetched["size"] > 0

        forged = Envelope(type="REQUEST", sender="B:lab", to="A:main", task_id="T-forge",
                          body={"objective": "x", "reason": "y"})
        with pytest.raises(Exception):
            await hub.bus.js.publish(hub.bus.names.inbox_subject(forged.to_addr, "B"), forged.to_json(), timeout=2)
        with pytest.raises(Exception):   # cannot overwrite another node's registry card
            await hub.bus.kv_put(hub.bus.names.agents_kv, "B.lab", {"address": "B:lab", "state": "hacked"})

        with pytest.raises(BusUnavailable):
            await Bus.open(NatsConfig(servers=[server.url], user="node_A", password="wrong"), "testproj", "x",
                           reconnect=False)
    finally:
        await cluster.close()
        server.stop()


async def _status(hub, task_id, status):
    view = await hub.task_view(task_id)
    return view and view.get("status") == status


async def test_blocked_then_result_is_delivered(make_config, cluster):
    """A worker that reported BLOCKED but then submitted a result must not lose that result (bug from node B)."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    result = await tools.wait_for_result(hub, await _request(hub, "B:lab", {"action": "block_then_result"}), 30)
    assert result["status"] == "COMPLETED" and result["result_status"] == "partial"
    assert "without a shell" in result["result"]["summary"]

    # Blocked with no result stays BLOCKED (waiting for the requester), and the reason reaches A.
    task_id = await _request(hub, "B:lab", {"action": "block_only"})
    await eventually(lambda: (hub.ledger.task(task_id, "requester") or {}).get("status") == "BLOCKED",
                     what="requester sees BLOCKED")
    assert any(m["type"] == "BLOCKED" and "dataset path" in m["body"]["reason"] for m in hub.ledger.thread(task_id))


async def test_vendor_quota_pauses_the_worker_instead_of_failing_tasks(make_config, cluster, tmp_path):
    """RSI round 1: Codex ran out of credits twice on 2026-09-24 and its tasks just failed.

    Now: the quota error pauses the worker (registry shows it), its tasks stay queued, and they complete
    after `resume` -- nothing is reported as failed.
    """
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    hub_b = await cluster.client(b)
    refilled = tmp_path / "refilled"
    first = await _request(hub_a, "B:lab", {"action": "quota", "refilled": str(refilled), "text": "one"})

    card = await eventually(lambda: _card_state(hub_a, "B:lab", "unavailable"), what="worker paused")
    assert "out of credits" in card["unavailable_reason"]
    assert (await hub_a.task_view(first))["status"] not in ("FAILED", "COMPLETED")
    assert any("paused" in (m["body"].get("message") or "") for m in hub_a.ledger.thread(first))

    second = await tools.send_request(hub_a, "A:main", "B:lab", "do it", "sent while paused",
                                      inputs={"action": "quota", "refilled": str(refilled), "text": "two"})
    assert "paused" in second["note"]
    await asyncio.sleep(2)
    assert (await hub_a.task_view(second["task_id"]))["status"] in ("PENDING", "ACCEPTED")   # held, not run

    refilled.touch()
    assert (await tools.resume(hub_b, "B:lab"))["resumed"]
    for task_id, text in ((first, "one"), (second["task_id"], "two")):
        result = await tools.wait_for_result(hub_a, task_id, 30)
        assert result["result_status"] == "complete" and text in result["result"]["summary"]
    assert (await hub_a.task_view(first))["attempts"] == 1      # the quota failure did not count as an attempt
    await eventually(lambda: _card_state(hub_a, "B:lab", "idle"), what="available again")


async def test_manual_pause_until(make_config, cluster):
    b = make_config("B", [worker("lab", "lab.py")])
    hub = await cluster.client(b)
    from mutmuas.cli import _parse_until
    until = _parse_until("+1m")
    await tools.pause(hub, "B:lab", "maintenance", until)
    assert hub.ledger.pause_of("B:lab")["reason"] == "maintenance"
    hub.ledger.pause("B:lab", "expired", "2000-01-01T00:00:00.000+00:00")
    assert hub.ledger.pause_of("B:lab") is None                 # a past `until` lifts the pause by itself


async def _card_state(hub, address, state):
    card = await hub.agent_card(address)
    return card if card and card.get("state") == state else None
