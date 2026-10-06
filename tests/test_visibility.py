"""Visibility step 1: no derived exit shows a task's content to someone who is not part of it.

Content = everything past the status layer: reason, inputs, the thread, the RESULT, artifacts, and the
objective past its first 80 characters. Non-participants: another agent on the requester's own node, an
agent on another node, and a coordinator (who sees the status layer only).

Observer copies are kept only from a task's requester or owner, checked against what the node persisted
or the owner's shared task record; an artifact reference grants nothing by itself."""

import argparse
import asyncio
import json

import pytest
from conftest import auto_worker_node, eventually, interactive, worker

from mutmuas import cli, tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub, PermissionDenied
from mutmuas.ledger import Ledger
from mutmuas.mcp_server import stale_notice
from mutmuas.node import NodeDaemon
from mutmuas.protocol import ArtifactRef, Envelope
from mutmuas.visibility import artifact_visible, is_participant


SECRETS = ("SECRET-REASON", "SECRET-INPUT", "SECRET-RESULT", "SECRET-TAIL", "SECRET-ARTIFACT")
OBJECTIVE = "Summarise the lab results for the leader " + "x" * 60 + " SECRET-TAIL"


def leaks(value) -> list[str]:
    text = json.dumps(value, ensure_ascii=False, default=str)
    return [s for s in SECRETS if s in text]


async def _scenario(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main"), interactive("peer")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("other"), interactive("sec")], coordinators=["C:sec"])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = (await cluster.client(a), await cluster.client(b), await cluster.client(c))
    sent = await tools.send_request(hub_a, "A:main", "B:desk", OBJECTIVE, "SECRET-REASON",
                                    inputs={"data": "SECRET-INPUT"})
    task_id = sent["task_id"]
    await eventually(lambda: tools.inbox(hub_b, "B:desk"), what="request at B")
    await tools.accept_task(hub_b, "B:desk", task_id)
    f = tmp_path / "report.txt"
    f.write_text("SECRET-ARTIFACT")
    ref = await tools.publish_artifact(hub_b, "B:desk", str(f), task_id=task_id, description="SECRET-ARTIFACT")
    await tools.submit_result(hub_b, "B:desk", "complete", "SECRET-RESULT", task_id=task_id, artifacts=[ref])
    await tools.wait_for_result(hub_a, task_id, 20, me="A:main")
    return hub_a, hub_b, hub_c, task_id, ref


async def test_no_exit_shows_content_to_non_participants(make_config, cluster, tmp_path):
    hub_a, hub_b, hub_c, task_id, ref = await _scenario(make_config, cluster, tmp_path)

    # shared stores hold no content at all (what a raw NATS reader on any node would get)
    bus = hub_c.bus
    assert not leaks(await bus.kv_all(bus.names.tasks_kv))
    cards = await bus.kv_all(bus.names.agents_kv)
    assert not {"current_task", "queue", "open_tasks", "inbox_unread", "session_cwd"} & {
        k for card in cards.values() for k in card}

    for hub, viewer in ((hub_a, "A:peer"), (hub_c, "C:other"), (hub_c, "C:sec")):
        assert not leaks(await hub.task_view(task_id, viewer)), viewer
        assert not leaks(await tools.check_task(hub, task_id, me=viewer)), viewer
        assert not leaks(await hub.all_tasks(viewer=viewer)), viewer
        assert ref["uri"] not in json.dumps(await tools.list_artifacts(hub, viewer)), viewer
        assert "error" in await tools.fetch_artifact(hub, ref["uri"], str(hub.cfg.data_path / "dl"), me=viewer)
        assert not leaks(await tools.inbox(hub, viewer, include_seen=True)), viewer
        assert not leaks(await tools.list_agents(hub)), viewer

    # others do not even learn that the task exists; the coordinator sees its status layer
    assert await hub_a.task_view(task_id, "A:peer") is None and await hub_c.task_view(task_id, "C:other") is None
    assert task_id not in {t["task_id"] for t in await hub_c.all_tasks(viewer="C:other")}
    seen = await hub_c.task_view(task_id, "C:sec")
    assert seen["status"] == "COMPLETED" and seen["objective"].endswith("…") and seen["requester"] == "A:main"
    assert task_id in {t["task_id"] for t in await hub_c.all_tasks(viewer="C:sec")}

    # the participants still see everything
    for hub, viewer in ((hub_a, "A:main"), (hub_b, "B:desk")):
        view = await hub.task_view(task_id, viewer)
        assert "SECRET-RESULT" in json.dumps(view) and "SECRET-REASON" in json.dumps(view), viewer
    assert ref["uri"] in json.dumps(await tools.list_artifacts(hub_b, "B:desk"))


async def test_observers_get_the_content_and_become_participants(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main"), interactive("peer")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("other")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = (await cluster.client(a), await cluster.client(b), await cluster.client(c))
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "SECRET-REASON", observers=["C:other"])
    task_id = sent["task_id"]
    copy = await eventually(lambda: tools.inbox(hub_c, "C:other", peek=True), what="observer copy of REQUEST")
    assert copy[0]["body"]["copy_of"]["body"]["reason"] == "SECRET-REASON" and copy[0]["body"]["fyi"]
    await eventually(lambda: tools.inbox(hub_b, "B:desk"), what="request at B")
    await tools.submit_result(hub_b, "B:desk", "complete", "SECRET-RESULT", task_id=task_id)
    await tools.wait_for_result(hub_a, task_id, 20, me="A:main")
    await eventually(lambda: _has(hub_c, "C:other", "SECRET-RESULT"), what="observer copy of RESULT")
    assert await hub_c.task_view(task_id, "C:other") is not None               # an observer is a participant
    assert await tools.inbox(hub_c, "C:other", peek=True, types=tools.WAKE) == []   # copies never wake

    # added later by a participant: A:peer gets what exists so far
    added = await tools.add_observer(hub_a, "A:main", task_id, "A:peer")
    assert added["copies_sent"] == ["REQUEST", "RESULT"]
    await eventually(lambda: _has(hub_a, "A:peer", "SECRET-RESULT"), what="late observer copies")
    with pytest.raises(PermissionError):                                      # a non-participant cannot add anyone
        await tools.add_observer(hub_b, "B:desk", "T-not-mine", "C:other")


async def _has(hub, me, secret):
    rows = await tools.inbox(hub, me, peek=True, include_seen=True)
    return rows if secret in json.dumps(rows) else None


def test_stale_mail_program_notice():
    note = stale_notice("abc1234", "def5678")
    assert "/mcp -> Reconnect" in note["content"] and note["meta"] == {"mcp_code": "abc1234", "disk_code": "def5678"}


async def test_codex_default_tasks_listing_is_per_viewer(make_config, cluster, tmp_path, capsys):
    hub_a, hub_b, hub_c, task_id, ref = await _scenario(make_config, cluster, tmp_path)
    capsys.readouterr()
    await cli.cmd_tasks(argparse.Namespace(all=False, limit=50, json=True, as_agent="A:peer"), hub_a)
    out = capsys.readouterr().out
    assert not leaks(out) and task_id not in out
    await cli.cmd_tasks(argparse.Namespace(all=False, limit=50, json=True, as_agent="A:main"), hub_a)
    assert task_id in capsys.readouterr().out                                   # the requester still sees it


async def test_codex_no_self_promotion_by_sending_on_a_task(make_config, cluster, tmp_path):
    hub_a, hub_b, hub_c, task_id, ref = await _scenario(make_config, cluster, tmp_path)
    with pytest.raises(PermissionDenied):
        await tools.ask_question(hub_a, "A:peer", task_id, "let me in")
    with pytest.raises(PermissionDenied):
        await hub_a.reply("A:peer", task_id, "UPDATE", {"message": "hi"})
    # even a hand-made message on the task makes nobody a participant
    from mutmuas.protocol import Envelope
    await hub_a.send(Envelope(type="QUESTION", sender="A:peer", to="B:desk", task_id=task_id,
                              body={"question": "raw"}))
    await hub_c.send(Envelope(type="QUESTION", sender="C:other", to="A:main", task_id=task_id,
                              body={"question": "raw"}))
    await asyncio.sleep(1)
    assert await hub_a.task_view(task_id, "A:peer") is None
    assert await hub_c.task_view(task_id, "C:other") is None


async def test_codex_observer_added_by_owner_gets_the_result(make_config, cluster):
    a = make_config("A", [interactive("main"), interactive("peer")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("other")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = (await cluster.client(a), await cluster.client(b), await cluster.client(c))
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "SECRET-REASON")
    task_id = sent["task_id"]
    await eventually(lambda: tools.inbox(hub_b, "B:desk"), what="request at B")
    await tools.add_observer(hub_b, "B:desk", task_id, "C:other")             # the owner adds, before the result
    await eventually(lambda: _has(hub_c, "C:other", "SECRET-REASON"), what="observer got the request")
    await asyncio.sleep(1)                                                     # let the requester side sync
    await tools.submit_result(hub_b, "B:desk", "complete", "SECRET-RESULT", task_id=task_id)
    await eventually(lambda: _has(hub_c, "C:other", "SECRET-RESULT"), what="observer got the later result")
    view = await hub_c.task_view(task_id, "C:other")
    assert "SECRET-RESULT" in json.dumps(view)
    # an observer is a participant, so it may add someone too (final design), and that one sees it all
    await tools.add_observer(hub_c, "C:other", task_id, "A:peer")
    await eventually(lambda: _has(hub_a, "A:peer", "SECRET-RESULT"), what="second observer got copies")
    assert "SECRET-RESULT" in json.dumps(await hub_a.task_view(task_id, "A:peer"))


async def test_codex_lead_fyi_carries_status_only(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("lead"), worker("lab", "lab.py", notify=["B:lead"])])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    sent = await tools.send_request(hub_a, "A:main", "B:lab", OBJECTIVE, "SECRET-REASON",
                                    inputs={"action": "echo", "text": "SECRET-RESULT"})
    await tools.wait_for_result(hub_a, sent["task_id"], 30, me="A:main")
    fyis = await eventually(lambda: _n_fyis(hub_b, 2), what="lead FYIs")
    assert not leaks(fyis) and sent["task_id"] in json.dumps(fyis)            # the lead knows what, not the content


async def _n_fyis(hub, n):
    rows = await tools.inbox(hub, "B:lead", peek=True, include_seen=True)
    return rows if len(rows) >= n else None


async def test_codex_object_store_holds_no_description(make_config, cluster, tmp_path):
    hub_a, hub_b, hub_c, task_id, ref = await _scenario(make_config, cluster, tmp_path)
    raw = await hub_c.artifacts.list()                                        # unfiltered, what a raw client sees
    assert ref["uri"] in json.dumps(raw) and not leaks(raw)                   # bytes: step-2 risk, documented


async def test_codex_shared_records_hold_only_the_agreed_fields(make_config, cluster, tmp_path):
    hub_a, hub_b, hub_c, task_id, ref = await _scenario(make_config, cluster, tmp_path)
    bus = hub_c.bus
    for record in (await bus.kv_all(bus.names.tasks_kv)).values():
        assert set(record) <= {"task_id", "objective", "status", "requester", "owner", "updated_at"}, record
    allowed = {"address", "node", "agent_id", "display", "role", "capabilities", "provider", "mode",
               "accepts_kinds", "state", "availability", "session", "session_seen", "heartbeat_s", "last_heartbeat"}
    for card in (await bus.kv_all(bus.names.agents_kv)).values():
        assert set(card) <= allowed, set(card) - allowed


async def test_the_requester_writes_no_record_of_another_node_and_sends_no_request_copy(tmp_path):
    """B:ops review of the slimming (probe): a node may write only its own task records (server permissions), so
    the requester's node must not write the owner's record; the owner forwards the REQUEST's copies."""
    from conftest import auto_worker_node
    from mutmuas.server_config import node_permissions
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)          # node B
    written, sent = [], []

    class FakeBus:
        connected = True
        names = type("N", (), {"tasks_kv": f"mm_{cfg.project}_tasks",
                               "task_key": staticmethod(lambda owner, t: f"{owner.node}.{owner.agent}.{t}")})()

        async def kv_put(self, bucket, key, rec):
            written.append(f"$KV.{bucket}.{key}")

        async def publish(self, env):
            sent.append(env.to)
    hub.bus = FakeBus()
    try:
        await tools.send_request(hub, "B:desk", "C:far", "x", "y", observers=["D:watch"])
        own = f"$KV.mm_{cfg.project}_tasks.{cfg.node}."
        assert all(w.startswith(own) for w in written), written
        assert f"$KV.mm_{cfg.project}_tasks.{cfg.node}.>" in node_permissions(cfg.project, cfg.node)["publish"]
        assert sent == ["C:far"]                                       # no copy to D:watch from the requester
    finally:
        ledger.close()


async def test_the_owner_forwards_the_request_copies_after_its_record(tmp_path, monkeypatch):
    from conftest import auto_worker_node
    from mutmuas.protocol import Envelope, request_body
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    order = []

    async def record(task_id):
        order.append("record")

    async def send(env):
        order.append(("send", env.to, env.body.get("copy_of", {}).get("type")))
        return "sent"
    monkeypatch.setattr(hub, "publish_task_record", record)
    monkeypatch.setattr(hub, "send", send)
    monkeypatch.setattr(hub, "try_publish", lambda env: send(env))
    body = request_body("x", "y", observers=["D:watch"])
    env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-o", body=body)
    ledger.ingest(env)
    try:
        await daemon._on_request(agent, env)
        assert order[0] == "record" and ("send", "D:watch", "REQUEST") in order
    finally:
        ledger.close()


def _local_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[AgentConfig(id=agent_id, mode="interactive") for agent_id in agent_ids],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def _request(task_id: str = "T-review") -> Envelope:
    return Envelope(
        type="REQUEST",
        sender="A:main",
        to="B:desk",
        task_id=task_id,
        body={"objective": "review", "reason": "regression test"},
    )


async def test_observer_can_add_observer_on_a_new_node(make_config, cluster):
    """The protocol allows any participant, including an observer, to add another observer."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("other")])
    d = make_config("D", [interactive("peer")])
    for cfg in (a, b, c, d):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c, hub_d = [await cluster.client(cfg) for cfg in (a, b, c, d)]

    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "observer chain")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner task")
    await tools.add_observer(hub_b, "B:desk", sent["task_id"], "C:other")
    await eventually(
        lambda: hub_c.ledger.task(sent["task_id"], "observer:C:other"),
        what="first observer copy",
    )

    await tools.add_observer(hub_c, "C:other", sent["task_id"], "D:peer")
    await asyncio.sleep(1)

    assert hub_d.ledger.task(sent["task_id"], "observer:D:peer") is not None


def test_observer_copy_still_requires_recipient_in_participant_list(tmp_path):
    """The documented copy check covers both its sender and its recipient."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main", "peer")
    request = _request("T-copy-recipient")
    ledger.queue_outgoing(request)
    malformed = Envelope(
        type="UPDATE",
        sender="B:desk",
        to="A:peer",
        task_id=request.task_id,
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:main", "B:desk"],
            "copy_of": request.to_dict(),
        },
    )
    try:
        assert asyncio.run(daemon._on_observer_copy(malformed)) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


def test_sending_an_artifact_reference_does_not_grant_fetch_access(tmp_path):
    """Artifact access comes from publishing or receiving the exact URI, not sending it."""
    _, ledger, _, _ = _local_stack(tmp_path, "main")
    uri = "artifact://testproj/C/other/T-private/report.txt"
    try:
        ledger.queue_outgoing(
            Envelope(
                type="UPDATE",
                sender="A:main",
                to="B:desk",
                task_id="T-private",
                body={"message": "a URI I do not own"},
                artifacts=[ArtifactRef(uri=uri)],
            )
        )

        assert not artifact_visible(ledger, "A:main", uri)
    finally:
        ledger.close()


def test_existing_observer_cannot_send_task_content_copy_directly(tmp_path):
    """Observer grants are owner-relayed; observers do not get to author task copies."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main", "peer")
    request = _request("T-observer-content")
    ledger.queue_outgoing(request)
    ledger.add_observers(request.task_id, ["C:other"])
    direct_copy = Envelope(
        type="UPDATE",
        sender="C:other",
        to="A:peer",
        task_id=request.task_id,
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:main", "A:peer", "B:desk", "C:other"],
            "copy_of": {
                "type": "REQUEST",
                "from": "A:main",
                "to": "B:desk",
                "body": {"objective": "content authored by an observer"},
            },
        },
    )
    try:
        assert asyncio.run(daemon._on_observer_copy(direct_copy)) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


async def test_requester_adds_observer_without_owner_sending_duplicate_copy(make_config, cluster):
    """The owner relay is for observer-originated grants, not grants already copied by a party."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("peer")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = [await cluster.client(cfg) for cfg in (a, b, c)]
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "duplicate copy")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner task")

    await tools.add_observer(hub_a, "A:main", sent["task_id"], "C:peer")
    await eventually(
        lambda: hub_c.ledger.task(sent["task_id"], "observer:C:peer"),
        what="observer copy",
    )
    await asyncio.sleep(1)

    copies = hub_c.ledger.db.execute(
        "SELECT COUNT(*) FROM messages WHERE direction='in' AND task_id=?"
        " AND json_extract(envelope, '$.body.copy_of') IS NOT NULL",
        (sent["task_id"],),
    ).fetchone()[0]
    assert copies == 1


def _publishing_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[
            AgentConfig(
                id=agent_id,
                mode="interactive",
                permissions=["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"],
            )
            for agent_id in agent_ids
        ],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def _requested_task(ledger: Ledger, task_id: str) -> Envelope:
    request = Envelope(
        type="REQUEST",
        sender="A:main",
        to="B:desk",
        task_id=task_id,
        body={"objective": "review", "reason": "regression test"},
    )
    ledger.queue_outgoing(request)
    return request


def test_observer_copy_requires_sender_in_persisted_acl(tmp_path):
    """A sender cannot make itself authoritative with its own participant list."""
    _, ledger, _, daemon = _publishing_stack(tmp_path, "main", "peer")
    try:
        request = _requested_task(ledger, "T-observer-acl")
        forged_copy = Envelope(
            type="UPDATE",
            sender="C:other",
            to="A:peer",
            task_id=request.task_id,
            body={
                "message": "observer copy",
                "fyi": True,
                "copy_of": request.to_dict(),
                "participants": ["A:main", "A:peer", "B:desk", "C:other"],
            },
        )

        assert asyncio.run(daemon._on_observer_copy(forged_copy)) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


async def test_result_requires_actual_task_owner(tmp_path):
    """Mail about a task is not a RESULT unless it came from the persisted owner."""
    _, ledger, _, daemon = _publishing_stack(tmp_path, "main")
    try:
        request = _requested_task(ledger, "T-result-owner")
        forged_result = Envelope(
            type="RESULT",
            sender="C:other",
            to="A:main",
            task_id=request.task_id,
            body={"status": "complete", "summary": "not produced by the owner"},
        )

        await daemon._on_reply(forged_result)

        task = ledger.task(request.task_id, "requester")
        assert task["status"] == "PENDING"
        assert task["result"] is None
    finally:
        ledger.close()


def test_artifact_visibility_treats_uri_literally(tmp_path):
    """SQL wildcard characters in an artifact URI must have no special meaning."""
    _, ledger, _, _ = _publishing_stack(tmp_path, "main")
    try:
        delivered = "artifact://testproj/B/desk/T-artifact/fooXbar"
        requested = "artifact://testproj/B/desk/T-artifact/foo_bar"
        ledger.ingest(
            Envelope(
                type="UPDATE",
                sender="B:desk",
                to="A:main",
                task_id="T-artifact",
                body={"message": "artifact ready"},
                artifacts=[ArtifactRef(uri=delivered)],
            )
        )

        assert not artifact_visible(ledger, "A:main", requested)
    finally:
        ledger.close()


async def test_default_object_key_does_not_disclose_source_filename(tmp_path, monkeypatch):
    """Shared object metadata must not expose a local source filename by default."""
    _, ledger, hub, _ = _publishing_stack(tmp_path, "main")
    source = tmp_path / "layoff-plan-secret.txt"
    source.write_text("content")
    captured = {}

    async def capture_publish(src, key, **kwargs):
        captured["key"] = key
        return ArtifactRef(uri=f"artifact://testproj/{key}")

    monkeypatch.setattr(hub.artifacts, "publish", capture_publish)
    try:
        await tools.publish_artifact(hub, "A:main", str(source), task_id="T-object-key")

        assert source.name not in captured["key"]
    finally:
        ledger.close()


async def test_forged_copy_to_a_node_that_does_not_know_the_task_is_checked_against_the_task_record(
        make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("peer"), interactive("evil")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_c = await cluster.client(a), await cluster.client(c)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "SECRET-REASON")
    await eventually(lambda: hub_a.ledger.task(sent["task_id"], "requester"), what="task recorded")
    await asyncio.sleep(1)                                     # the owner's node publishes the task record
    forged = Envelope(type="UPDATE", sender="C:evil", to="C:peer", task_id=sent["task_id"], body={
        "message": "observer copy", "fyi": True, "participants": ["A:main", "B:desk", "C:evil", "C:peer"],
        "copy_of": {"type": "REQUEST", "from": "C:evil", "to": "B:desk", "body": {"objective": "FORGED"}}})
    await hub_c.send(forged)
    state = await eventually(lambda: (lambda r: r if r and r[0] not in ("new", "unverified") else None)(
        hub_c.ledger.db.execute("SELECT state FROM messages WHERE message_id=? AND direction='in'",
                                (forged.message_id,)).fetchone()), what="copy checked")
    assert state[0] == "rejected"
    assert hub_c.ledger.task(sent["task_id"], "observer:C:peer") is None
    assert not await tools.inbox(hub_c, "C:peer", peek=True)


async def test_cancel_from_someone_other_than_the_requester_is_ignored(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("evil")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c = [await cluster.client(cfg) for cfg in (a, b, c)]
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "cancel test")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner has the task")
    await hub_c.send(Envelope(type="CANCEL", sender="C:evil", to="B:desk", task_id=sent["task_id"],
                              body={"reason": "not mine to cancel"}))
    await asyncio.sleep(2)
    assert hub_b.ledger.task(sent["task_id"], "owner")["status"] == "PENDING"


async def test_default_object_key_keeps_the_extension_but_not_the_name(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main", permissions=["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"])])
    await cluster.start(a)
    hub = await cluster.client(a)
    src = tmp_path / "layoff-plan.md"
    src.write_text("x")
    ref = await tools.publish_artifact(hub, "A:main", str(src), task_id="T-key")
    assert "layoff-plan" not in ref["uri"] and ref["uri"].endswith(".md")
    from mutmuas.visibility import artifact_visible
    assert artifact_visible(hub.ledger, "A:main", ref["uri"])          # the publisher, from its own record
    named = await tools.publish_artifact(hub, "A:main", str(src), key="A/main/T-key/report.md")
    assert named["uri"].endswith("/report.md")                  # a readable name only when asked for


async def test_a_bad_observer_is_refused_before_anything_goes_out(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    try:
        with pytest.raises(ValueError):
            await tools.send_request(hub, "B:desk", "C:far", "eval", "test", observers=["C:ok", "not an address"])
        assert ledger.outbox() == [] and ledger.tasks(role="requester", limit=None) == []
    finally:
        ledger.close()
