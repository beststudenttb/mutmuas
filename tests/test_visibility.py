"""Visibility step 1: no derived exit shows a task's content to someone who is not part of it.

Content = everything past the status layer: reason, inputs, the thread, the RESULT, artifacts, and the
objective past its first 80 characters. Non-participants: another agent on the requester's own node, an
agent on another node, and a coordinator (who sees the status layer only).
"""

import asyncio
import json

from conftest import eventually, interactive

from mutmuas import tools
from mutmuas.mcp_server import stale_notice

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
        assert not leaks(await tools.history(hub, viewer)), viewer
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
    assert "SECRET-RESULT" in json.dumps(await tools.history(hub_a, "A:main", task_id))


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


# ---- A:codex review of cd60dce (T-20260925092939-ad0855d2): each scenario first reproduced, then fixed ----

import argparse  # noqa: E402

import pytest  # noqa: E402
from conftest import worker  # noqa: E402

from mutmuas import cli  # noqa: E402
from mutmuas.hub import PermissionDenied  # noqa: E402


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
