"""Visibility step 1: no derived exit shows a task's content to someone who is not part of it.

Content = everything past the status layer: reason, inputs, the thread, the RESULT, artifacts, and the
objective past its first 80 characters. Non-participants: another agent on the requester's own node, an
agent on another node, and a coordinator (who sees the status layer only).
"""

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
    try:
        await tools.add_observer(hub_a, "A:peer", task_id, "C:other")         # an observer cannot add people
        raise AssertionError("an observer added another observer")
    except PermissionError:
        pass


async def _has(hub, me, secret):
    rows = await tools.inbox(hub, me, peek=True, include_seen=True)
    return rows if secret in json.dumps(rows) else None


def test_stale_mail_program_notice():
    note = stale_notice("abc1234", "def5678")
    assert "/mcp -> Reconnect" in note["content"] and note["meta"] == {"mcp_code": "abc1234", "disk_code": "def5678"}
