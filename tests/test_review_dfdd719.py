"""A's own tests for the fixes to A:codex's review of dfdd719: the behaviour the fixes add or keep, next to
Codex's regression tests (test_codex_visibility_regressions.py), which only show what must be refused."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

from conftest import eventually, interactive
from mutmuas import tools
from mutmuas.node import session_alive
from mutmuas.protocol import Envelope


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


async def test_watch_needs_the_session_unless_headers_only(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("coder")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])  # a live session elsewhere
    agentctl = Path(sys.executable).parent / "agentctl"
    logs = {}
    procs = []
    try:
        hub_b.ledger.session_beat("B:coder", holder.pid, "/elsewhere", session_pid=holder.pid)
        assert session_alive(hub_b.ledger.session_of("B:coder"))
        for name, extra in (("content", []), ("headers", ["--headers-only"])):
            logs[name] = tmp_path / f"{name}.log"
            f = open(logs[name], "w")
            procs.append(subprocess.Popen([str(agentctl), "watch", *extra, "--dry-run", "--interval", "5",
                                           "--config", str(b.path), "--as", "B:coder"], stdout=f, stderr=f))
        await asyncio.sleep(2)
        await tools.send_request(hub_a, "A:main", "B:coder", "SECRET-OBJECTIVE", "watch test")
        await eventually(lambda: "notify:" in logs["headers"].read_text(), what="headers-only notice")
        headers = logs["headers"].read_text()
        assert "REQUEST from A:main" in headers and "SECRET-OBJECTIVE" not in headers
        assert "held by another session" in logs["content"].read_text()     # refused, and nothing shown
        assert "SECRET-OBJECTIVE" not in logs["content"].read_text()
    finally:
        for p in procs:
            p.terminate()
            p.wait(5)
        holder.terminate()
        holder.wait(5)


async def test_clear_inbox_only_clears_what_inbox_has_shown(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    await tools.send_request(hub_a, "A:main", "B:desk", "first", "clear test")
    shown = await eventually(lambda: tools.inbox(hub_b, "B:desk", peek=True), what="first arrived")
    later = await tools.send_request(hub_a, "A:main", "B:desk", "second, never listed", "clear test")
    await eventually(lambda: hub_b.ledger.task(later["task_id"], "owner"), what="second arrived")
    await asyncio.sleep(0.5)
    top = hub_b.ledger.db.execute("SELECT MAX(rowid) FROM messages").fetchone()[0]
    out = await tools.clear_inbox(hub_b, "B:desk", top)
    assert out["marked_read"] == len(shown) and "left_unread" in out
    left = await tools.inbox(hub_b, "B:desk", peek=True)
    assert [m["task_id"] for m in left] == [later["task_id"]]           # the unseen one is still unread


async def test_send_on_a_task_only_by_its_participants_and_only_to_the_other_one(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main"), interactive("other")])
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "send test")
    agentctl = Path(sys.executable).parent / "agentctl"

    body = tmp_path / "update.yaml"
    body.write_text("message: progress note\n")

    def send(as_agent, to):
        return subprocess.run([str(agentctl), "send", to, "--type", "UPDATE", "--task", sent["task_id"],
                               "--file", str(body), "--config", str(a.path), "--as", as_agent],
                              capture_output=True, text=True, timeout=60)
    outsider = send("A:other", "B:desk")
    assert outsider.returncode != 0 and "not the requester or owner" in outsider.stderr
    wrong_peer = send("A:main", "A:other")
    assert wrong_peer.returncode != 0 and "go to B:desk" in wrong_peer.stderr
    ok = send("A:main", "B:desk")
    assert ok.returncode == 0, ok.stderr



# Second round (A:codex's review of 8c018ee): behaviour its tests do not reach.

async def test_unverified_copies_are_checked_again_after_a_daemon_restart(tmp_path, monkeypatch):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.hub import Hub
    from mutmuas.ledger import Ledger
    from mutmuas.node import NodeDaemon
    from mutmuas.visibility import is_participant
    cfg = NodeConfig(project="testproj", node="A", data_dir=str(tmp_path / "data"),
                     agents=[AgentConfig(id="peer", mode="interactive")]).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    copy = Envelope(type="UPDATE", sender="A:main", to="A:peer", task_id="T-restart", body={
        "message": "observer copy", "fyi": True, "participants": ["A:main", "A:peer", "B:desk"],
        "copy_of": {"type": "REQUEST", "from": "A:main", "to": "B:desk", "body": {"objective": "x"}}})
    ledger.ingest(copy)
    ledger.mark_handled(copy.message_id, "unverified")          # the daemon stopped while it waited
    hub.bus = object()

    async def record(task_id, owner):
        return {"task_id": task_id, "requester": "A:main", "owner": "B:desk"}
    monkeypatch.setattr(hub, "_remote_task", record)
    monkeypatch.setattr(hub, "flush_outbox", lambda: asyncio.sleep(0))
    try:
        await daemon.recover()
        await asyncio.sleep(0.2)
        assert is_participant(ledger, "A:peer", "T-restart")
    finally:
        ledger.close()


async def test_a_notifier_read_does_not_count_as_shown(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    await tools.send_request(hub_a, "A:main", "B:desk", "pushed, not listed", "shown test")
    await eventually(lambda: tools.inbox(hub_b, "B:desk", peek=True, show=False), what="arrived")  # e.g. push
    top = hub_b.ledger.db.execute("SELECT MAX(rowid) FROM messages").fetchone()[0]
    assert (await tools.clear_inbox(hub_b, "B:desk", top))["marked_read"] == 0
    assert len(await tools.inbox(hub_b, "B:desk", peek=True)) == 1
