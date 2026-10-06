"""D-074: with more unread mail than one page, the inbox lists the newest (the leader's still first), says how many
older ones it left out, and pages back to them with before_seq. A notifier's `since` cursor keeps arrival order.

What counts as shown: only a foreground listing (not a notifier's read, not a shell's `inbox --peek`);
clear_inbox marks read only mail shown that way. ACKs and progress are not unread mail."""

from __future__ import annotations

import argparse
import asyncio

import pytest
from conftest import auto_worker_node, eventually, interactive
from test_job_wake import _node, _node as _plain_node

from mutmuas import cli, tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import Envelope, request_body


def _mail(ledger, n: int, leader_at: int | None = None) -> list[str]:
    ids = []
    for i in range(n):
        body = {"message": f"note {i}", "next": "B:desk"}
        if i == leader_at:
            body["leader"] = True
        env = Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id=f"T-{i:02d}", body=body)
        ledger.ingest(env)
        ledger.mark_handled(env.message_id)
        ids.append(f"T-{i:02d}")
    return ids


async def test_a_long_backlog_lists_the_newest_with_the_leaders_first(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    ids = _mail(ledger, 60, leader_at=3)
    try:
        rows = await tools.inbox(daemon.hub, "B:desk", peek=True)
        listed = [r["task_id"] for r in rows]
        assert len(listed) == 50 and listed[0] == "T-03"                    # the leader's mail first (D-049)
        assert listed[1:] == ids[:2:-1][:49] and "T-59" in listed           # then newest to oldest
        assert "T-00" not in listed
    finally:
        ledger.close()


async def test_the_listing_says_what_it_left_out_and_pages_back(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _mail(ledger, 60)
    try:
        page = await tools.inbox_page(daemon.hub, "B:desk", peek=True)
        assert page["unread"] == 60 and page["listed"] == 50 and page["older_unlisted"] == 10
        before = page["before_seq"]
        assert f"before_seq={before}" in page["more"]
        older = await tools.inbox_page(daemon.hub, "B:desk", peek=True, before_seq=before)
        assert [r["task_id"] for r in older["messages"]] == [f"T-{i:02d}" for i in range(9, -1, -1)]
        assert older["older_unlisted"] == 0 and "more" not in older
    finally:
        ledger.close()


async def test_reading_marks_only_what_was_listed(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _mail(ledger, 60)
    try:
        await tools.inbox(daemon.hub, "B:desk")                                 # read: marks the 50 listed
        left = await tools.inbox(daemon.hub, "B:desk", peek=True)
        assert [r["task_id"] for r in left] == [f"T-{i:02d}" for i in range(9, -1, -1)]
    finally:
        ledger.close()


async def test_a_notifier_cursor_keeps_arrival_order(tmp_path):
    """watch / channel push read with since=<cursor> and advance it to the last row: oldest first, or mail would
    be skipped."""
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _mail(ledger, 60)
    try:
        rows = await tools.inbox(daemon.hub, "B:desk", peek=True, since="0", show=False)
        assert [r["task_id"] for r in rows][:2] == ["T-00", "T-01"]
    finally:
        ledger.close()


async def test_agentctl_inbox_pages_back_and_tells_what_is_left(tmp_path, monkeypatch, capsys):
    calls = []

    async def fake_page(hub, me, **kw):
        calls.append(kw)
        return {"messages": [], "unread": 60, "listed": 0, "older_unlisted": 10, "before_seq": 7,
                "next": {"before_seq": 7}, "more": "10 older unread: inbox(before_seq=7)"}
    monkeypatch.setattr(tools, "inbox_page", fake_page)
    args = cli.agentctl_parser().parse_args(["inbox", "--peek", "--before-seq", "11", "--as", "B:desk"])
    await cli.cmd_inbox(args, None)
    assert calls[0]["before_seq"] == 11 and calls[0]["peek"] is True
    assert "before-seq 7" in capsys.readouterr().out


async def _walk(daemon, peek=True):
    """Page from the first page back to the oldest, always with the cursor the page gives."""
    pages, cursor = [], {}
    while True:
        page = await tools.inbox_page(daemon.hub, "B:desk", peek=peek, **cursor)
        if not page["messages"]:
            break
        pages.append(page)
        cursor = page.get("next") or {}
        if not cursor:
            break
    return pages


async def test_paging_back_with_an_old_leader_message_lists_each_once(tmp_path):
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _mail(ledger, 60, leader_at=3)
    try:
        pages = await _walk(daemon)
        ids = [r["message_id"] for p in pages for r in p["messages"]]
        assert len(ids) == len(set(ids)) == 60 and [p["listed"] for p in pages] == [50, 10]
    finally:
        ledger.close()


async def test_a_first_page_of_leader_mail_pages_on_to_the_rest(tmp_path):
    import json
    _, ledger, daemon = _node(tmp_path, mode="interactive")
    _mail(ledger, 110)
    for row in ledger.db.execute("SELECT message_id, envelope FROM messages WHERE direction='in' ORDER BY rowid"
                                 " LIMIT 55").fetchall():                     # 55 leader messages: > one page
        envelope = json.loads(row["envelope"])
        envelope["body"]["leader"] = True
        ledger.db.execute("UPDATE messages SET envelope=? WHERE message_id=?", (json.dumps(envelope), row["message_id"]))
    try:
        for peek in (True, False):
            pages = await _walk(daemon, peek=peek)
            ids = [r["message_id"] for p in pages for r in p["messages"]]
            assert len(ids) == len(set(ids)) == 110, peek
            first = [r["task_id"] for r in pages[0]["messages"]]
            assert first[0] == "T-54" and all(r["body"].get("leader") for r in pages[0]["messages"])
        assert ledger.unseen_count("B:desk") == 0
    finally:
        ledger.close()


async def test_a_project_sessions_page_counts_and_pages_its_own_project_only(tmp_path):
    """Integration of D-074 paging with D-072 routing: the totals, the page and the cursor all leave out the
    requests of other projects, which the worker takes."""
    from conftest import Orphan, auto_worker_node

    from mutmuas.protocol import request_body
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    for p in ("robo", "other"):
        (agent.workdir_path / p).mkdir(parents=True)
    for i in range(70):
        body = {**request_body(f"job {i}", "x"), "project": "robo" if i < 60 else "other"}
        env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=f"T-{i:02d}", body=body)
        ledger.ingest(env)
        ledger.mark_handled(env.message_id)
    session = Orphan("import time; time.sleep(60)")
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path / "robo"), session_pid=session.pid)
        first = await tools.inbox_page(hub, "B:desk", peek=True, types=tools.WAKE)
        assert first["unread"] == 60 and first["listed"] == 50 and first["older_unlisted"] == 10
        assert {r["body"]["project"] for r in first["messages"]} == {"robo"}
        rest = await tools.inbox_page(hub, "B:desk", peek=True, types=tools.WAKE, **first["next"])
        assert rest["listed"] == 10 and "next" not in rest
    finally:
        session.kill()
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


async def test_headers_only_watch_does_not_make_body_eligible_for_clear(tmp_path, monkeypatch):
    """Seeing only a notification header is not seeing the message content."""
    cfg, ledger, hub, _ = _local_stack(tmp_path, "main")
    request = Envelope(
        type="REQUEST",
        sender="B:desk",
        to="A:main",
        task_id="T-header-only",
        body={"objective": "body never shown", "reason": "private body"},
    )
    ledger.ingest(request)
    ledger.mark_handled(request.message_id)
    (cfg.data_path / "A_main.notify-cursor").write_text("0")

    class WatchStopped(Exception):
        pass

    def stop_after_notification(*args, **kwargs):
        raise WatchStopped

    monkeypatch.setattr(cli, "_desktop_notify", stop_after_notification)
    args = argparse.Namespace(
        as_agent="A:main", interval=0, headers_only=True, dry_run=True
    )
    try:
        with pytest.raises(WatchStopped):
            await cli.cmd_watch(args, hub)

        cleared = await tools.clear_inbox(hub, "A:main", ledger.last_rowid())
        assert cleared["marked_read"] == 0
        assert ledger.unseen_count("A:main") == 1
    finally:
        ledger.close()


async def test_inbox_all_counts_as_foreground_display_for_clear(tmp_path):
    """The CLI documents --all as a way to look before using --clear-before."""
    _, ledger, hub, _ = _local_stack(tmp_path, "main")
    request = Envelope(
        type="REQUEST",
        sender="B:desk",
        to="A:main",
        task_id="T-inbox-all",
        body={"objective": "shown by inbox --all", "reason": "display test"},
    )
    ledger.ingest(request)
    ledger.mark_handled(request.message_id)
    try:
        listed = await tools.inbox(hub, "A:main", include_seen=True)
        assert [row["task_id"] for row in listed] == [request.task_id]

        cleared = await tools.clear_inbox(hub, "A:main", ledger.last_rowid())
        assert cleared["marked_read"] == 1
    finally:
        ledger.close()


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


async def test_a_shells_inbox_peek_does_not_let_clear_inbox_drop_the_mail(tmp_path, capsys):
    _, ledger, daemon = _plain_node(tmp_path, mode="interactive")
    env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-p", body=request_body("look", "test"))
    ledger.ingest(env)
    ledger.mark_handled(env.message_id)
    try:
        args = cli.agentctl_parser().parse_args(["inbox", "--peek", "--as", "B:desk"])
        await cli.cmd_inbox(args, daemon.hub)
        assert "T-p" in capsys.readouterr().out
        out = await tools.clear_inbox(daemon.hub, "B:desk", 10**9)
        assert out["marked_read"] == 0 and "left_unread" in out              # the session never saw it
    finally:
        ledger.close()


async def _handled(daemon, agent, env):
    """As the dispatcher does it: handle, then mark handled."""
    daemon.hub.ledger.ingest(env)
    state = await daemon._handle(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")
    daemon._read_if_info(env)


async def test_acks_and_progress_are_not_unread_but_mail_that_names_me_is(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "train it", "r")
        tid = sent["task_id"]
        for body, kind in (({"state": "RUNNING", "message": "accepted by C:far"}, "ACK"),
                           ({"state": "RUNNING", "message": "epoch 3 of 10"}, "UPDATE"),
                           ({"message": "half done"}, "UPDATE"),
                           ({"message": "your turn: pick a camera", "next": "B:desk"}, "UPDATE"),
                           ({"message": "C:far took a task (FYI to its lead)", "fyi": True}, "UPDATE")):
            await _handled(daemon, agent, Envelope(type=kind, sender="C:far", to="B:desk", task_id=tid, body=body))
        me = await tools.whoami(hub, "B:desk")
        assert me["inbox_unread"] == 2                                   # the one that names me, and the FYI
        page = await tools.inbox_page(hub, "B:desk", peek=True)          # only="all"
        assert sorted(m["body"]["message"] for m in page["messages"]) == [
            "C:far took a task (FYI to its lead)", "your turn: pick a camera"]
        seen = await tools.inbox(hub, "B:desk", include_seen=True, peek=True)
        assert len(seen) == 5                                            # all still there to look back on
    finally:
        ledger.close()


async def test_clear_inbox_clears_an_old_backlog_of_acks_and_progress_never_listed(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:far", "train it", "r")
        for i in range(30):                                  # a backlog from before this change: still unread
            env = Envelope(type="ACK" if i % 2 else "UPDATE", sender="C:far", to="B:desk", task_id=sent["task_id"],
                           body={"state": "RUNNING", "message": f"progress {i}"})
            ledger.ingest(env)
            ledger.mark_handled(env.message_id)
        question = Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=sent["task_id"],
                            body={"question": "which camera?"})
        ledger.ingest(question)
        ledger.mark_handled(question.message_id)
        out = await tools.clear_inbox(hub, "B:desk", ledger.last_rowid())
        assert out["marked_read"] == 30
        assert "1 message(s) never listed" in out["left_unread"]        # the question still has to be read
    finally:
        ledger.close()


@pytest.mark.parametrize("body,info", [({"next": ""}, True), ({"fyi": False}, True), ({"next": None}, True),
                                       ({"next": "B:desk"}, False), ({"fyi": True}, False)])
def test_info_is_the_same_in_python_and_in_sql(tmp_path, body, info):
    from mutmuas.ledger import INFO_SQL, is_info
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        env = Envelope(type="UPDATE", sender="C:far", to="B:desk", task_id="T-x", body={"message": "m", **body})
        ledger.ingest(env)
        in_sql = ledger.db.execute(f"SELECT {INFO_SQL} FROM messages WHERE message_id=?",
                                   (env.message_id,)).fetchone()[0]
        assert is_info(env) is info and bool(in_sql) is info
    finally:
        ledger.close()


@pytest.mark.parametrize("control", ["pause", "resume", "interrupt"])
def test_a_control_message_is_not_informational(tmp_path, control):
    from mutmuas.ledger import INFO_SQL, is_info
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        env = Envelope(type="UPDATE", sender="B:secretary", to="B:desk", task_id="T-x",
                       body={"message": "m", control: True})
        ledger.ingest(env)
        in_sql = ledger.db.execute(f"SELECT {INFO_SQL} FROM messages WHERE message_id=?",
                                   (env.message_id,)).fetchone()[0]
        assert not is_info(env) and not in_sql
    finally:
        ledger.close()
