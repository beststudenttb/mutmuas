"""D-074: with more unread mail than one page, the inbox lists the newest (the leader's still first), says how many
older ones it left out, and pages back to them with before_seq. A notifier's `since` cursor keeps arrival order."""

from __future__ import annotations

from test_job_wake import _node

from mutmuas import cli, tools
from mutmuas.protocol import Envelope


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


# --------------------------------------------------------------------------- Codex light review of 374be94: paging


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
