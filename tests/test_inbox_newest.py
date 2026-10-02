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
                "more": "10 older unread: inbox(before_seq=7)"}
    monkeypatch.setattr(tools, "inbox_page", fake_page)
    args = cli.agentctl_parser().parse_args(["inbox", "--peek", "--before-seq", "11", "--as", "B:desk"])
    await cli.cmd_inbox(args, None)
    assert calls[0]["before_seq"] == 11 and calls[0]["peek"] is True
    assert "before-seq 7" in capsys.readouterr().out
