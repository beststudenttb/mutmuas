"""Interaction flow v1: reply modes and read receipts, `next` wakes, channel push, session presence, follow-ups."""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from conftest import eventually, interactive

from mutmuas import tools
from mutmuas.mcp_server import SUMMARY_CHARS, channel_notice
from mutmuas.node import session_fields


async def _pair(make_config, cluster, **a_extra):
    a = make_config("A", [interactive("main")], **a_extra)
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    return a, b, await cluster.client(a), await cluster.client(b)


async def test_reply_none_is_answered_by_reading_it(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "FYI: merged f7b4664", "notice", reply="none")
    await eventually(lambda: tools.inbox(hub_b, "B:desk", peek=True), what="notice arrived")
    await asyncio.sleep(0.5)
    assert (await hub_a.task_view(sent["task_id"]))["status"] != "COMPLETED"   # peeking is not reading
    await tools.inbox(hub_b, "B:desk")                                           # the session reads it
    result = await tools.wait_for_result(hub_a, sent["task_id"], 20)
    assert result["result_status"] == "complete" and "read by B:desk" in result["result"]["summary"]
    # an ordinary REQUEST read the same way stays open: it is owed a RESULT
    owed = await tools.send_request(hub_a, "A:main", "B:desk", "please review", "needs an answer")
    await eventually(lambda: tools.inbox(hub_b, "B:desk"), what="request read")
    await asyncio.sleep(0.5)
    assert (await hub_a.task_view(owed["task_id"]))["status"] == "PENDING"


async def test_next_wakes_whoever_it_names(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "draft it", "baton test")
    await eventually(lambda: tools.inbox(hub_b, "B:desk", types=tools.WAKE), what="request")
    await tools.accept_task(hub_b, "B:desk", sent["task_id"])
    await tools.inbox(hub_a, "A:main")                                           # clear the ACK
    await tools.report_progress(hub_b, "B:desk", "plain progress", sent["task_id"])
    await tools.report_progress(hub_b, "B:desk", "your call: A or B?", sent["task_id"], next="A:main")
    woke = await tools.inbox(hub_a, "A:main", peek=True, wait_s=10, types=tools.WAKE)
    assert [m["body"]["message"] for m in woke] == ["your call: A or B?"]       # plain UPDATE still does not wake


async def test_channel_push_and_session_presence(make_config, cluster, tmp_path):
    a, b, hub_a, hub_b = await _pair(make_config, cluster)
    workdir = b.agents[0].workdir_path
    workdir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    env.pop("MUTMUAS_TASK_ID", None)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "mutmuas.cli", "mcp", "--channel", "--config", str(b.path), "--as", "B:desk",
        cwd=str(workdir), env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL)

    async def send(msg):
        proc.stdin.write((json.dumps(msg) + "\n").encode())
        await proc.stdin.drain()

    async def read_until(pred, timeout=20):
        async def loop():
            while True:
                line = await proc.stdout.readline()
                assert line, "MCP server exited"
                msg = json.loads(line)
                if pred(msg):
                    return msg
        return await asyncio.wait_for(loop(), timeout)
    try:
        await send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}})
        init = await read_until(lambda m: m.get("id") == 1)
        assert init["result"]["capabilities"]["experimental"] == {"claude/channel": {}}
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        card = await eventually(lambda: _card(hub_a, "B:desk", "online"), what="session online on the card")
        assert "session_cwd" not in card                                      # the directory is private
        mine = await tools.whoami(hub_b, "B:desk")
        assert mine["session_cwd"] == str(workdir) and "session_warning" not in mine

        sent = await tools.send_request(hub_a, "A:main", "B:desk", "wake up and review PR 7", "channel test")
        note = await read_until(lambda m: m.get("method") == "notifications/claude/channel")
        assert sent["task_id"] in note["params"]["content"] and "review PR 7" in note["params"]["content"]
        assert note["params"]["meta"] == {"task_id": sent["task_id"], "msg_type": "REQUEST", "sender": "A:main",
                                          "summary": "wake up and review PR 7"}
        assert await tools.inbox(hub_b, "B:desk", peek=True)                     # pushing did not mark it read
        listed = {c["address"]: c for c in await tools.list_agents(hub_a)}
        assert listed["B:desk"]["session"] == "online"                          # visible in `agents`, not only raw

        await tools.remind_me(hub_b, "B:desk", "+0m", "check C's reply to T3")
        reminder = await read_until(lambda m: m.get("method") == "notifications/claude/channel"
                                    and "reminder" in m["params"]["meta"])
        assert "check C's reply to T3" in reminder["params"]["content"]
    finally:
        proc.stdin.close()
        await asyncio.wait_for(proc.wait(), 15)
    await eventually(lambda: _card(hub_a, "B:desk", "offline"), what="session offline once it is gone")


async def _card(hub, address, session):
    card = await hub.agent_card(address)
    return card if card and card.get("session") == session else None


async def test_follow_ups_for_overdue_replies_and_missing_sessions(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, escalate_to=["A:desk2"])
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    late = await tools.send_request(hub_a, "A:main", "B:desk", "report", "overdue test", deadline=past)
    fyi = await tools.send_request(hub_a, "A:main", "B:desk", "notice", "no reply wanted", deadline=past, reply="none")
    hub_b.ledger.session_beat("B:desk", 999999, "/nowhere")                    # a session that has gone away
    hub_b.ledger.session_end("B:desk", 999999)
    await eventually(lambda: _card(hub_a, "B:desk", "offline"), what="B:desk session offline")
    await eventually(lambda: hub_a.task_view(late["task_id"]), what="task known")
    daemon = cluster.daemons["A"]
    await daemon._follow_ups()
    await daemon._follow_ups()                                                   # a second pass sends nothing new
    got = await eventually(lambda: _follow_ups(hub_a), what="follow-ups")
    await asyncio.sleep(0.5)
    got = await _follow_ups(hub_a)
    assert sorted((m["task_id"], m["body"]["follow_up"]) for m in got) == sorted(
        [(late["task_id"], "overdue"), (late["task_id"], "session_offline")])     # none for the reply:none notice
    assert all(m["body"]["next"] == "A:main" for m in got)                       # it wakes the requester
    assert fyi["task_id"] not in {m["task_id"] for m in got}


async def _follow_ups(hub):
    rows = await tools.inbox(hub, "A:main", peek=True, types=tools.WAKE)
    return [m for m in rows if m["body"].get("follow_up")] or None


def test_session_card_fields(tmp_path):
    now = datetime.now(timezone.utc).isoformat()
    assert session_fields(None, tmp_path) == {"session": "unknown"}
    here = session_fields({"pid": os.getpid(), "cwd": str(tmp_path), "last_seen": now}, tmp_path)
    assert here["session"] == "online" and "session_warning" not in here
    wrong = session_fields({"pid": os.getpid(), "cwd": "/", "last_seen": now}, tmp_path)
    assert "not in its workdir" in wrong["session_warning"]
    stale = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    assert session_fields({"pid": os.getpid(), "cwd": "/", "last_seen": stale}, tmp_path)["session"] == "offline"
    assert session_fields({"pid": 0, "cwd": "/", "last_seen": now}, tmp_path)["session"] == "offline"


def test_due_parsing():
    from mutmuas.cli import _parse_due
    soon = datetime.fromisoformat(_parse_due("+90m"))
    assert timedelta(minutes=89) < soon - datetime.now(timezone.utc) < timedelta(minutes=91)
    assert _parse_due("2026-09-25T18:00:00+09:00") == "2026-09-25T18:00:00+09:00"
    for bad in ("+90", "tomorrow", "2026-09-25T18:00:00"):
        with pytest.raises(SystemExit):
            _parse_due(bad)


async def test_wrong_kind_from_a_colleague_is_refused_but_seen(make_config, cluster):
    a = make_config("A", [interactive("main"), interactive("stranger")])
    b = make_config("B", [interactive("desk", accept_from=["A:main"])])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    wrong = await tools.send_request(hub_a, "A:main", "B:desk", "please fix these 4 things", "review", kind="code")
    await tools.send_request(hub_a, "A:stranger", "B:desk", "let me in", "not allowed")
    assert (await tools.wait_for_result(hub_a, wrong["task_id"], 20))["status"] == "FAILED"   # still refused
    rows = await eventually(lambda: tools.inbox(hub_b, "B:desk", peek=True, types=tools.WAKE), what="seen")
    await asyncio.sleep(0.5)
    rows = await tools.inbox(hub_b, "B:desk", peek=True, types=tools.WAKE)
    assert [m["task_id"] for m in rows] == [wrong["task_id"]]                  # the stranger stays invisible
    assert rows[0]["note"].startswith("rejected: permission denied") and "kind=code" in rows[0]["note"]
    assert "[rejected: permission denied" in channel_notice(rows[0])["content"]


async def test_clear_inbox_marks_a_backlog_read(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster)
    for i in range(3):
        await tools.send_request(hub_a, "A:main", "B:desk", f"old {i}", "backlog", reply="none")
    rows = await eventually(lambda: _n(hub_b, 3), what="backlog")
    last = max(m["seq"] for m in rows)
    fresh = await tools.send_request(hub_a, "A:main", "B:desk", "new one", "after the backlog")
    await eventually(lambda: _n(hub_b, 4), what="new one")
    assert (await tools.clear_inbox(hub_b, "B:desk", last))["marked_read"] == 3
    assert [m["task_id"] for m in await tools.inbox(hub_b, "B:desk", peek=True)] == [fresh["task_id"]]


async def _n(hub, n):
    rows = await tools.inbox(hub, "B:desk", peek=True)
    return rows if len(rows) >= n else None


def test_push_summary_is_short_and_never_empty():
    long = {"type": "REQUEST", "from": "A:x", "task_id": "T-1", "body": {"objective": "word " * 60}}
    note = channel_notice(long)
    assert len(note["meta"]["summary"]) <= SUMMARY_CHARS + 1 and note["meta"]["summary"].endswith("…")
    odd = {"type": "UPDATE", "from": "A:x", "task_id": "T-2", "body": {"state": "RUNNING", "detail": "halfway"}}
    assert channel_notice(odd)["meta"]["summary"] == "RUNNING"
