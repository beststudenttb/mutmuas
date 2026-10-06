"""The MCP server is how Claude Code / Codex use the network. Drive it over real stdio like they do.

When the MCP server starts it pushes every unread message once more, oldest first."""

import json
import sys

from conftest import auto_worker_node, interactive, worker
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from mutmuas import tools
from mutmuas.protocol import Envelope


async def call(session, name, **args):
    res = await session.call_tool(name, args)
    assert not getattr(res, "is_error", False), res
    return json.loads(res.content[0].text)


async def test_agent_delegates_through_mcp(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py", capabilities=["isaac_lab"])])
    await cluster.start(a)
    await cluster.start(b)

    params = StdioServerParameters(command=sys.executable, args=["-m", "mutmuas.cli", "mcp", "--as", "A:main"],
                                   env={"MUTMUAS_CONFIG": str(a.path), "PATH": "/usr/bin:/bin"}, cwd=str(tmp_path))
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        names = {t.name for t in (await session.list_tools()).tools}
        assert {"find_agent", "send_request", "wait_for_result", "fetch_artifact", "submit_result",
                "publish_artifact", "inbox", "list_agents", "check_task"} <= names

        me = await call(session, "whoami")
        assert me["address"] == "A:main"
        best = (await call(session, "find_agent", capability="isaac_lab"))["best"]["address"]
        sent = await call(session, "send_request", to=best, objective="run a short experiment",
                          reason="mcp test", kind="experiment",
                          inputs={"action": "experiment", "steps": 2, "step_s": 0.05})
        result = await call(session, "wait_for_result", task_id=sent["task_id"], timeout_s=30)
        assert result["result_status"] == "complete"
        fetched = await call(session, "fetch_artifact", uri=result["output_refs"][0]["uri"])
        assert fetched["path"].startswith(str(tmp_path))


def _wake_mail(ledger, n):
    for i in range(n):
        env = Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id=f"T-{i}",
                       body={"message": f"note {i}", "next": "B:desk"})
        ledger.ingest(env)
        ledger.mark_handled(env.message_id)


async def test_the_mcp_start_pushes_every_unread_message_once_more(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _wake_mail(ledger, 3)
    try:
        first, cursor = await tools.push_due(hub, "B:desk", None)          # the MCP server starts: all unread
        assert [m["task_id"] for m in first] == ["T-0", "T-1", "T-2"]
        again, cursor = await tools.push_due(hub, "B:desk", cursor)        # later: only what is new
        assert again == []
        _wake_mail(ledger, 4)                                             # four new messages arrive
        new, cursor = await tools.push_due(hub, "B:desk", cursor)
        assert len(new) == 4                                              # each new one once, the old ones not
        restart, _ = await tools.push_due(hub, "B:desk", None)            # a reconnect: all unread again
        assert len(restart) >= 4
        row = ledger.db.execute("SELECT pushed, pushed_at FROM messages WHERE task_id='T-0' AND direction='in'"
                                " ORDER BY rowid LIMIT 1").fetchone()
        assert row["pushed"] == 2 and row["pushed_at"]
        me = await tools.whoami(hub, "B:desk")
        assert me["inbox_unread"] >= 4 and me["oldest_unread_s"] >= 0 and me["last_push_at"]
    finally:
        ledger.close()


async def test_the_start_push_covers_every_unread_message_oldest_first(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    _wake_mail(ledger, 75)
    try:
        first, cursor = await tools.push_due(hub, "B:desk", None)
        assert len(first) == 75 and [m["seq"] for m in first] == sorted(m["seq"] for m in first)
        assert (await tools.push_due(hub, "B:desk", cursor))[0] == []
        assert ledger.db.execute("SELECT count(*) FROM messages WHERE direction='in' AND pushed=0").fetchone()[0] == 0
    finally:
        ledger.close()
