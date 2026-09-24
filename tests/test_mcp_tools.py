"""The MCP server is how Claude Code / Codex use the network. Drive it over real stdio like they do."""

import json
import sys

from conftest import interactive, worker
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


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
