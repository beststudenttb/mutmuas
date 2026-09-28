"""wait_result must not return a finished task without its result (found through the v4 grace test on B).

The owner publishes its task record to the shared KV right after sending the RESULT; the requester can see
the record (status COMPLETED, no result) before its own daemon has handled the RESULT message.
"""

from __future__ import annotations

import asyncio

from conftest import interactive, worker

from mutmuas import tools


async def test_wait_for_result_waits_for_the_result_message_not_just_the_record(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("w", "lab.py")])
    daemon_a = await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    on_reply = daemon_a._on_reply

    async def slow_reply(env):
        if env.type == "RESULT":
            await asyncio.sleep(2)             # the RESULT reaches the requester well after the KV record
        return await on_reply(env)

    daemon_a._on_reply = slow_reply
    sent = await tools.send_request(hub_a, "A:main", "B:w", "echo", "record before result",
                                    inputs={"action": "echo", "text": "hi"})
    result = await tools.wait_for_result(hub_a, sent["task_id"], 30)
    assert result.get("result_status") == "complete", result
    assert result["result"]["summary"] == "echo: hi"


async def test_wait_for_result_still_times_out_when_the_closing_message_never_comes(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("w", "lab.py")])
    daemon_a = await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    on_reply = daemon_a._on_reply

    async def lose_result(env):
        if env.type != "RESULT":
            return await on_reply(env)

    daemon_a._on_reply = lose_result
    sent = await tools.send_request(hub_a, "A:main", "B:w", "echo", "result lost",
                                    inputs={"action": "echo", "text": "hi"})
    result = await tools.wait_for_result(hub_a, sent["task_id"], 3)
    assert result.get("timed_out_waiting") and result["status"] == "COMPLETED"
