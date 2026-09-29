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


async def test_wait_result_decides_on_the_same_snapshot_it_returns(make_config, cluster):
    """717d7e0 on B: the view was built while the local row was still open (the owner's record won, without the
    result); the RESULT was handled right after; the closed-here check then read the ledger again, saw it closed,
    and returned the stale view without result_status. The check must use the snapshot the view came from."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("w", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    task_view = hub_a._task_view

    async def view_then_result_arrives(task_id):
        view = await task_view(task_id)
        if view and view.get("status") == "COMPLETED" and not view.get("result_status"):
            for _ in range(200):                     # the stale view is returned only after the RESULT is handled
                local = hub_a.ledger.task(task_id, "requester")
                if local and local["status"] == "COMPLETED":
                    break
                await asyncio.sleep(0.05)
        return view

    hub_a._task_view = view_then_result_arrives
    daemon_a = cluster.daemons["A"]
    on_reply = daemon_a._on_reply

    async def slow_reply(env):
        if env.type == "RESULT":
            await asyncio.sleep(1)                   # the owner's record is seen before the RESULT
        return await on_reply(env)

    daemon_a._on_reply = slow_reply
    sent = await tools.send_request(hub_a, "A:main", "B:w", "echo", "snapshot",
                                    inputs={"action": "echo", "text": "hi"})
    result = await tools.wait_for_result(hub_a, sent["task_id"], 30)
    assert result.get("result_status") == "complete", result


async def test_the_requesters_own_result_is_kept_when_the_owners_record_is_newer(tmp_path, monkeypatch):
    """The owner's record may carry a later updated_at than the requester's row (it is republished); it has no
    result, so the result the requester's ledger holds must still be in the view."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.hub import Hub
    from mutmuas.ledger import Ledger
    from mutmuas.protocol import Envelope, request_body, result_body
    cfg = NodeConfig(project="p", node="A", data_dir=str(tmp_path / "data"),
                     agents=[AgentConfig(id="main", mode="interactive")])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    request = Envelope(type="REQUEST", sender="A:main", to="B:w", task_id="T-newer",
                       body=request_body("x", "y"))
    ledger.queue_outgoing(request)
    ledger.update_task("T-newer", "requester", status="COMPLETED", result=result_body("complete", "done"),
                       result_status="complete")
    hub.bus = object()

    async def remote(task_id, owner):
        return {"task_id": task_id, "status": "COMPLETED", "requester": "A:main", "owner": "B:w",
                "updated_at": "2999-01-01T00:00:00.000+00:00"}

    monkeypatch.setattr(hub, "_remote_task", remote)
    try:
        view = await hub.task_view("T-newer", "A:main")
        assert view["result_status"] == "complete" and view["result"]["summary"] == "done"
    finally:
        ledger.close()
