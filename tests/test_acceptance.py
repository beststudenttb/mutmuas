"""D-109: a delivery waits for the requester to accept it. The requesting agent itself accepts (pass), sends it back
with a reason (reject: the owner carries on), or, for a partial or failed result, closes it as failed: such a result
never counts as done. Before that the owner may withdraw it. A child's delivery wakes the parent that asked for it,
and the parent's wait on its children ends only with their acceptance. Requests from scripts (agentctl ask) are
accepted on delivery, as before."""

from __future__ import annotations

import asyncio

import pytest
from conftest import auto_worker_node

from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon


def _requester_node(tmp_path):
    """A:lead on its own node: an auto_worker post (a brain without a session), no bus."""
    agent = AgentConfig(id="lead", mode="interactive", auto_worker=True, runtime="script", command=["true"],
                        workdir=str(tmp_path / "A" / "work"))
    cfg = NodeConfig(project="p", node="A", data_dir=str(tmp_path / "A" / "data"), agents=[agent]).validate()
    ledger = Ledger(cfg.db_path)
    daemon = NodeDaemon(cfg)
    daemon.hub = Hub(cfg, None, ledger)
    daemon._queues["A:lead"], daemon._queued["A:lead"] = asyncio.PriorityQueue(), set()
    return agent, ledger, daemon


async def _carry(src_ledger, daemon, agent, type_):
    """Deliver the last message of this type from one node's outbox to the other's daemon."""
    env = [e for e in src_ledger.outbox() if e.type == type_][-1]
    daemon.hub.ledger.ingest(env)
    return env, await daemon._handle(agent, env)


async def _manual_task(tmp_path, parent=None):
    """A:lead asks B:desk with manual acceptance; B has the request. Returns everything the tests use."""
    b_agent, _, b_ledger, b_hub, b_daemon = auto_worker_node(tmp_path)
    a_agent, a_ledger, a_daemon = _requester_node(tmp_path)
    if parent:                                       # A:lead's own task, which the request is a part of
        from mutmuas.protocol import Envelope, request_body
        req = Envelope(type="REQUEST", sender="C:boss", to="A:lead", task_id=parent, body=request_body("x", "y"))
        a_ledger.create_owned_task(req)
        a_ledger.update_task(parent, "owner", status="RUNNING")
    sent = await tools.send_request(a_daemon.hub, "A:lead", "B:desk", "train it", "for the paper",
                                    acceptance="manual", parent_task=parent)
    req, _ = await _carry(a_ledger, b_daemon, b_agent, "REQUEST")
    assert req.body["acceptance"] == "manual"
    return sent["task_id"], (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon)


async def test_a_delivery_waits_for_the_requester_and_a_pass_completes_it(tmp_path):
    task_id, (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon) = await _manual_task(tmp_path)
    try:
        out = await tools.submit_result(b_daemon.hub, "B:desk", "complete", "trained", task_id=task_id)
        assert b_ledger.task(task_id, "owner")["status"] == "DELIVERED" and out["delivered"]
        result, _ = await _carry(b_ledger, a_daemon, a_agent, "RESULT")
        assert result.body["acceptance"] == "pending" and result.body["next"] == "A:lead"
        assert a_ledger.task(task_id, "requester")["status"] == "DELIVERED"
        view = await a_daemon.hub.wait_result(task_id, timeout=1, viewer="A:lead")
        assert not view.get("timed_out_waiting")                            # the result is there to look at
        with pytest.raises(ValueError):
            await tools.submit_result(b_daemon.hub, "B:desk", "complete", "again", task_id=task_id)
        await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "pass")
        assert a_ledger.task(task_id, "requester")["status"] == "COMPLETED"
        verdict, _ = await _carry(a_ledger, b_daemon, b_agent, "UPDATE")
        assert verdict.body["acceptance"] == "pass"                          # on the task's record
        assert b_ledger.task(task_id, "owner")["status"] == "COMPLETED"
    finally:
        a_ledger.close()
        b_ledger.close()


async def test_a_rejected_delivery_goes_back_to_its_owner_with_the_reason(tmp_path):
    task_id, (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon) = await _manual_task(tmp_path)
    try:
        await tools.submit_result(b_daemon.hub, "B:desk", "complete", "trained", task_id=task_id)
        await _carry(b_ledger, a_daemon, a_agent, "RESULT")
        with pytest.raises(ValueError, match="reason"):
            await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "reject")
        await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "reject", reason="no seed recorded")
        assert a_ledger.task(task_id, "requester")["status"] == "ACCEPTED"
        verdict, _ = await _carry(a_ledger, b_daemon, b_agent, "UPDATE")
        assert verdict.body["next"] == "B:desk"
        task = b_ledger.task(task_id, "owner")
        assert task["status"] == "ACCEPTED" and any("no seed recorded" in n for n in task["interrupts"])
        assert task_id in b_daemon._queued["B:desk"]                       # the worker carries on
    finally:
        a_ledger.close()
        b_ledger.close()


async def test_a_partial_result_can_be_closed_as_failed_but_never_passed(tmp_path):
    task_id, (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon) = await _manual_task(tmp_path)
    try:
        await tools.submit_result(b_daemon.hub, "B:desk", "partial", "half of it", task_id=task_id)
        await _carry(b_ledger, a_daemon, a_agent, "RESULT")
        with pytest.raises(ValueError, match="complete"):
            await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "pass")
        await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "close", reason="enough for now")
        assert a_ledger.task(task_id, "requester")["status"] == "FAILED"
        await _carry(a_ledger, b_daemon, b_agent, "UPDATE")
        assert b_ledger.task(task_id, "owner")["status"] == "FAILED"
    finally:
        a_ledger.close()
        b_ledger.close()


async def test_the_owner_may_withdraw_a_delivery_before_it_is_accepted(tmp_path):
    task_id, (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon) = await _manual_task(tmp_path)
    try:
        await tools.submit_result(b_daemon.hub, "B:desk", "complete", "trained", task_id=task_id)
        await _carry(b_ledger, a_daemon, a_agent, "RESULT")
        await tools.withdraw_delivery(b_daemon.hub, "B:desk", task_id, "the wrong checkpoint")
        assert b_ledger.task(task_id, "owner")["status"] == "RUNNING"
        await _carry(b_ledger, a_daemon, a_agent, "UPDATE")
        assert a_ledger.task(task_id, "requester")["status"] == "RUNNING"
        with pytest.raises(ValueError):
            await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "pass")     # nothing to accept now
    finally:
        a_ledger.close()
        b_ledger.close()


async def test_a_childs_delivery_wakes_its_parent_whose_wait_ends_only_with_acceptance(tmp_path):
    task_id, (a_agent, a_ledger, a_daemon), (b_agent, b_ledger, b_daemon) = await _manual_task(tmp_path,
                                                                                              parent="T-p")
    try:
        await tools.add_job(a_daemon.hub, "A:lead", "T-p", children=True, note="the training")
        await tools.submit_result(b_daemon.hub, "B:desk", "complete", "trained", task_id=task_id)
        await _carry(b_ledger, a_daemon, a_agent, "RESULT")
        assert "T-p" in a_daemon._queued["A:lead"]                         # woken to accept it
        await tools.add_job(a_daemon.hub, "A:lead", "T-p", children=True, note="the training")
        await a_daemon._check_jobs()
        assert a_ledger.jobs("T-p")                                         # delivered, not accepted: still waits
        await tools.accept_delivery(a_daemon.hub, "A:lead", task_id, "pass")
        await a_daemon._check_jobs()
        assert a_ledger.jobs("T-p") == []
    finally:
        a_ledger.close()
        b_ledger.close()


async def test_requests_are_accepted_on_delivery_unless_they_ask_for_acceptance(tmp_path):
    """Scripts (agentctl ask) and code calling the tools: as before. Agents through MCP ask for acceptance."""
    from mutmuas import cli
    b_agent, _, b_ledger, b_hub, b_daemon = auto_worker_node(tmp_path)
    try:
        sent = (await tools.send_request(b_hub, "B:desk", "B:desk", "x", "y"))["task_id"]
        assert "acceptance" not in b_ledger.task(sent, "requester")["request"]
        seen = []

        async def fake_send(hub, me, to, objective, reason, **kw):
            seen.append(kw.get("acceptance"))
            return {"task_id": "T-x"}
        tools_send, tools.send_request = tools.send_request, fake_send
        try:
            await cli.cmd_ask(cli.agentctl_parser().parse_args(
                ["ask", "C:far", "o", "--reason", "r", "--expect", "x", "--accept", "y", "--as", "B:desk"]), None)
        finally:
            tools.send_request = tools_send
        assert seen == [None]
    finally:
        b_ledger.close()


def test_a_woken_brain_is_told_which_deliveries_wait_for_it(tmp_path):
    from test_worker_setup import _claude_ctx

    from mutmuas.runtime import worker_prompt
    _, ctx, _ = _claude_ctx(tmp_path)
    ctx.deliveries = [{"task_id": "T-c", "owner": "B:desk", "result_status": "partial",
                       "result": {"summary": "half the seeds"}}]
    prompt = worker_prompt(ctx)
    assert "T-c" in prompt and "half the seeds" in prompt and "accept_delivery" in prompt
    assert "partial" in prompt
    ctx.deliveries = []
    assert "wait for your acceptance" not in worker_prompt(ctx)


def test_the_mcp_instructions_say_deliveries_wait_for_acceptance():
    from mutmuas.mcp_server import INSTRUCTIONS
    assert "accept_delivery" in INSTRUCTIONS
