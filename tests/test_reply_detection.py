"""D-104 item 3: a new letter is checked against the work its sender holds. To someone with exactly one open task
of theirs in the sender's hands, it is the delivery of that task (its RESULT, the requester named next, the task
closed); with several, the sender must say which (reply_to); with none, or reply_to="new", it is a new request."""

from __future__ import annotations

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools


def _sent(ledger, type_):
    return [e for e in ledger.outbox() if e.type == type_]


async def test_a_letter_to_the_requester_of_my_only_open_task_delivers_it(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    try:
        out = await tools.send_request(hub, "B:desk", "A:sender", "the analysis is done: see the report", "deliver",
                                       artifacts=[{"uri": "artifact://p/B/desk/T-1/r.md"}])
        assert out["delivered_as_result_of"] == "T-1"
        task = ledger.task("T-1", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "the analysis is done: see the report"
        [result] = _sent(ledger, "RESULT")
        assert result.body["next"] == "A:sender" and result.artifacts[0].uri.endswith("r.md")
        assert _sent(ledger, "REQUEST") == []
    finally:
        ledger.close()


async def test_with_several_open_tasks_of_the_recipient_the_sender_must_say_which(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    for task_id in ("T-1", "T-2"):
        owned_task(ledger, task_id, "RUNNING", claim="session", ingest=True)
    try:
        with pytest.raises(ValueError, match="T-1.*T-2|T-2.*T-1"):
            await tools.send_request(hub, "B:desk", "A:sender", "done", "deliver")
        assert _sent(ledger, "REQUEST") == [] and _sent(ledger, "RESULT") == []
        out = await tools.send_request(hub, "B:desk", "A:sender", "done", "deliver", reply_to="T-2")
        assert out["delivered_as_result_of"] == "T-2" and ledger.task("T-1", "owner")["status"] == "RUNNING"
    finally:
        ledger.close()


async def test_reply_to_new_or_no_open_task_sends_a_new_request(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    try:
        out = await tools.send_request(hub, "B:desk", "A:sender", "a new question", "new", reply_to="new")
        assert "task_id" in out and ledger.task("T-1", "owner")["status"] == "RUNNING"
        await tools.send_request(hub, "B:desk", "C:other", "unrelated", "nothing of theirs here")
        assert len(_sent(ledger, "REQUEST")) == 2 and _sent(ledger, "RESULT") == []
        with pytest.raises(ValueError, match="T-9"):
            await tools.send_request(hub, "B:desk", "A:sender", "x", "y", reply_to="T-9")
    finally:
        ledger.close()


async def test_a_child_request_is_never_a_delivery(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    try:
        await tools.send_request(hub, "B:desk", "A:sender", "part of T-1", "split", parent_task="T-1")
        assert len(_sent(ledger, "REQUEST")) == 1 and ledger.task("T-1", "owner")["status"] == "RUNNING"
    finally:
        ledger.close()
