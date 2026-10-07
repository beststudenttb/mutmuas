"""D-109 (F1): a new letter is never taken for a delivery. A delivery is a delivery receipt naming its task
(submit_result / the delivery template); send_request always sends a new request, whatever its sender holds."""

from __future__ import annotations

import os

from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.node import proc_start


async def test_a_worker_delegating_to_its_own_requester_sends_a_child_request(tmp_path, monkeypatch):
    """Worker prompt rule 8: 'Parts you delegate with send_request are this task's child tasks (parent_task is set
    for you)'. A worker of T-w (asked by A:sender) delegates a part to A:sender: that must be a child REQUEST."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-w", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-w", os.getpid(), proc_start(os.getpid()))
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-w")
    try:
        out = await tools.send_request(hub, "B:desk", "A:sender", "please run the Linux e2e for me", "sub-part")
        assert "delivered_as_result_of" not in out, out
        assert ledger.task("T-w", "owner").get("result_draft") is None
    finally:
        ledger.close()


async def test_a_session_sending_new_work_to_whoever_asked_it_something_sends_a_request(tmp_path):
    """B:desk's session holds one open task from A:sender (a review) and sends A:sender a new piece of work."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    try:
        out = await tools.send_request(hub, "B:desk", "A:sender", "fix F1b in node.py", "new work", kind="code")
        assert "delivered_as_result_of" not in out, out
        assert ledger.task("T-1", "owner")["status"] == "RUNNING"
    finally:
        ledger.close()
