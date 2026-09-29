"""Regression checks for auto-worker ownership and recovery."""

import asyncio

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.node import NodeDaemon


def _auto_worker(tmp_path):
    _, cfg, ledger, hub, _ = auto_worker_node(tmp_path)
    return cfg, ledger, hub


@pytest.mark.asyncio
async def test_recover_requeues_accepted_task_not_yet_claimed_by_worker(tmp_path):
    """A crash between ACK persistence and enqueue must not strand an ACCEPTED task."""
    cfg, ledger, hub = _auto_worker(tmp_path)
    task_id = "T-review-recover"
    owned_task(ledger, task_id)
    ledger.update_task(task_id, "owner", status="ACCEPTED")  # _accept finished; no claim/enqueue yet
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        await daemon._auto_dispatch()
        assert task_id in daemon._queued["B:desk"]
    finally:
        ledger.close()


@pytest.mark.asyncio
async def test_session_cannot_reject_a_running_worker_task(tmp_path):
    """A later session must not close a task already held by the worker."""
    _, ledger, hub = _auto_worker(tmp_path)
    task_id = "T-review-running"
    owned_task(ledger, task_id)
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    try:
        with pytest.raises(PermissionError, match="worker"):
            await tools.reject_task(hub, "B:desk", task_id, "session declines")
        assert ledger.task(task_id, "owner")["status"] == "RUNNING"
    finally:
        ledger.close()


@pytest.mark.asyncio
async def test_worker_may_not_accept_another_task_as_the_session(tmp_path, monkeypatch):
    """A daemon-run worker's MCP server must not claim another task on the same address for the session."""
    _, ledger, hub = _auto_worker(tmp_path)
    task_id = "T-review-other"
    owned_task(ledger, task_id)
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-review-worker")
    try:
        with pytest.raises(PermissionError, match="worker|session"):
            await tools.accept_task(hub, "B:desk", task_id)
        assert ledger.task(task_id, "owner")["runner"] is None
    finally:
        ledger.close()


@pytest.mark.asyncio
async def test_worker_may_not_deliver_the_sessions_other_task(tmp_path, monkeypatch):
    """The worker for task A must not finish task B after the session claimed B."""
    _, ledger, hub = _auto_worker(tmp_path)
    task_id = "T-review-session-task"
    owned_task(ledger, task_id)
    assert ledger.claim_task(task_id, "session", ("PENDING",)) is None
    ledger.update_task(task_id, "owner", status="RUNNING")
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-review-worker")
    try:
        with pytest.raises(PermissionError, match="session"):
            await tools.submit_result(hub, "B:desk", "complete", "wrong worker delivered",
                                      task_id=task_id)
        assert ledger.task(task_id, "owner")["status"] == "RUNNING"
    finally:
        ledger.close()
