"""Option-B restart regressions: do not run twice or strand a task."""


import pytest
from conftest import auto_worker_node, owned_task



def _daemon(tmp_path, status="RUNNING"):
    _, _, ledger, _, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-review-b", status, claim="worker")
    return ledger, daemon, "T-review-b"


@pytest.mark.asyncio
async def test_recover_resolves_pending_task_still_claimed_by_worker(tmp_path):
    """A crash between the PENDING transition and release_task must not strand the task."""
    ledger, daemon, task_id = _daemon(tmp_path, status="PENDING")
    try:
        await daemon.recover()
        await daemon._auto_dispatch()
        assert task_id in daemon._queued["B:desk"]
    finally:
        ledger.close()
