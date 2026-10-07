"""D-104 items 4+5: two levels, priority normal and high (the leader's work counts as high). The owner's node
decides with what it is doing: urgent work that arrives while the post runs something not urgent stops that run,
which is laid out again after it (noted, not counted as a failed attempt); urgent behind urgent waits its turn;
work that is not urgent waits for the run in hand."""

from __future__ import annotations

import asyncio
import sqlite3

from conftest import owned_task
import test_interrupt
from test_interrupt import _node, _running

from mutmuas.ledger import Ledger
from mutmuas.protocol import Envelope, request_body
from mutmuas.runtime import worker_prompt


blocking = test_interrupt.blocking          # the fixture: a runtime whose runs last until stopped


async def _arrive(daemon, agent, task_id, priority="normal"):
    env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id, priority=priority,
                   body=request_body(f"work {task_id}", "test"))
    daemon.hub.ledger.ingest(env)
    await daemon._handle(agent, env)


async def test_urgent_work_stops_a_run_that_is_not_urgent_which_comes_back_after_it(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-n", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-n")
    try:
        await _arrive(daemon, agent, "T-u", priority="high")
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert [ctx.task_id for ctx in blocking.runs] == ["T-n", "T-u"]
        task = ledger.task("T-n", "owner")
        assert task["attempts"] == 0 and "T-n" in daemon._queued["B:desk"]       # back in the queue, not failed
        blocking.started.clear()
        daemon._running["T-u"].cancel()                                          # the urgent run ends
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert blocking.runs[-1].task_id == "T-n" and "T-u" in worker_prompt(blocking.runs[-1])
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_urgent_behind_urgent_waits_its_turn(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    await _arrive(daemon, agent, "T-u1", priority="high")
    runner = await _running(daemon, agent, "T-u1")
    try:
        await _arrive(daemon, agent, "T-u2", priority="high")
        await asyncio.sleep(0.3)
        assert [ctx.task_id for ctx in blocking.runs] == ["T-u1"] and "T-u2" in daemon._queued["B:desk"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_work_that_is_not_urgent_waits_for_the_run_in_hand(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-a", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-a")
    try:
        await _arrive(daemon, agent, "T-b")
        await asyncio.sleep(0.3)
        assert [ctx.task_id for ctx in blocking.runs] == ["T-a"] and "T-b" in daemon._queued["B:desk"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


def test_an_existing_ledger_gains_the_priority_column(tmp_path):
    """Columns are only ever added (D-104 item 2: old and new code share a ledger during a deploy)."""
    path = tmp_path / "ledger.sqlite3"
    Ledger(path).close()
    db = sqlite3.connect(path)
    db.execute("ALTER TABLE tasks DROP COLUMN priority")
    db.close()
    ledger = Ledger(path)
    try:
        assert "priority" in [r["name"] for r in ledger.db.execute("PRAGMA table_info(tasks)")]
    finally:
        ledger.close()
