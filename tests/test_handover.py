"""D-104 item 2: a deploy (the daemon stopping and a new one starting) does not stop the runs in progress. They
run to their end on the code they started with, their output in their own log (not a pipe to the old daemon);
the new daemon takes only new work, adopts the runs it finds still going, and delivers each one's result when it
ends: the draft it submitted, or the result it printed."""

from __future__ import annotations

import asyncio
import os
import sys

from conftest import owned_task
from test_job_wake import _node

from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, same_process


def _new_daemon(cfg):
    """The daemon a deploy starts: same node, same ledger, nothing in memory from the old one."""
    daemon = NodeDaemon(cfg)
    daemon.hub = Hub(cfg, None, Ledger(cfg.db_path))
    for agent in cfg.agents:
        daemon._queues[f"{cfg.node}:{agent.id}"] = asyncio.PriorityQueue()
        daemon._queued[f"{cfg.node}:{agent.id}"] = set()
    return daemon


async def _until(cond, timeout=10):
    for _ in range(int(timeout * 20)):
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not reached")


async def test_a_run_outlives_a_deploy_and_the_new_daemon_delivers_its_printed_result(tmp_path):
    agent, ledger, daemon = _node(tmp_path)
    release = tmp_path / "release"
    agent.command = [sys.executable, "-c",
                     "import json, os, time\n"
                     f"while not os.path.exists({str(release)!r}): time.sleep(0.05)\n"
                     "print(json.dumps({'status': 'complete', 'summary': 'finished across the deploy'}))"]
    owned_task(ledger, "T-h", "ACCEPTED", ingest=True)
    daemon._enqueue("B:desk", "T-h")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    await _until(lambda: (ledger.task("T-h", "owner") or {}).get("runner_pid"))
    task = ledger.task("T-h", "owner")
    pid, start = task["runner_pid"], task["runner_start"]
    # the deploy: the old daemon stops ...
    daemon._stopping = True
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)
    assert same_process(pid, start)                            # the run goes on
    new = _new_daemon(daemon.cfg)
    try:
        await new.recover()                                    # ... the new one adopts it, runs nothing beside it
        assert "T-h" in new._recheck and new.hub.ledger.task("T-h", "owner")["status"] not in ("COMPLETED",)
        release.touch()                                        # the run ends, on the old daemon's watch no more
        await _until(lambda: not same_process(pid, start))
        for task_id in list(new._recheck):                     # what the heartbeat does
            await new._recover_auto(new.hub.ledger.task(task_id, "owner"))
        task = new.hub.ledger.task("T-h", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "finished across the deploy"
        assert task["attempts"] == 1 and "T-h" not in new._queued["B:desk"]          # it was not run again
    finally:
        new.hub.ledger.close()
        ledger.close()
        if same_process(pid, start):
            os.kill(pid, 9)


async def test_a_runs_input_is_a_file_written_before_it_starts(tmp_path):
    """B:ops (F6): the prompt went through a pipe the daemon wrote while the run started; a deploy in the middle
    left the run with half of it. The run now reads a file the daemon wrote in full before starting it."""
    agent, ledger, daemon = _node(tmp_path)
    agent.command = [sys.executable, "-c",
                     "import json, os, stat, sys\n"
                     "regular = stat.S_ISREG(os.fstat(0).st_mode)\n"
                     "task = json.load(sys.stdin)\n"
                     "print(json.dumps({'status': 'complete', 'summary': f\"{regular} {task['task_id']}\"}))"]
    owned_task(ledger, "T-in", "ACCEPTED", ingest=True)
    try:
        await daemon._execute(agent, "T-in")
        assert ledger.task("T-in", "owner")["result"]["summary"] == "True T-in"
        assert not list((daemon.cfg.data_path / "runs").glob("T-in.*.input"))      # gone with its run's end
    finally:
        ledger.close()
