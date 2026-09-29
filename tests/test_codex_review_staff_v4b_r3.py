"""Review regression for b8ec89b: malformed ps output must not prove an old group empty."""

import asyncio
import contextlib
import os
import subprocess
import sys

from mutmuas import node as node_mod
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, proc_start
from mutmuas.protocol import Envelope, request_body


async def test_malformed_ps_output_does_not_retry_while_old_child_runs(tmp_path, monkeypatch):
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    request = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-malformed-ps",
                       body=request_body("test task", "test", kind="query"))
    ledger.ingest(request)
    ledger.create_owned_task(request)
    ledger.update_task(request.task_id, "owner", status="RUNNING")
    assert ledger.claim_task(request.task_id, "worker", ("RUNNING",)) is None
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()

    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    leader = subprocess.Popen([sys.executable, "-c",
                               "import pathlib, subprocess, sys, time; "
                               "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
                               f"pathlib.Path({str(ready)!r}).write_text('ready'); "
                               f"p = pathlib.Path({str(release)!r})\nwhile not p.exists(): time.sleep(0.01)"],
                              start_new_session=True)
    try:
        for _ in range(200):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(request.task_id, leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)  # the child is still running in the old group

        # One ps row names this process, satisfying the current completeness check,
        # but has an extra field. The target group is omitted: this output is malformed,
        # not evidence that the target group is empty.
        bogus = f"{os.getpid()} {os.getpgrp()} S EXTRA\n"
        monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs:
                            subprocess.CompletedProcess(args[0], 0, bogus, ""))
        state = node_mod.group_state(leader.pid)[0]
        await daemon.recover()
        task = ledger.task(request.task_id, "owner")
        assert (state, task["status"], request.task_id in daemon._queued["B:desk"]) == (
            "unknown", "FAILED", False)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(leader.pid, 9)
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)
        ledger.close()
