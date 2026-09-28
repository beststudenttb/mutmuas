"""Regression checks for fail-closed worker identity and adopted-task cancellation."""

import asyncio
import os
import subprocess
import sys

import pytest

from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, proc_start, same_process
from mutmuas.protocol import Envelope, request_body


def test_unknown_start_time_never_authenticates_a_worker():
    """A missing start timestamp (migration or read failure) must not make a reused PID authoritative."""
    assert not same_process(os.getpid(), None)


@pytest.mark.asyncio
async def test_cancel_stops_an_adopted_worker(tmp_path):
    """Cancellation must terminate a surviving worker, just as it terminates an ordinary running task."""
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    task_id = "T-adopt-cancel"
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                                      body=request_body("test task", "test", kind="query")))
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        assert daemon._background and worker.poll() is None
        await daemon._on_cancel(Envelope(type="CANCEL", sender="A:sender", to="B:desk", task_id=task_id,
                                         body={"reason": "withdrawn"}))
        await asyncio.sleep(0.2)
        assert worker.poll() is not None, "adopted worker continues after cancellation"
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        for job in list(daemon._background):
            job.cancel()
        await asyncio.gather(*daemon._background, return_exceptions=True)
        ledger.close()


@pytest.mark.asyncio
async def test_recovery_delivers_a_draft_from_a_worker_that_already_exited(tmp_path):
    """A completed draft must win over retry even if its process died while the daemon was offline."""
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    task_id = "T-adopt-draft"
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                                      body=request_body("test task", "test", kind="query")))
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
    ledger.update_task(task_id, "owner", result_draft={"status": "complete", "summary": "worker finished"})
    worker.terminate()
    worker.wait(5)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        task = ledger.task(task_id, "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "worker finished"
        assert task_id not in daemon._queued["B:desk"]
    finally:
        ledger.close()


@pytest.mark.asyncio
async def test_adopted_worker_that_ignores_term_is_stopped_at_the_deadline(tmp_path):
    """The deadline must eventually stop a TERM-resistant worker instead of leaving the task stuck."""
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"), task_timeout_s=0.1)
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    task_id = "T-adopt-timeout"
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                                      body=request_body("test task", "test", kind="query")))
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    ready = tmp_path / "ready"
    code = ("import pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"pathlib.Path({str(ready)!r}).write_text('ready'); time.sleep(30)")
    worker = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    try:
        for _ in range(100):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists()
        ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
        daemon = NodeDaemon(cfg)
        daemon.hub = hub
        daemon._queues["B:desk"] = asyncio.Queue()
        daemon._queued["B:desk"] = set()
        await asyncio.wait_for(daemon._adopt(agent, ledger.task(task_id, "owner")), timeout=2.5)
        assert worker.poll() is not None
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


@pytest.mark.asyncio
async def test_recover_adopts_a_live_worker_that_reported_waiting(tmp_path):
    """WAITING is nonterminal; restart must not forget its still-running worker."""
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    task_id = "T-adopt-waiting"
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                                      body=request_body("test task", "test", kind="query")))
    ledger.update_task(task_id, "owner", status="WAITING")
    assert ledger.claim_task(task_id, "worker", ("WAITING",)) is None
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        assert daemon._background or task_id in daemon._queued["B:desk"]
    finally:
        worker.kill()
        worker.wait(5)
        for job in list(daemon._background):
            job.cancel()
        await asyncio.gather(*daemon._background, return_exceptions=True)
        ledger.close()
