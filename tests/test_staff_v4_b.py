"""Staff v4, option B (Codex review of f8c105e): no adoption. At a restart a surviving worker is stopped (TERM,
then KILL), then its draft is delivered or the task is queued again. Tests 1 and 2 are Codex's, unchanged; 3-5
are Codex's rewritten for option B (the originals are kept in tests/reviews/ for comparison)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import pytest

from mutmuas import node as node_mod
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, proc_start, same_process
from mutmuas.protocol import Envelope, request_body


def _setup(tmp_path, status, **agent_extra):
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script",
                        command=["true"], workdir=str(tmp_path / "work"), **agent_extra)
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    request = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-b",
                       body=request_body("test task", "test", kind="query"))
    ledger.ingest(request)
    ledger.create_owned_task(request)
    ledger.update_task("T-b", "owner", status=status)
    assert ledger.claim_task("T-b", "worker", (status,)) is None
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    return agent, ledger, daemon


def _worker(code="import time; time.sleep(30)"):
    return subprocess.Popen([sys.executable, "-c", code], start_new_session=True)


# 1 (Codex, unchanged)
def test_unknown_start_time_never_authenticates_a_worker():
    """A missing start timestamp (migration or read failure) must not make a reused PID authoritative."""
    assert not same_process(os.getpid(), None)


# 2 (Codex, unchanged apart from the shared setup)
async def test_recovery_delivers_a_draft_from_a_worker_that_already_exited(tmp_path):
    """A completed draft must win over retry even if its process died while the daemon was offline."""
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = _worker()
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    ledger.update_task("T-b", "owner", result_draft={"status": "complete", "summary": "worker finished"})
    worker.terminate()
    worker.wait(5)
    try:
        await daemon.recover()
        task = ledger.task("T-b", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "worker finished"
        assert "T-b" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


# 3 (rewritten: was "cancel stops an adopted worker"; with no adoption, the restart itself stops the worker,
#    so no process outlives the restart for a CANCEL to miss)
async def test_restart_stops_a_surviving_worker_and_queues_the_task_again(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = _worker()
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    try:
        await daemon.recover()
        assert worker.poll() is not None, "the worker from before the restart still runs"
        task = ledger.task("T-b", "owner")
        assert task["status"] == "ACCEPTED" and task["runner_pid"] is None
        assert "T-b" in daemon._queued["B:desk"] and not daemon._background
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


# 4 (rewritten: was "adopted worker that ignores TERM is stopped at the deadline"; now it is stopped at the
#    restart, with KILL after the grace period)
async def test_restart_kills_a_worker_that_ignores_term(tmp_path, monkeypatch):
    monkeypatch.setattr(node_mod, "STOP_GRACE_S", 0.3)
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    ready = tmp_path / "ready"
    worker = _worker("import pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                     f"pathlib.Path({str(ready)!r}).write_text('ready'); time.sleep(30)")
    try:
        for _ in range(100):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
        await asyncio.wait_for(daemon.recover(), timeout=5)
        worker.wait(5)
        assert worker.returncode == -9                                   # killed, not terminated
        assert "T-b" in daemon._queued["B:desk"]
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


# 5 (rewritten: was "recover adopts a live worker that reported WAITING"; now it stops it and queues the task)
async def test_recover_stops_a_live_worker_that_reported_waiting(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "WAITING")
    worker = _worker()
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    try:
        await daemon.recover()
        assert worker.poll() is not None
        assert "T-b" in daemon._queued["B:desk"]
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


# ours: a worker that cannot be stopped fails the task with the reason, instead of running twice
async def test_a_worker_that_cannot_be_stopped_fails_the_task(tmp_path, monkeypatch):
    monkeypatch.setattr(node_mod, "STOP_GRACE_S", 0.1)
    monkeypatch.setattr(node_mod.os, "killpg", lambda pid, sig: None)   # signals have no effect
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = _worker()
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    try:
        await daemon.recover()
        task = ledger.task("T-b", "owner")
        assert task["status"] == "FAILED" and "could not stop" in task["result"]["summary"]
        assert "T-b" not in daemon._queued["B:desk"]
    finally:
        worker.kill()
        worker.wait(5)
        ledger.close()


# ours (item 1): a worker whose start time cannot be read is stopped and the attempt fails
async def test_spawn_without_a_start_time_stops_the_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(node_mod, "proc_start", lambda pid: None)
    agent, ledger, daemon = _setup(tmp_path, "ACCEPTED")
    agent.command = [sys.executable, "-c", "import time; time.sleep(30)"]
    try:
        await asyncio.wait_for(daemon._execute(agent, "T-b"), timeout=10)
        task = ledger.task("T-b", "owner")
        assert task["status"] == "FAILED" and "start time" in task["result"]["summary"]
    finally:
        ledger.close()


# ours (Codex review of e6a9df9): the whole process group is stopped, children included, before a retry
async def test_restart_stops_the_whole_process_group_before_the_retry(tmp_path, monkeypatch):
    """The child ignores TERM and outlives its leader: only KILL for the whole group stops it."""
    monkeypatch.setattr(node_mod, "STOP_GRACE_S", 0.3)
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    child_pid = tmp_path / "child.pid"
    leader = _worker("import pathlib, subprocess, sys, time; "
                     "c = subprocess.Popen([sys.executable, '-c', 'import signal, time; "
                     "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)']); "
                     f"pathlib.Path({str(child_pid)!r}).write_text(str(c.pid)); time.sleep(30)")
    try:
        for _ in range(200):
            if child_pid.exists() and child_pid.read_text():
                break
            await asyncio.sleep(0.02)
        child = int(child_pid.read_text())
        ledger.set_runner_pid("T-b", leader.pid, proc_start(leader.pid))
        await daemon.recover()
        leader.wait(5)
        assert not node_mod.group_members(leader.pid), "a process of the old worker's group still runs"
        assert not same_process(child, proc_start(child) or "gone")
        assert "T-b" in daemon._queued["B:desk"]
    finally:
        with __import__("contextlib").suppress(ProcessLookupError):
            os.killpg(leader.pid, 9)
        if leader.poll() is None:
            leader.kill()
        leader.wait(5)
        ledger.close()


# ours (restores the ba28e70 migration edge): a record without a start time never proves a worker
async def test_a_record_without_start_time_does_not_admit_a_worker(tmp_path):
    from mutmuas.node import lease_refusal
    agent, ledger, _ = _setup(tmp_path, "RUNNING")
    ledger.set_runner_pid("T-b", os.getpid(), None)           # a record from before start times were kept
    session = _worker()
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
        assert lease_refusal(ledger, "B:desk") is not None       # this process is not proven to be the worker
    finally:
        session.kill()
        session.wait(5)
        ledger.close()


# ours: an old worker pid without a start time that is not a group leader is quarantined too
async def test_unknown_start_time_quarantines_a_live_process_that_leads_no_group(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])   # not a group leader
    ledger.set_runner_pid("T-b", worker.pid, None)
    try:
        await daemon.recover()
        assert worker.poll() is None                                       # not signalled
        assert "T-b" not in daemon._queued["B:desk"]
        assert ledger.task("T-b", "owner")["status"] == "FAILED"
    finally:
        worker.kill()
        worker.wait(5)
        ledger.close()


# ours (Codex review of 0a9f031, D-034): the group query has three answers; "unknown" never allows a retry
def _leader_gone_child_running(tmp_path):
    ready, release = tmp_path / "child-ready", tmp_path / "release-parent"
    leader = _worker("import pathlib, subprocess, sys, time; "
                     "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
                     f"pathlib.Path({str(ready)!r}).write_text('ready'); "
                     f"p = pathlib.Path({str(release)!r})\nwhile not p.exists(): time.sleep(0.01)")
    return leader, ready, release


async def _run_with_group_child(tmp_path, monkeypatch, ps):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    leader, ready, release = _leader_gone_child_running(tmp_path)
    try:
        for _ in range(200):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        ledger.set_runner_pid("T-b", leader.pid, proc_start(leader.pid))
        release.write_text("go")
        leader.wait(5)
        os.killpg(leader.pid, 0)                                         # the child still runs in the group
        monkeypatch.setattr(node_mod, "_PS", ps)
        await daemon.recover()
        task = ledger.task("T-b", "owner")
        assert "T-b" not in daemon._queued["B:desk"], "retried although the group could not be checked"
        assert task["status"] == "FAILED" and "could not be checked" in task["result"]["summary"]
        os.killpg(leader.pid, 0)                                         # quarantined, not signalled
    finally:
        with __import__("contextlib").suppress(ProcessLookupError):
            os.killpg(leader.pid, 9)
        ledger.close()


async def test_failing_ps_is_unknown_not_empty(tmp_path, monkeypatch):
    """A ps that lists processes but exits non-zero: the list may be partial, so it proves nothing."""
    fake = tmp_path / "ps-fails"
    fake.write_text(f"#!/bin/sh\n{node_mod._PS} \"$@\"\nexit 1\n")
    fake.chmod(0o755)
    await _run_with_group_child(tmp_path, monkeypatch, str(fake))


async def test_ps_with_no_output_is_unknown_not_empty(tmp_path, monkeypatch):
    await _run_with_group_child(tmp_path, monkeypatch, "/usr/bin/true")


async def test_no_ps_at_all_is_unknown_not_empty(tmp_path, monkeypatch):
    await _run_with_group_child(tmp_path, monkeypatch, None)


def test_group_state_answers_members_empty_or_unknown(monkeypatch):
    assert node_mod.group_state(os.getpgid(0))[0] == "members"        # this test's own group
    assert node_mod.group_state(2 ** 22 + 12345) == ("empty", [])     # no such group
    monkeypatch.setattr(node_mod, "_PS", "/usr/bin/false")
    assert node_mod.group_state(os.getpgid(0))[0] == "unknown"


# ours (non-blocking item, choice written down): a start time that differs proves the pid now belongs to
# another process, so the task is run again; only an unreadable start time counts as unknown
async def test_a_reused_pid_with_another_start_time_does_not_block_the_retry(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])   # not a group leader
    ledger.set_runner_pid("T-b", other.pid, "a start time of the old worker")
    try:
        await daemon.recover()
        assert other.poll() is None and "T-b" in daemon._queued["B:desk"]
    finally:
        other.kill()
        other.wait(5)
        ledger.close()


async def test_an_unreadable_start_time_of_a_live_pid_quarantines(tmp_path, monkeypatch):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    ledger.set_runner_pid("T-b", other.pid, "a start time of the old worker")
    monkeypatch.setattr(node_mod, "proc_start", lambda pid: None)
    try:
        await daemon.recover()
        assert other.poll() is None and "T-b" not in daemon._queued["B:desk"]
        assert ledger.task("T-b", "owner")["status"] == "FAILED"
    finally:
        other.kill()
        other.wait(5)
        ledger.close()
