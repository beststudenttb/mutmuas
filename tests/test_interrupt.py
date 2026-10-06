"""D-089: the leader's word (or priority=high) interrupts a worker in the middle of a run: the node stops the run and
lays the task out again with the message, which the next run (the same conversation for a brain) is given first.
Pause and resume stop and restart a worker's task and travel down to its child tasks."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
import yaml
from conftest import auto_worker_node, owned_task

from mutmuas import node as node_module
from mutmuas import tools
from mutmuas.protocol import Envelope, request_body
from mutmuas.runtime import RunOutcome, worker_prompt


class Blocking:
    """A runtime whose run never returns until cancelled; records each run's context."""
    runs: list = []
    started = None

    def __init__(self, *_):
        pass

    async def run(self, ctx):
        Blocking.runs.append(ctx)
        Blocking.started.set()
        await asyncio.Event().wait()
        return RunOutcome(0, "")


@pytest.fixture
def blocking(monkeypatch):
    Blocking.runs, Blocking.started = [], asyncio.Event()
    monkeypatch.setattr(node_module, "make_runtime", Blocking)
    return Blocking


async def _running(daemon, agent, task_id):
    """Queue the task and start the post's runner; return once the run is under way."""
    daemon._enqueue("B:desk", task_id)
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    await asyncio.wait_for(Blocking.started.wait(), 5)
    Blocking.started.clear()
    return runner


def _node(tmp_path):
    """B:desk whose node trusts B:secretary (the leader's word is relayed by the secretary) to interrupt."""
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    cfg.trusted_controllers = ["B:secretary"]
    return agent, cfg, ledger, hub, daemon


def _update(task_id, sender="A:sender", priority="normal", **body):
    return Envelope(type="UPDATE", sender=sender, to="B:desk", task_id=task_id, priority=priority,
                    body={"message": "stop using the old environment; use env v2", **body})


async def _deliver(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    await daemon._handle(agent, env)


async def _settle(cond, timeout=5):
    for _ in range(int(timeout * 50)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached")


async def test_the_leaders_word_interrupts_a_running_worker_and_is_given_to_the_next_run(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary", leader=True))
        await asyncio.wait_for(blocking.started.wait(), 5)             # stopped, laid out again, running again
        first, second = blocking.runs
        assert "stop using the old environment" not in worker_prompt(first)
        assert "interrupted" in worker_prompt(second) and "stop using the old environment" in worker_prompt(second)
        notes = [e.body.get("message", "") for e in ledger.outbox() if e.task_id == "T-r" and e.type == "UPDATE"]
        assert any("interrupted" in n for n in notes)                      # the requester is told
        assert ledger.task("T-r", "owner")["interrupts"] in (None, "[]", [])  # handed over to the run
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_ordinary_mail_does_not_interrupt(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r"))
        await asyncio.sleep(0.3)
        assert len(blocking.runs) == 1 and "T-r" in daemon._running
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_high_priority_with_interrupt_stops_whatever_the_post_runs(tmp_path, blocking):
    """A new instruction (not about the running task) marked interrupt: the running task is laid out again."""
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-other", sender="B:secretary", priority="high", interrupt=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert "stop using the old environment" in worker_prompt(blocking.runs[1])
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_pause_stops_a_running_worker_until_resume(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", leader=True, pause=True))
        await _settle(lambda: "T-r" not in daemon._running)
        await asyncio.sleep(0.2)
        task = ledger.task("T-r", "owner")
        assert task["status"] == "WAITING" and task["paused"] and len(blocking.runs) == 1
        await daemon._auto_dispatch()
        await daemon.recover()
        await asyncio.sleep(0.2)
        assert len(blocking.runs) == 1                                   # nothing restarts a paused task
        await _deliver(daemon, agent, _update("T-r", leader=True, resume=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert not ledger.task("T-r", "owner")["paused"]
        assert "resume" in worker_prompt(blocking.runs[1]).lower()
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_pause_and_resume_travel_down_to_child_tasks(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    child = await tools.send_request(hub, "B:desk", "C:rl", "train", "for T-p", parent_task="T-p")
    try:
        await _deliver(daemon, agent, _update("T-p", leader=True, pause=True))
        [down] = [e for e in ledger.outbox() if e.task_id == child["task_id"] and e.type == "UPDATE"]
        assert down.to == "C:rl" and down.body["pause"] is True and down.body["leader"] is True
        await _deliver(daemon, agent, _update("T-p", leader=True, resume=True))
        ups = [e for e in ledger.outbox() if e.task_id == child["task_id"] and e.body.get("resume")]
        assert len(ups) == 1
    finally:
        ledger.close()


async def test_only_the_requester_or_the_leader_can_pause(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        await _deliver(daemon, agent, _update("T-r", sender="C:stranger", pause=True))   # not leader, not requester
        await asyncio.sleep(0.3)
        assert "T-r" in daemon._running and not ledger.task("T-r", "owner")["paused"]
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_a_restart_leaves_a_paused_task_paused(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1)
    try:
        await daemon.recover()
        assert ledger.task("T-r", "owner")["status"] == "WAITING" and "T-r" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_an_interrupted_brain_run_is_resumed_in_the_same_conversation(tmp_path, blocking):
    """Sandbox finding: a brain's session id came only from the JSON a run prints when it ends, so a run stopped
    midway left none and the next run started afresh. The node now names a new conversation itself
    (--session-id) and records it before the run starts."""
    from mutmuas.runtime import ClaudeCodeRuntime
    agent, cfg, ledger, hub, daemon = _node(tmp_path)
    agent.runtime = "claude-code"
    owned_task(ledger, "T-b", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-b")
    try:
        first = blocking.runs[0]
        assert first.resume is None and first.session_id
        argv, _ = ClaudeCodeRuntime(agent, cfg).command(first)
        assert argv[argv.index("--session-id") + 1] == first.session_id
        assert ledger.brain_session("B:desk", None) == first.session_id         # known before the run ends
        await _deliver(daemon, agent, _update("T-b", sender="B:secretary", leader=True))
        await asyncio.wait_for(blocking.started.wait(), 5)
        second = blocking.runs[1]
        assert second.resume == first.session_id
        argv, _ = ClaudeCodeRuntime(agent, cfg).command(second)
        assert "--session-id" not in argv and argv[argv.index("--resume") + 1] == first.session_id
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


# --------------------------------------------------------------------------- Codex review of 9f39ff0


async def test_stopping_a_run_stops_its_whole_process_group_and_frees_its_lock(tmp_path):
    """A child in the run's process group that ignores SIGTERM and holds a (GPU-like) flock is killed too."""
    import fcntl
    import os
    import sys
    from mutmuas.runtime import _kill_group
    lock = tmp_path / "gpu0.lock"
    code = ("import os,signal,time,fcntl,sys\n"
            "fd=os.open(sys.argv[1], os.O_RDWR|os.O_CREAT, 0o600)\nfcntl.flock(fd, fcntl.LOCK_EX)\n"
            "os.set_inheritable(fd,True)\npid=os.fork()\n"
            "if pid==0:\n signal.signal(signal.SIGTERM,signal.SIG_IGN)\n os.close(1)\n while True: time.sleep(1)\n"
            "else:\n print(pid,flush=True)\n while True: time.sleep(1)\n")
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", code, str(lock),
                                                stdout=asyncio.subprocess.PIPE, start_new_session=True)
    child = int(await proc.stdout.readline())
    try:
        await _kill_group(proc, grace_s=0.5)
        with pytest.raises(ProcessLookupError):
            os.kill(child, 0)
        fd = os.open(lock, os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)            # free again
        os.close(fd)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, 9)


async def test_a_resume_while_the_paused_run_is_still_stopping_keeps_the_task_going(tmp_path, monkeypatch):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    started, cleaning, release, runs = asyncio.Event(), asyncio.Event(), asyncio.Event(), []

    class SlowStop:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            runs.append(ctx)
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleaning.set()
                await release.wait()
                raise
    monkeypatch.setattr(node_module, "make_runtime", SlowStop)
    daemon._enqueue("B:desk", "T-r")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        await asyncio.wait_for(started.wait(), 3)
        started.clear()
        await _deliver(daemon, agent, _update("T-r", pause=True))
        await asyncio.wait_for(cleaning.wait(), 3)
        await _deliver(daemon, agent, _update("T-r", resume=True))
        release.set()
        await asyncio.wait_for(started.wait(), 3)                       # run again
        assert len(runs) == 2 and not ledger.task("T-r", "owner")["paused"]
    finally:
        release.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_after_a_restart_pause_stops_the_old_worker_still_running(tmp_path):
    import subprocess
    import sys
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
    try:
        daemon._record_worker("T-r", proc.pid)
        await daemon.recover()                                           # the old worker is left running
        await _deliver(daemon, agent, _update("T-r", pause=True))
        assert proc.wait(10) is not None
        task = ledger.task("T-r", "owner")
        assert task["paused"] and task["status"] == "WAITING"
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        ledger.close()


async def test_an_interrupt_before_the_runtime_starts_still_lays_the_task_out_again(tmp_path, monkeypatch):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    entered, original = asyncio.Event(), hub.owner_transition

    async def stuck_transition(task_id, state, *args, **kwargs):
        if task_id == "T-r" and state == "RUNNING" and not entered.is_set():
            entered.set()
            await asyncio.Event().wait()
        return await original(task_id, state, *args, **kwargs)
    monkeypatch.setattr(hub, "owner_transition", stuck_transition)
    daemon._enqueue("B:desk", "T-r")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary", leader=True))
        # laid out again, and the next run (attempt 1 again: the stopped one does not count) closes it
        await _settle(lambda: ledger.task("T-r", "owner")["status"] in ("COMPLETED", "FAILED"))
        assert ledger.task("T-r", "owner")["attempts"] == 1
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_a_job_ending_while_paused_leaves_the_task_paused(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1)
    done = tmp_path / "train.done"
    done.write_text("exit 0")
    ledger.add_job("T-r", "B:desk", None, None, str(done), None, "training")
    try:
        await daemon._check_jobs()
        task = ledger.task("T-r", "owner")
        assert task["status"] == "WAITING" and task["paused"] and "T-r" not in daemon._queued["B:desk"]
        assert ledger.jobs("T-r") == [] and any("training" in n for n in task["interrupts"])
        await _deliver(daemon, agent, _update("T-r", resume=True))
        assert "T-r" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_held_child_is_paused_and_not_sent_until_resumed(tmp_path):
    from mutmuas.protocol import result_body
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-p", "RUNNING", ingest=True)
    dep = await tools.send_request(hub, "B:desk", "C:far", "train", "dep", parent_task="T-x")
    held = await tools.send_request(hub, "B:desk", "C:rl", "evaluate", "after", parent_task="T-p",
                                    depends_on=[dep["task_id"]])
    try:
        await _deliver(daemon, agent, _update("T-p", pause=True))
        await daemon._on_reply(Envelope(type="RESULT", sender="C:far", to="B:desk", task_id=dep["task_id"],
                                        body=result_body("complete", "trained")))
        await daemon._release_held()
        assert not [e for e in ledger.outbox() if e.task_id == held["task_id"] and e.type == "REQUEST"]
        await _deliver(daemon, agent, _update("T-p", resume=True))
        await daemon._release_held()
        assert [e for e in ledger.outbox() if e.task_id == held["task_id"] and e.type == "REQUEST"]
    finally:
        ledger.close()


async def test_only_trusted_addresses_interrupt_and_priority_is_no_permission(tmp_path, blocking):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runner = await _running(daemon, agent, "T-r")
    try:
        for env in (_update("T-r", sender="C:stranger", priority="high", leader=True),   # claims, not trusted
                    _update("T-r", priority="high", leader=True),                         # even its requester
                    _update("T-other", sender="C:stranger", priority="high", interrupt=True)):
            await _deliver(daemon, agent, env)
        await asyncio.sleep(0.3)
        assert len(blocking.runs) == 1 and "T-r" in daemon._running
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary"))             # trusted
        await asyncio.wait_for(blocking.started.wait(), 5)
        assert len(blocking.runs) == 2
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


async def test_a_requester_pauses_only_its_own_task_sent_to_this_address(tmp_path):
    agent, cfg, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)                 # A:sender -> B:desk
    try:
        stray = Envelope(type="UPDATE", sender="A:sender", to="B:other", task_id="T-r",
                         body={"message": "x", "pause": True})          # not the address the task belongs to
        assert daemon._interrupt_kind(stray) is None
        assert daemon._interrupt_kind(_update("T-r", sender="C:stranger", pause=True)) is None
        assert daemon._interrupt_kind(_update("T-r", pause=True)) == "pause"
    finally:
        ledger.close()


async def test_a_restart_stops_the_old_worker_of_a_paused_task(tmp_path):
    import subprocess
    import sys
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
    try:
        daemon._record_worker("T-r", proc.pid)          # paused, but its worker outlived the old node
        await daemon.recover()
        assert proc.wait(10) is not None
        task = ledger.task("T-r", "owner")
        assert task["paused"] and task["status"] == "WAITING"
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        ledger.close()


# --------------------------------------------------------------------------- Codex re-review of cb77a33


async def test_a_resume_while_the_pause_is_being_published_still_lays_the_task_out(tmp_path, monkeypatch):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    started, publishing, release, runs = asyncio.Event(), asyncio.Event(), asyncio.Event(), []

    class Blocking:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            runs.append(ctx)
            started.set()
            await asyncio.Event().wait()
    original = hub.try_publish

    async def slow_publish(env):            # the WAITING notice of the pause takes a while to go out
        if env.task_id == "T-r" and env.type == "UPDATE" and env.body.get("state") == "WAITING":
            publishing.set()
            await release.wait()
        return await original(env)
    monkeypatch.setattr(hub, "try_publish", slow_publish)
    monkeypatch.setattr(node_module, "make_runtime", Blocking)
    daemon._enqueue("B:desk", "T-r")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        await asyncio.wait_for(started.wait(), 3)
        started.clear()
        await _deliver(daemon, agent, _update("T-r", pause=True))
        await asyncio.wait_for(publishing.wait(), 3)
        await _deliver(daemon, agent, _update("T-r", resume=True))
        release.set()
        await asyncio.wait_for(started.wait(), 3)                    # it runs again, once
        await asyncio.sleep(0.2)
        assert len(runs) == 2 and not ledger.task("T-r", "owner")["paused"]
    finally:
        release.set()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        ledger.close()


@pytest.fixture
def old_worker(monkeypatch):
    """A worker from before a restart that still runs (synthetic pid identity); stop_group records, and answers
    `stops` (True: stopped)."""
    stopped, gone = [], set()
    state = {"stops": True}

    async def fake_stop(pgid, grace_s=5.0):
        stopped.append(pgid)
        if state["stops"]:
            gone.add(pgid)
        return state["stops"]
    monkeypatch.setattr(node_module, "same_process", lambda pid, start: bool(pid) and pid not in gone)
    monkeypatch.setattr(node_module, "stop_group", fake_stop)
    return stopped, state


async def test_a_trusted_update_reaches_an_old_worker_after_a_restart(tmp_path, old_worker):
    stopped, _ = old_worker
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    try:
        await daemon.recover()
        assert "T-r" in daemon._recheck
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary"))       # about the task it runs
        assert stopped == [12345] and "T-r" in daemon._queued["B:desk"]
        assert ledger.task("T-r", "owner")["interrupts"]
    finally:
        ledger.close()


async def test_a_trusted_interrupt_of_the_post_reaches_its_old_workers(tmp_path, old_worker):
    stopped, _ = old_worker
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    try:
        await daemon.recover()
        await _deliver(daemon, agent, _update("T-other", sender="B:secretary", interrupt=True))
        assert stopped == [12345] and "T-r" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_stop_that_fails_is_not_reported_as_done(tmp_path, old_worker):
    stopped, state = old_worker
    state["stops"] = False
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    try:
        await daemon.recover()
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary"))
        assert stopped == [12345] and "T-r" not in daemon._queued["B:desk"]      # no second run next to it
        await _deliver(daemon, agent, _update("T-r", pause=True))
        assert stopped == [12345, 12345]                                          # tried again
        task = ledger.task("T-r", "owner")
        assert task["paused"] and task["status"] == "RUNNING" and "T-r" in daemon._recheck
        assert any("could not stop" in f["error"] for f in ledger.failures())
    finally:
        ledger.close()


async def test_a_group_answering_eperm_is_gone_only_if_all_its_processes_are_zombies(monkeypatch):
    from mutmuas import runtime

    def eperm(pgid, sig):
        raise PermissionError(1, "Operation not permitted")
    monkeypatch.setattr(runtime.os, "killpg", eperm)
    monkeypatch.setattr(runtime, "_group_states", lambda pgid: ["Z", "S"])
    assert await runtime.stop_group(4242, grace_s=0.1) is False
    monkeypatch.setattr(runtime, "_group_states", lambda pgid: ["Z"])
    assert await runtime.stop_group(4242, grace_s=0.1) is True


@pytest.mark.parametrize("value", ["B:secretary", None, ["B"], ["B:secretary", 3]])
def test_trusted_controllers_must_be_a_list_of_full_addresses(tmp_path, value):
    from mutmuas.config import ConfigError, load_config
    path = tmp_path / "node.yaml"
    path.write_text(yaml.safe_dump({"project": "p", "node": "B", "trusted_controllers": value,
                                    "agents": [{"id": "desk", "mode": "interactive"}]}))
    with pytest.raises(ConfigError, match="trusted_controllers"):
        load_config(path)


async def test_a_group_whose_states_cannot_be_read_counts_as_not_stopped(monkeypatch):
    """EPERM, and /bin/ps cannot run (a sandbox): the group cannot be shown gone, so the stop failed."""
    from mutmuas import runtime

    def eperm(pgid, sig):
        raise PermissionError(1, "Operation not permitted")

    def no_ps(*args, **kwargs):
        raise PermissionError(1, "ps is not allowed here")
    monkeypatch.setattr(runtime.os, "killpg", eperm)
    monkeypatch.setattr(runtime.subprocess, "run", no_ps)
    assert await runtime.stop_group(4242, grace_s=0.1) is False


async def test_a_failing_ps_does_not_make_a_group_look_gone(monkeypatch):
    import subprocess
    from mutmuas import runtime

    def eperm(pgid, sig):
        raise PermissionError(1, "Operation not permitted")
    monkeypatch.setattr(runtime.os, "killpg", eperm)
    monkeypatch.setattr(runtime.subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a, 1, stdout="", stderr="ps: not allowed"))
    assert await runtime.stop_group(4242, grace_s=0.1) is False


async def test_a_group_of_zombies_is_gone_where_kill_succeeds_on_them(monkeypatch):
    """Linux: kill() on a zombie succeeds (macOS answers EPERM), so a stopped group whose members are not reaped
    yet still answers killpg (secretary's run on B, 752de2e)."""
    from mutmuas import runtime
    sent = []
    monkeypatch.setattr(runtime.os, "killpg", lambda pgid, sig: sent.append(sig))
    monkeypatch.setattr(runtime, "_group_states", lambda pgid: ["Z"])
    assert await runtime.stop_group(4242, grace_s=1) is True
    monkeypatch.setattr(runtime, "_group_states", lambda pgid: ["Z", "S"])
    assert await runtime.stop_group(4242, grace_s=0.1) is False


def test_group_states_reads_this_systems_ps():
    import os
    import subprocess
    import sys
    import time
    from mutmuas import runtime
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    try:
        assert runtime._group_states(child.pid) and not runtime._group_states(child.pid)[0].startswith("Z")
        os.kill(child.pid, 9)                # dead, not reaped yet (no wait): a zombie, on macOS and Linux
        for _ in range(100):
            if all(s.startswith("Z") for s in runtime._group_states(child.pid)):
                break
            time.sleep(0.02)
        assert runtime._group_states(child.pid) and all(s.startswith("Z") for s in runtime._group_states(child.pid))
    finally:
        child.kill()
        child.wait()


# --------------------------------------------------------------------------- Codex third review (7a109df)


async def test_a_group_left_by_a_failed_stop_is_watched_after_its_leader_ends(tmp_path, monkeypatch):
    live = {"leader": True, "group": True}

    async def failed_stop(pgid, grace_s=5.0):
        return False
    monkeypatch.setattr(node_module, "same_process", lambda pid, start: bool(pid) and live["leader"])
    monkeypatch.setattr(node_module, "group_alive", lambda pgid: live["group"])
    monkeypatch.setattr(node_module, "stop_group", failed_stop)
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    try:
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary", interrupt=True))
        assert "T-r" in daemon._recheck and ledger.task("T-r", "owner")["stuck_pgid"] == 12345
        live["leader"] = False                                   # the leader ends, a child in its group lives on
        await daemon._recover_auto(ledger.task("T-r", "owner"))
        assert "T-r" in daemon._recheck and "T-r" not in daemon._queued["B:desk"]
        live["group"] = False                                    # now the whole group is gone
        await daemon._recover_auto(ledger.task("T-r", "owner"))
        assert "T-r" not in daemon._recheck and "T-r" in daemon._queued["B:desk"]
        assert not ledger.task("T-r", "owner")["stuck_pgid"]
    finally:
        ledger.close()


async def test_a_resume_while_the_heartbeat_stops_a_paused_old_worker_lays_it_out(tmp_path, monkeypatch):
    live = {"leader": True}
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_stop(pgid, grace_s=5.0):
        entered.set()
        await release.wait()
        live["leader"] = False
        return True
    monkeypatch.setattr(node_module, "same_process", lambda pid, start: bool(pid) and live["leader"])
    monkeypatch.setattr(node_module, "stop_group", slow_stop)
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    ledger.update_task("T-r", "owner", paused=1)
    daemon._recheck.add("T-r")
    runs = []

    class Record:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            runs.append(ctx)
            await asyncio.Event().wait()
    monkeypatch.setattr(node_module, "make_runtime", Record)
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    heartbeat = asyncio.create_task(daemon._recover_auto(ledger.task("T-r", "owner")))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await _deliver(daemon, agent, _update("T-r", resume=True))
        await asyncio.sleep(0.1)          # the runner takes it and skips it: the old worker still runs
        release.set()
        await heartbeat
        await _settle(lambda: len(runs) == 1)                     # run once the old worker is gone
        assert not ledger.task("T-r", "owner")["paused"]
    finally:
        release.set()
        heartbeat.cancel()
        runner.cancel()
        await asyncio.gather(heartbeat, runner, return_exceptions=True)
        ledger.close()


async def test_a_running_task_whose_stop_fails_is_not_run_again_beside_it(tmp_path, monkeypatch):
    import sys
    from mutmuas import runtime
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    started, runs, procs = asyncio.Event(), [], []

    async def could_not_stop(pgid, grace_s=5.0):
        return False

    class Live:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            runs.append(ctx)
            proc = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(60)",
                                                        start_new_session=True)
            procs.append(proc)
            ctx.on_spawn(proc.pid)
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await runtime._kill_group(proc, grace_s=0.01)      # the real path, its stop failing
                raise
    monkeypatch.setattr(runtime, "stop_group", could_not_stop)
    monkeypatch.setattr(node_module, "make_runtime", Live)
    daemon._enqueue("B:desk", "T-r")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        await asyncio.wait_for(started.wait(), 3)
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary"))
        await asyncio.sleep(0.5)
        assert len(runs) == 1 and "T-r" in daemon._recheck                      # not run beside the live group
        assert ledger.task("T-r", "owner")["stuck_pgid"] == procs[0].pid
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        for p in procs:
            p.kill()
            await p.wait()
        ledger.close()


@pytest.mark.parametrize("stdout", ["", "4242\n", "4242 Z extra\n", "4242 Z\nnonsense\n"])
def test_a_ps_output_that_says_nothing_clear_is_not_a_stopped_group(monkeypatch, stdout):
    import subprocess
    from mutmuas import runtime
    monkeypatch.setattr(runtime.os, "killpg", lambda pgid, sig: None)          # the group answers
    monkeypatch.setattr(runtime.subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=stdout, stderr=""))
    assert runtime._group_gone(4242, 0) is False


async def test_a_command_that_exits_without_reading_its_stdin_is_not_a_runtime_error(tmp_path):
    """C's Linux runs: a script that does not read the task JSON (`true`) closed its stdin before the node had
    written it; the write raised ConnectionResetError and the run counted as failed. Made certain here with an
    input larger than a pipe holds."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.runtime import TaskContext, make_runtime
    agent = AgentConfig(id="desk", mode="worker", runtime="script", command=["sh", "-c", "exit 0"],
                        workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"))
    request = Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-1",
                       body=request_body("x" * 2_000_000, "a big task"))
    outcome = await make_runtime(agent, cfg).run(TaskContext("T-1", request, agent, cfg))
    assert outcome.exit_code == 0


# --------------------------------------------------------------------------- Codex fourth review (a41498e)


async def test_a_run_that_fails_with_an_io_error_and_cannot_be_stopped_is_watched(tmp_path, monkeypatch):
    import sys
    from mutmuas import runtime
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    runs, procs = [], []

    async def could_not_stop(pgid, grace_s=5.0):
        return False

    class Breaks:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            runs.append(ctx)
            proc = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(60)",
                                                        start_new_session=True)
            procs.append(proc)
            ctx.on_spawn(proc.pid)
            try:
                raise OSError("synthetic: reading the run's output failed")
            except BaseException:
                await runtime._kill_group(proc, grace_s=0.01)       # the runtime's own cleanup, failing
                raise
    monkeypatch.setattr(runtime, "stop_group", could_not_stop)
    monkeypatch.setattr(node_module, "make_runtime", Breaks)
    daemon._enqueue("B:desk", "T-r")
    runner = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        await _settle(lambda: "T-r" in daemon._recheck)
        await asyncio.sleep(0.3)
        assert len(runs) == 1 and ledger.task("T-r", "owner")["stuck_pgid"] == procs[0].pid
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        for p in procs:
            p.kill()
            await p.wait()
        ledger.close()


async def test_an_interrupt_of_a_paused_task_stops_its_group_and_leaves_it_paused(tmp_path, monkeypatch):
    live = {"group": True}

    async def stopped(pgid, grace_s=5.0):
        live["group"] = False
        return True
    monkeypatch.setattr(node_module, "same_process", lambda *_: False)
    monkeypatch.setattr(node_module, "group_alive", lambda pgid: live["group"])
    monkeypatch.setattr(node_module, "stop_group", stopped)
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1, stuck_pgid=12345)
    try:
        await _deliver(daemon, agent, _update("T-r", sender="B:secretary"))
        task = ledger.task("T-r", "owner")
        assert not live["group"] and task["paused"] and task["status"] == "WAITING"
        assert "T-r" not in daemon._queued["B:desk"] and task["interrupts"]          # kept for after resume
    finally:
        ledger.close()


async def test_a_runner_cancelled_as_its_run_ends_stops_instead_of_waiting_forever(tmp_path, monkeypatch):
    """C's Linux runs (hangdiag): the runner's own cancel (a daemon stop, a test's cleanup) landing as the run it
    awaited ended was swallowed - `runner.done()` was already true - and the runner went back to its empty queue
    for good. Made certain here: the run cancels the runner on its last step."""
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "ACCEPTED", ingest=True)
    outer = {}

    async def run_that_ends_as_the_runner_is_cancelled(agent_, task_id):
        outer["runner"].cancel()                        # the stop arrives now; this run ends normally anyway
    monkeypatch.setattr(daemon, "_execute", run_that_ends_as_the_runner_is_cancelled)
    daemon._enqueue("B:desk", "T-r")
    outer["runner"] = asyncio.create_task(daemon._runner(agent, "B:desk", daemon._queues["B:desk"]))
    try:
        done, _ = await asyncio.wait({outer["runner"]}, timeout=3)
        assert done and outer["runner"].cancelled()
    finally:
        outer["runner"].cancel()
        await asyncio.gather(outer["runner"], return_exceptions=True)
        ledger.close()
