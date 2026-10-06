"""B:ops review of exp/slim (a)(b)(d) (T-20261006102234-7fa1afc4): a shell's `inbox --peek` does not count as
showing the mail, retire waits for a stuck process group, MUTMUAS_TASK_ID is checked against the ledger, a bad
observer is refused before anything goes out; an interrupt reaches a WAITING task's live worker and starts it
afresh, a paused PENDING task stays paused after a restart, and a time from now is at least a second."""

from __future__ import annotations

import os

import pytest
from conftest import Orphan, auto_worker_node, owned_task
from test_interrupt import _deliver, _node, _update, old_worker  # noqa: F401  (old_worker: a fixture)
from test_job_wake import _node as _plain_node
from test_retire import node  # noqa: F401  (a fixture)

from mutmuas import cli, tools
from mutmuas import runtime as runtime_module
from mutmuas.node import lease_refusal, proc_start, worker_task
from mutmuas.protocol import Envelope, request_body
from mutmuas.retire import retire


async def test_a_shells_inbox_peek_does_not_let_clear_inbox_drop_the_mail(tmp_path, capsys):
    _, ledger, daemon = _plain_node(tmp_path, mode="interactive")
    env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-p", body=request_body("look", "test"))
    ledger.ingest(env)
    ledger.mark_handled(env.message_id)
    try:
        args = cli.agentctl_parser().parse_args(["inbox", "--peek", "--as", "B:desk"])
        await cli.cmd_inbox(args, daemon.hub)
        assert "T-p" in capsys.readouterr().out
        out = await tools.clear_inbox(daemon.hub, "B:desk", 10**9)
        assert out["marked_read"] == 0 and "left_unread" in out              # the session never saw it
    finally:
        ledger.close()


async def test_retire_waits_for_a_stuck_process_group_of_a_finished_task(node, monkeypatch):  # noqa: F811
    path, cfg, ledger, post = node
    ledger.update_task("T-open", "owner", status="CANCELLED", stuck_pgid=12345)
    monkeypatch.setattr(runtime_module, "group_alive", lambda pgid: pgid == 12345)
    with pytest.raises(PermissionError, match="T-open"):
        await retire(path, "vision")
    assert "id: vision" in path.read_text() and post.is_dir()


def test_mutmuas_task_id_makes_a_worker_only_of_this_agents_running_task(tmp_path, monkeypatch):
    _, _, ledger, _, _ = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-w", "RUNNING", claim="worker")
        monkeypatch.setenv("MUTMUAS_TASK_ID", "T-w")
        assert worker_task(ledger, "B:desk") is None                         # no run recorded
        ledger.set_runner_pid("T-w", os.getpid(), proc_start(os.getpid()))
        assert worker_task(ledger, "B:other") is None                        # not that agent's task
        assert worker_task(ledger, "B:desk") == "T-w"
        monkeypatch.setenv("MUTMUAS_TASK_ID", "T-none")
        assert worker_task(ledger, "B:desk") is None                         # no such task
    finally:
        ledger.close()


def test_a_shell_with_a_stale_task_id_does_not_get_past_the_sessions_lease(tmp_path, monkeypatch):
    agent, _, ledger, _, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-old", "RUNNING", claim="worker")
    ledger.set_runner_pid("T-old", 999999, "gone")                          # its worker has ended
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-old")
    session = Orphan("import time; time.sleep(30)")
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
        assert lease_refusal(ledger, "B:desk") is not None
    finally:
        session.kill()
        ledger.close()


async def test_a_bad_observer_is_refused_before_anything_goes_out(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    try:
        with pytest.raises(ValueError):
            await tools.send_request(hub, "B:desk", "C:far", "eval", "test", observers=["C:ok", "not an address"])
        assert ledger.outbox() == [] and ledger.tasks(role="requester", limit=None) == []
    finally:
        ledger.close()


async def test_an_interrupt_of_the_post_reaches_the_live_worker_of_a_waiting_task(tmp_path, old_worker):  # noqa: F811
    stopped, _ = old_worker
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "WAITING", claim="worker", ingest=True)        # it reported WAITING, still runs
    ledger.set_runner_pid("T-r", 12345, "synthetic-start")
    ledger.update_task("T-r", "owner", attempts=2)
    try:
        await _deliver(daemon, agent, _update("T-other", sender="B:secretary", interrupt=True))
        assert stopped == [12345] and "T-r" in daemon._queued["B:desk"]
        assert ledger.task("T-r", "owner")["attempts"] == 0                  # the next run starts afresh
    finally:
        ledger.close()


async def test_a_paused_pending_task_stays_paused_after_a_restart(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    owned_task(ledger, "T-r", "PENDING", claim="worker", ingest=True)
    ledger.update_task("T-r", "owner", paused=1)
    try:
        await daemon._recover_auto(ledger.task("T-r", "owner"))
        task = ledger.task("T-r", "owner")
        assert task["status"] == "WAITING" and task["paused"] and "T-r" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


@pytest.mark.parametrize("text", ["+0.001s", "+0s", "+0.5s", "-1m", "10m"])
def test_a_time_from_now_is_at_least_a_second(text):
    with pytest.raises(ValueError, match="at least a second"):
        tools.from_now(text, "deadline")
    assert tools.from_now("+1s", "deadline")


# --------------------------------------------------------------------------- re-review of 9b979ac (R1): a mode: worker
# run is never claimed 'worker', yet the daemon records its process all the same (B:ops probes)


async def test_the_daemon_records_the_process_of_an_unclaimed_run(tmp_path):
    _, _, ledger, _, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-own", "RUNNING")
    try:
        ledger.set_runner_pid("T-own", os.getpid(), proc_start(os.getpid()))
        assert ledger.task("T-own", "owner")["runner_pid"] == os.getpid()
    finally:
        ledger.close()


async def test_a_plain_worker_may_not_close_another_task_of_its_agent(tmp_path, monkeypatch):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-mine", "RUNNING")
    owned_task(ledger, "T-other", "RUNNING")
    ledger.set_runner_pid("T-mine", os.getpid(), proc_start(os.getpid()))   # what _record_worker does
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-mine")
    try:
        with pytest.raises(PermissionError):
            await tools.submit_result(hub, "B:desk", "complete", "not mine", task_id="T-other")
    finally:
        ledger.close()


async def test_retire_waits_for_a_running_plain_worker(node):  # noqa: F811
    path, cfg, ledger, post = node
    run = Envelope(type="REQUEST", sender="C:lead", to="C:plain", task_id="T-run", body=request_body("run", "test"))
    ledger.ingest(run)
    ledger.create_owned_task(run)
    ledger.update_task("T-run", "owner", status="RUNNING")
    ledger.set_runner_pid("T-run", os.getpid(), proc_start(os.getpid()))   # mode: worker: no runner claim
    with pytest.raises(PermissionError, match="worker is running"):
        await retire(path, "plain")


# --------------------------------------------------------------------------- review of 2016ae0 (E1): a worker task that
# registered a job and then reported BLOCKED is laid out again when the job ends or it is named next (B:ops probes)


async def test_job_end_wakes_a_blocked_worker_task(tmp_path):
    _, ledger, daemon = _plain_node(tmp_path)
    owned_task(ledger, "T-j", "RUNNING", ingest=True)
    job = Orphan("import time; time.sleep(60)")
    await tools.add_job(daemon.hub, "B:desk", "T-j", pid=job.pid, note="training")
    await daemon.hub.owner_transition("T-j", "BLOCKED", "also needs a dataset")   # what report_progress(BLOCKED) does
    assert ledger.jobs("T-j")
    try:
        job.terminate()
        job.wait(5)
        await daemon._check_jobs()
        assert ledger.jobs("T-j") == []
        task = ledger.task("T-j", "owner")
        assert "T-j" in daemon._queued["B:desk"], task["status"]
    finally:
        ledger.close()


async def test_named_next_wakes_a_blocked_worker_task_with_a_job(tmp_path):
    """_wake_task ends the jobs, hands over to _wake_for_job, and that goes to _settle while still BLOCKED."""
    _, ledger, daemon = _plain_node(tmp_path)
    owned_task(ledger, "T-j", "RUNNING", ingest=True)
    job = Orphan("import time; time.sleep(60)")
    await tools.add_job(daemon.hub, "B:desk", "T-j", pid=job.pid, note="training")
    await daemon.hub.owner_transition("T-j", "BLOCKED", "needs an answer")
    try:
        await daemon._wake_task(ledger.task("T-j", "owner"), "named next")
        assert ledger.jobs("T-j") == []                 # the wait was ended ...
        assert "T-j" in daemon._queued["B:desk"], ledger.task("T-j", "owner")["status"]   # ... so it must run
    finally:
        job.terminate()
        ledger.close()


async def test_a_mode_worker_run_records_its_process(tmp_path):
    """A real mode: worker run through _execute: the process the daemon started is the one in runner_pid."""
    import sys
    agent, ledger, daemon = _plain_node(tmp_path)
    mark = tmp_path / "pid"
    agent.command = [sys.executable, "-c", f"import os; open({str(mark)!r}, 'w').write(str(os.getpid()))"]
    owned_task(ledger, "T-x", "ACCEPTED", ingest=True)
    try:
        await daemon._execute(agent, "T-x")
        task = ledger.task("T-x", "owner")
        assert task["runner"] is None and task["runner_pid"] == int(mark.read_text())
    finally:
        ledger.close()
