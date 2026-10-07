"""Who acts for an agent (D-030, D-032, D-102). One session holds the agent (a lease); a shell beside it only
reads. A worker the daemon started acts on its own task only, known by MUTMUAS_TASK_ID checked against the process
the daemon recorded (pid and start time). An auto_worker address runs its tasks as a worker while no session holds
it and lists them for the session while one does; a running worker is never interrupted by the session. After a
restart a worker's task gets its draft delivered or is run again, never beside an old worker that still runs."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import HANDLERS, Orphan, auto_worker_node, eventually, interactive, owned_task
from test_job_wake import _node as _plain_node

from mutmuas import cli, node as node_mod, tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, lease_refusal, proc_start, same_process, session_alive, worker_task
from mutmuas.protocol import Envelope, request_body


def auto(id: str, **extra) -> dict:
    return interactive(id, auto_worker=True, runtime="script",
                       command=["{python}", str(HANDLERS / "lab.py")], **extra)


@pytest.fixture
def holder():
    """A live process standing in for the leader's Claude Code session."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    yield proc
    proc.terminate()
    proc.wait(5)


def _session_on(hub, addr, proc):
    hub.ledger.session_beat(addr, proc.pid, str(hub.local_agent(addr)[1].workdir_path), session_pid=proc.pid)


async def _status(hub, task_id):
    view = hub.ledger.task(task_id, "owner")
    return view and view["status"]


async def _two_nodes(make_config, cluster, **extra):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [auto("desk", **extra)])
    await cluster.start(a)
    await cluster.start(b)
    return await cluster.client(a), await cluster.client(b)


async def test_without_a_session_the_daemon_runs_the_task_as_a_worker(make_config, cluster):
    hub_a, hub_b = await _two_nodes(make_config, cluster)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "echo", "no session", inputs={"action": "echo",
                                                                                           "text": "hi"})
    result = await tools.wait_for_result(hub_a, sent["task_id"], 30)
    assert result["result_status"] == "complete" and result["result"]["summary"] == "echo: hi"
    assert hub_b.ledger.task(sent["task_id"], "owner")["runner"] == "worker"


async def test_while_the_session_is_there_requests_are_listed_not_run(make_config, cluster, holder, tmp_path):
    """D-032a: the leader decides; other agents' requests are shown to him, not executed."""
    hub_a, hub_b = await _two_nodes(make_config, cluster)
    _session_on(hub_b, "B:desk", holder)
    marker = tmp_path / "ran"
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "echo", "session online",
                                    inputs={"action": "echo", "marker": str(marker)})
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner task")
    await asyncio.sleep(2)
    assert await _status(hub_b, sent["task_id"]) == "PENDING" and not marker.exists()
    assert "ACK" not in [m["type"] for m in hub_a.ledger.thread(sent["task_id"])]    # never taken for a worker
    listed = await tools.inbox(hub_b, "B:desk", peek=True)
    assert sent["task_id"] in {m["task_id"] for m in listed}


async def test_after_the_session_ends_the_worker_takes_over_after_a_grace_period(make_config, cluster, holder,
                                                                              monkeypatch, tmp_path):
    """Condition-driven, no fixed sleeps against the grace period (it flaked under load on B, 5576ad5): while
    the grace period lasts the task waits; once it is over the heartbeat hands it to the worker."""
    monkeypatch.setattr(node_mod, "AUTO_WORKER_GRACE_S", 3600.0)
    hub_a, hub_b = await _two_nodes(make_config, cluster)
    _session_on(hub_b, "B:desk", holder)
    hub_b.ledger.session_end("B:desk", holder.pid)                    # the leader just closed it
    marker = tmp_path / "ran"
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "echo", "grace",
                                    inputs={"action": "echo", "marker": str(marker)})
    await eventually(lambda: "UPDATE" in [m["type"] for m in hub_a.ledger.thread(sent["task_id"])],
                     what="delivered to the inbox (the interactive path)")
    await asyncio.sleep(1.5)                                           # three heartbeats inside the grace period
    assert await _status(hub_b, sent["task_id"]) == "PENDING" and not marker.exists()
    monkeypatch.setattr(node_mod, "AUTO_WORKER_GRACE_S", 0.0)         # the grace period is over
    result = await tools.wait_for_result(hub_a, sent["task_id"], 30)
    daemon = cluster.daemons["B"]
    state = {"owner_task": {k: v for k, v in (hub_b.ledger.task(sent["task_id"], "owner") or {}).items()
                            if k in ("status", "runner", "runner_pid", "attempts", "updated_at")},
             "session": hub_b.ledger.session_of("B:desk"), "queued": daemon._queued.get("B:desk"),
             "present": node_mod.session_present(hub_b.ledger, "B:desk"),
             "thread": [m["type"] for m in hub_a.ledger.thread(sent["task_id"])],
             "b_rows": [tuple(r) for r in hub_b.ledger.db.execute(
                 "SELECT direction, json_extract(envelope, '$.type'), state, last_error, created_at FROM messages"
                 " WHERE task_id=? ORDER BY rowid", (sent["task_id"],))],
             "a_rows": [tuple(r) for r in hub_a.ledger.db.execute(
                 "SELECT direction, json_extract(envelope, '$.type'), state, last_error, created_at FROM messages"
                 " WHERE task_id=? ORDER BY rowid", (sent["task_id"],))],
             "a_task": {k: v for k, v in (hub_a.ledger.task(sent["task_id"], "requester") or {}).items()
                        if k in ("status", "result_status", "updated_at")}}
    assert result.get("result_status") == "complete", f"not taken over after the grace period: {state}"
    assert marker.read_text().count("\n") == 1
    assert state["thread"].count("ACK") == 1


async def test_a_running_worker_is_not_interrupted_and_its_task_is_never_done_twice(make_config, cluster, holder,
                                                                                    tmp_path):
    hub_a, hub_b = await _two_nodes(make_config, cluster)
    marker, release = tmp_path / "ran", tmp_path / "release"
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "long", "worker first",
                                    inputs={"action": "wait_file", "release": str(release), "marker": str(marker)})
    task_id = sent["task_id"]
    await eventually(lambda: _status(hub_b, task_id), what="task")
    await eventually(lambda: marker.exists(), what="worker started")
    _session_on(hub_b, "B:desk", holder)                               # the leader comes in meanwhile

    running = (await tools.whoami(hub_b, "B:desk"))["worker_running"]
    assert [w["task_id"] for w in running] == [task_id] and running[0]["latest_end"]
    with pytest.raises(PermissionError, match="worker"):
        await tools.accept_task(hub_b, "B:desk", task_id)             # the session cannot take it over
    with pytest.raises(PermissionError, match="worker"):
        await tools.submit_result(hub_b, "B:desk", "complete", "done by the session", task_id=task_id)

    release.write_text("go")                                           # the worker finishes, uninterrupted
    result = await tools.wait_for_result(hub_a, task_id, 30)
    assert result["result_status"] == "complete" and result["result"]["summary"] == "waited for release"
    assert marker.read_text().count("\n") == 1
    notes = [m.get("body", {}).get("message") or "" for m in hub_a.ledger.thread(task_id)]
    assert any("released, finishing" in n for n in notes)        # its agentctl got past the session's lease


async def test_a_queued_task_waits_for_the_session_once_it_is_there(make_config, cluster, holder, tmp_path):
    """Queued behind the running worker when the leader came in: listed for him, not started (D-032a)."""
    hub_a, hub_b = await _two_nodes(make_config, cluster)
    release, first_ran, second_ran = tmp_path / "release", tmp_path / "first", tmp_path / "second"
    first = await tools.send_request(hub_a, "A:main", "B:desk", "long", "first",
                                     inputs={"action": "wait_file", "release": str(release),
                                             "marker": str(first_ran)})
    await eventually(lambda: first_ran.exists(), what="first running")
    second = await tools.send_request(hub_a, "A:main", "B:desk", "echo", "second",
                                      inputs={"action": "echo", "marker": str(second_ran)})
    await eventually(lambda: (hub_b.ledger.task(second["task_id"], "owner") or {}).get("status") == "ACCEPTED",
                     what="second accepted into the worker's queue")
    _session_on(hub_b, "B:desk", holder)
    release.write_text("go")
    await tools.wait_for_result(hub_a, first["task_id"], 30)
    await eventually(lambda: (hub_b.ledger.task(second["task_id"], "owner") or {}).get("status") == "PENDING",
                     what="second handed to the session")
    await asyncio.sleep(1)
    assert not second_ran.exists()
    assert hub_b.ledger.task(second["task_id"], "owner")["runner"] is None


def test_a_workers_mcp_process_does_not_hold_the_session(tmp_path):
    """The MCP server a worker starts must not beat as the agent's session (the daemon would think the leader
    is there and stop running tasks)."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.mcp_server import holds_session
    from mutmuas.protocol import Envelope, request_body
    from mutmuas.runtime import TaskContext, _mcp_server_spec
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path))
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="claude-code",
                        workdir=str(tmp_path))
    agent.validate()
    req = Envelope(type="REQUEST", sender="A:main", to="B:desk", task_id="T-1", body=request_body("x", "y"))
    args = _mcp_server_spec(TaskContext("T-1", req, agent, node))["args"]
    assert args[args.index("--worker-task") + 1] == "T-1"
    assert holds_session(agent, worker_task=None) and not holds_session(agent, worker_task="T-1")


def test_auto_worker_config_is_validated():
    from mutmuas.config import AgentConfig, ConfigError
    with pytest.raises(ConfigError, match="auto_worker"):
        AgentConfig(id="x", mode="worker", runtime="claude-code", auto_worker=True).validate()
    with pytest.raises(ConfigError, match="runtime"):
        AgentConfig(id="x", mode="interactive", auto_worker=True).validate()


def _setup(tmp_path, status, **agent_extra):
    agent, _, ledger, _, daemon = auto_worker_node(tmp_path, **agent_extra)
    owned_task(ledger, "T-b", status, claim="worker", ingest=True)
    return agent, ledger, daemon


def test_unknown_start_time_never_authenticates_a_worker():
    """A missing start timestamp (migration or read failure) must not make a reused PID authoritative."""
    assert not same_process(os.getpid(), None)


async def test_recovery_delivers_a_draft_from_a_worker_that_already_exited(tmp_path):
    """A completed draft must win over retry even if its process died while the daemon was offline."""
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = Orphan("import time; time.sleep(30)")
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


async def test_an_old_worker_that_still_runs_is_skipped_and_looked_at_again(tmp_path):
    """D-040: not stopped, not quarantined. This round it is skipped (a run that outlived a deploy is normal, D-104:
    not a failure); after it ends, the next look queues the task again."""
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    worker = Orphan("import time; time.sleep(30)")
    ledger.set_runner_pid("T-b", worker.pid, proc_start(worker.pid))
    try:
        await daemon.recover()
        assert worker.poll() is None and "T-b" not in daemon._queued["B:desk"]
        assert "T-b" in daemon._recheck
        await daemon._recover_auto(ledger.task("T-b", "owner"))       # still running: skipped again
        assert "T-b" in daemon._recheck and not [r for r in ledger.failures() if r["task_id"] == "T-b"]
        worker.terminate()
        worker.wait(5)
        await daemon._recover_auto(ledger.task("T-b", "owner"))       # what the heartbeat does
        assert "T-b" in daemon._queued["B:desk"] and "T-b" not in daemon._recheck
    finally:
        worker.kill()
        ledger.close()


async def test_a_record_without_start_time_does_not_admit_a_worker(tmp_path):
    from mutmuas.node import lease_refusal
    agent, ledger, _ = _setup(tmp_path, "RUNNING")
    ledger.set_runner_pid("T-b", os.getpid(), None)           # a record from before start times were kept
    session = Orphan("import time; time.sleep(30)")
    try:
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
        assert lease_refusal(ledger, "B:desk") is not None       # this process is not proven to be the worker
    finally:
        session.kill()
        ledger.close()


async def test_a_reused_pid_with_another_start_time_does_not_block_the_retry(tmp_path):
    _, ledger, daemon = _setup(tmp_path, "RUNNING")
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    ledger.set_runner_pid("T-b", other.pid, "a start time of the old worker")
    try:
        await daemon.recover()
        assert other.poll() is None and "T-b" in daemon._queued["B:desk"]
    finally:
        other.kill()
        other.wait(5)
        ledger.close()


def test_process_probes_contract(monkeypatch):
    """proc_start and _zombie read /proc or an absolute-path ps; on failure they say "not known" (None or
    False), never a guess (pinned before cleanup #1 merged their shared reading)."""
    me = os.getpid()
    assert node_mod.proc_start(me) and node_mod.proc_start(me) == node_mod.proc_start(me)
    assert node_mod._zombie(me) is False
    gone = 2 ** 22 + 54321                                             # no such process
    assert node_mod.proc_start(gone) is None and node_mod._zombie(gone) is False
    monkeypatch.setattr(node_mod, "_PS", None)
    if not os.path.exists(f"/proc/{me}/stat"):                         # macOS: no /proc, and now no ps either
        assert node_mod.proc_start(me) is None and node_mod._zombie(me) is False


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


def _be_the_worker_of(ledger, monkeypatch, task_id):
    """This process is the worker the daemon started for task_id: its task, a live runner, MUTMUAS_TASK_ID."""
    import os
    from mutmuas.node import proc_start
    owned_task(ledger, task_id, "RUNNING", claim="worker")
    ledger.set_runner_pid(task_id, os.getpid(), proc_start(os.getpid()))
    monkeypatch.setenv("MUTMUAS_TASK_ID", task_id)


@pytest.mark.asyncio
async def test_worker_may_not_accept_another_task_as_the_session(tmp_path, monkeypatch):
    """A daemon-run worker's MCP server must not claim another task on the same address for the session."""
    _, ledger, hub = _auto_worker(tmp_path)
    task_id = "T-review-other"
    owned_task(ledger, task_id)
    _be_the_worker_of(ledger, monkeypatch, "T-review-worker")
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
    _be_the_worker_of(ledger, monkeypatch, "T-review-worker")
    try:
        with pytest.raises(PermissionError, match="session"):
            await tools.submit_result(hub, "B:desk", "complete", "wrong worker delivered",
                                      task_id=task_id)
        assert ledger.task(task_id, "owner")["status"] == "RUNNING"
    finally:
        ledger.close()


def _auto_node(tmp_path):
    agent, cfg, ledger, hub, _ = auto_worker_node(tmp_path)
    return agent, cfg, ledger, hub


def _holder():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"])


@pytest.mark.asyncio
async def test_recovery_does_not_release_a_still_running_worker_to_the_session(tmp_path):
    """An unclean daemon restart must not hand off a task while its detached worker process is alive.
    D-040: the old worker is not stopped; the task is skipped while it runs (never two actors)."""
    agent, cfg, ledger, hub = _auto_node(tmp_path)
    task_id = "T-worker-survives"
    owned_task(ledger, task_id)
    ledger.update_task(task_id, "owner", status="RUNNING")
    assert ledger.claim_task(task_id, "worker", ("RUNNING",)) is None
    worker, session = _holder(), _holder()
    from mutmuas.node import proc_start
    ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        assert worker.poll() is None                # D-040: not stopped, skipped and looked at again
        await daemon._execute(agent, task_id)  # dequeued after restart while the session is present
        task = ledger.task(task_id, "owner")
        assert task["runner"] == "worker" and task["status"] != "PENDING"
    finally:
        for proc in (worker, session):
            proc.terminate()
            proc.wait(5)
        ledger.close()


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


@pytest.mark.asyncio
async def test_cleanly_exited_old_worker_is_requeued_not_quarantined(tmp_path):
    ledger, daemon, task_id = _daemon(tmp_path)
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"],
                              start_new_session=True)
    try:
        ledger.set_runner_pid(task_id, worker.pid, proc_start(worker.pid))
        worker.wait(5)
        await daemon.recover()
        assert ledger.task(task_id, "owner")["status"] == "ACCEPTED"
        assert task_id in daemon._queued["B:desk"]
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.wait(5)
        ledger.close()


class FakeStat:
    def __init__(self, contents):
        self.contents = contents

    def exists(self):
        return True

    def read_text(self):
        return self.contents


def test_malformed_proc_state_does_not_guess_zombie(monkeypatch):
    # Linux /proc state is one character. A longer value is malformed, not Z.
    monkeypatch.setattr(node_mod, "Path", lambda path: FakeStat("123 (worker) Zextra 1 0\n"))
    assert node_mod._zombie(123) is False


async def test_session_cannot_report_progress_on_a_worker_task(tmp_path):
    """Codex review of 6116466: report_progress lacked the holder check that submit_result had."""
    import pytest

    from mutmuas import tools
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.hub import Hub
    from mutmuas.ledger import Ledger
    from mutmuas.protocol import Envelope, request_body
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script", command=["true"],
                        workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-w",
                                      body=request_body("t", "t")))
    ledger.update_task("T-w", "owner", status="RUNNING")
    assert ledger.claim_task("T-w", "worker", ("RUNNING",)) is None
    try:
        with pytest.raises(PermissionError, match="worker"):
            await tools.report_progress(hub, "B:desk", "the session speaks for the worker", task_id="T-w")
        assert ledger.task("T-w", "owner")["status"] == "RUNNING"
    finally:
        ledger.close()


async def test_an_old_worker_that_still_runs_gets_its_draft_delivered_once_it_ends(tmp_path):
    """D-040: a worker that outlived the daemon is not stopped; once it has ended, the draft it submitted is
    delivered at the next look instead of running the task again."""
    from conftest import Orphan, auto_worker_node, owned_task

    from mutmuas.node import proc_start
    _, _, ledger, _, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-orphan", "RUNNING", claim="worker")
    orphan = Orphan("import time; time.sleep(30)")
    ledger.set_runner_pid("T-orphan", orphan.pid, proc_start(orphan.pid))
    ledger.update_task("T-orphan", "owner", result_draft={"status": "complete", "summary": "done by the orphan"})
    try:
        await daemon.recover()
        assert ledger.task("T-orphan", "owner")["status"] == "RUNNING"          # skipped while it runs
        orphan.terminate()
        orphan.wait(5)
        await daemon._recover_auto(ledger.task("T-orphan", "owner"))           # the next heartbeat's look
        task = ledger.task("T-orphan", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "done by the orphan"
        assert not daemon._queued["B:desk"]
    finally:
        orphan.kill()
        ledger.close()


def test_a_reused_pid_is_not_the_recorded_worker():
    import os

    from mutmuas.node import proc_start, same_process
    assert same_process(os.getpid(), proc_start(os.getpid()))
    assert not same_process(os.getpid(), "a start time of another process")
    assert not same_process(None, None)


async def test_a_worker_accepting_its_own_task_gets_success(tmp_path, monkeypatch):
    """The pilot's worker called accept_task on the task the daemon had already claimed for it and got the
    misleading 'being done by the worker (D-032a)' error; for the worker itself it is a no-op."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-own", "RUNNING", claim="worker")
    ledger.set_runner_pid("T-own", os.getpid(), proc_start(os.getpid()))    # this process is the running worker
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-own")                     # the daemon started this process for it
    try:
        out = await tools.accept_task(hub, "B:desk", "T-own")
        assert out["accepted"] is True
        task = ledger.task("T-own", "owner")
        assert task["status"] == "RUNNING" and task["runner"] == "worker"
    finally:
        ledger.close()


async def test_a_plain_workers_accept_does_not_hand_its_task_to_the_session(tmp_path, monkeypatch):
    """A worker-mode task has no runner claim; an accept from its worker must not claim it for the session,
    or the worker's own submit_result is refused afterwards."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-own", "RUNNING")                                  # mode: worker leaves it unclaimed
    ledger.set_runner_pid("T-own", os.getpid(), proc_start(os.getpid()))    # this process is the running worker
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-own")
    try:
        assert (await tools.accept_task(hub, "B:desk", "T-own"))["accepted"] is True
        assert ledger.task("T-own", "owner")["runner"] is None
        out = await tools.submit_result(hub, "B:desk", "complete", "done", task_id="T-own")
        assert out["recorded"] is True                                     # not refused as the session's task
    finally:
        ledger.close()


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


def _publishing_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[
            AgentConfig(
                id=agent_id,
                mode="interactive",
                permissions=["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"],
            )
            for agent_id in agent_ids
        ],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def test_lease_ancestry_does_not_trust_path_ps(tmp_path, monkeypatch):
    """The lease check must not derive process ancestry from a PATH-controlled tool."""
    _, ledger, _, _ = _publishing_stack(tmp_path, "main")
    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        ledger.session_beat("A:main", holder.pid, "/holder", session_pid=holder.pid)
        fake_ps = tmp_path / "ps"
        fake_ps.write_text(f"#!/bin/sh\nprintf '%s\\n' {holder.pid}\n")
        fake_ps.chmod(0o755)
        monkeypatch.setenv("PATH", str(tmp_path))
        monkeypatch.delenv("MUTMUAS_TASK_ID", raising=False)

        assert lease_refusal(ledger, "A:main") is not None
    finally:
        holder.terminate()
        holder.wait(timeout=5)
        ledger.close()


def test_watch_is_not_lease_free_because_it_reads_mail_content():
    """A second session must not use watch to inspect the live holder's mail."""
    assert "cmd_watch" not in cli.LEASE_FREE


async def test_watch_needs_the_session_unless_headers_only(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("coder")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])  # a live session elsewhere
    agentctl = Path(sys.executable).parent / "agentctl"
    logs = {}
    procs = []
    try:
        hub_b.ledger.session_beat("B:coder", holder.pid, "/elsewhere", session_pid=holder.pid)
        assert session_alive(hub_b.ledger.session_of("B:coder"))
        for name, extra in (("content", []), ("headers", ["--headers-only"])):
            logs[name] = tmp_path / f"{name}.log"
            f = open(logs[name], "w")
            procs.append(subprocess.Popen([str(agentctl), "watch", *extra, "--dry-run", "--interval", "5",
                                           "--config", str(b.path), "--as", "B:coder"], stdout=f, stderr=f))
        await asyncio.sleep(2)
        await tools.send_request(hub_a, "A:main", "B:coder", "SECRET-OBJECTIVE", "watch test")
        await eventually(lambda: "notify:" in logs["headers"].read_text(), what="headers-only notice")
        headers = logs["headers"].read_text()
        assert "REQUEST from A:main" in headers and "SECRET-OBJECTIVE" not in headers
        assert "held by another session" in logs["content"].read_text()     # refused, and nothing shown
        assert "SECRET-OBJECTIVE" not in logs["content"].read_text()
    finally:
        for p in procs:
            p.terminate()
            p.wait(5)
        holder.terminate()
        holder.wait(5)


def _request(task_id: str, objective: str = "label the desk images", **extra) -> Envelope:
    return Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                    body={**request_body(objective, "reason"), **extra})


async def _arrive(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    state = await daemon._on_request(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")


@pytest.fixture
def session():
    proc = Orphan("import time; time.sleep(60)")
    yield proc
    proc.kill()


async def test_a_session_switched_off_leaves_the_work_to_the_worker(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        ledger.set_session_accepting("B:desk", False)
        await _arrive(daemon, agent, _request("T-o"))
        assert ledger.task("T-o", "owner")["status"] == "ACCEPTED" and "T-o" in daemon._queued["B:desk"]
        assert "T-o" not in [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)]
        ledger.set_session_accepting("B:desk", True)
        await _arrive(daemon, agent, _request("T-on"))
        assert ledger.task("T-on", "owner")["status"] == "PENDING"
    finally:
        ledger.close()


async def test_off_is_refused_without_a_worker_to_take_the_work(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        agent.auto_worker = False
        with pytest.raises(PermissionError, match="worker"):
            await tools.set_session_taking_work(hub, "B:desk", False)
        agent.auto_worker = True
        assert (await tools.set_session_taking_work(hub, "B:desk", False))["session_takes_work"] is False
    finally:
        ledger.close()


async def test_a_second_sessions_mcp_tools_refuse_to_act_but_whoami_answers(tmp_path, monkeypatch):
    """The MCP server's own front door: while another live session holds the agent, every tool but whoami
    refuses, and nothing is read or marked."""
    from mutmuas.mcp_server import build_server

    async def no_bus(cfg, *_args, **_kwargs):
        return Hub(cfg, None, Ledger(cfg.db_path))
    monkeypatch.setattr(Hub, "open", no_bus)
    agent, cfg, ledger, _, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-s", ingest=True)
    holder = Orphan("import time; time.sleep(30)")
    ledger.session_beat("B:desk", holder.pid, str(agent.workdir_path), session_pid=holder.pid)
    server = build_server(cfg, "B:desk")
    try:
        async with server.settings.lifespan(server):
            for tool, args in (("inbox", {"only": "all"}), ("accept_task", {"task_id": "T-s"})):
                out = await server.call_tool(tool, args)
                assert "does not hold the agent" in out.content[0].text, tool
            who = await server.call_tool("whoami", {})
            assert "B:desk" in who.content[0].text
        assert ledger.task("T-s", "owner")["status"] == "PENDING"
    finally:
        holder.kill()
        ledger.close()
