"""Staff system v4 C4 (D-030, D-032, D-032a): one address, two ways of working.

An interactive agent with auto_worker: while no session holds it (after a grace period), the daemon runs its
tasks as a worker; while the leader's session is there, requests are listed for him and not run; a worker that
is already running is not interrupted, and the ledger keeps any task from being done twice.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys

import pytest
from conftest import HANDLERS, eventually, interactive

from mutmuas import node as node_mod
from mutmuas import tools


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
