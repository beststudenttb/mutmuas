"""D-073 brain batches: a post's worker runs share one conversation per project, resumed while work keeps coming,
forgotten after an idle spell (unless a task of that project still runs or waits on a job).

A brain with no session is woken by what names it (next, its reminders, a child's question or block),
unless it is paused or its session is online."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import tools
from mutmuas.protocol import Envelope, request_body


def _node(tmp_path, **extra):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path, **extra)
    (agent.workdir_path / "robo").mkdir(parents=True)
    return agent, cfg, ledger, hub, daemon


def _parent(ledger, task_id="T-p", project="robo"):
    body = request_body("build the detector", "robo-desk", kind="query")
    body["project"] = project
    env = Envelope(type="REQUEST", sender="A:lead", to="B:desk", task_id=task_id, body=body)
    ledger.ingest(env)
    ledger.create_owned_task(env)
    ledger.update_task(task_id, "owner", status="RUNNING")
    return env


# --------------------------------------------------------------------------- brain batches


def test_a_brain_run_resumes_its_batch(tmp_path):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.runtime import ClaudeCodeRuntime, TaskContext
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"))
    agent = AgentConfig(id="desk", runtime="claude-code", workdir=str(tmp_path))
    ctx = TaskContext("T-1", Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-1",
                                      body=request_body("x", "y")), agent, node, resume="S-123")
    cmd, _ = ClaudeCodeRuntime(agent, node).command(ctx)
    assert cmd[cmd.index("--resume") + 1] == "S-123"


async def test_brain_runs_share_a_conversation_until_an_idle_spell(tmp_path, monkeypatch):
    from mutmuas import node as node_module
    from mutmuas.runtime import RunOutcome, SubprocessRuntime
    agent, _, ledger, hub, daemon = _node(tmp_path)
    agent.runtime = "claude-code"                                     # brain batches are claude-code's
    seen = []

    class Brain(SubprocessRuntime):
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            seen.append(ctx.resume)
            done = {"status": "complete", "summary": "done"}
            return RunOutcome(0, json.dumps(done), result=done, session_id="S-1")
    monkeypatch.setattr(node_module, "make_runtime", Brain)
    try:
        for t in ("T-1", "T-2"):
            _parent(ledger, t)
            ledger.update_task(t, "owner", status="ACCEPTED")
            await daemon._execute(agent, t)
        assert seen == [None, "S-1"]
        assert ledger.brain_session("B:desk", "robo") == "S-1"
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ledger.db.execute("UPDATE brains SET updated_at=?", (old,))
        await daemon._expire_brains()
        assert ledger.brain_session("B:desk", "robo") is None
    finally:
        ledger.close()


async def test_a_failed_resume_starts_the_next_attempt_afresh(tmp_path, monkeypatch):
    from mutmuas import node as node_module
    from mutmuas.runtime import RunOutcome, SubprocessRuntime
    agent, _, ledger, hub, daemon = _node(tmp_path)
    agent.runtime = "claude-code"
    _parent(ledger)
    ledger.update_task("T-p", "owner", status="ACCEPTED")
    ledger.set_brain_session("B:desk", "robo", "S-gone")

    class Lost(SubprocessRuntime):
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            return RunOutcome(1, "No conversation found with session ID: S-gone")
    monkeypatch.setattr(node_module, "make_runtime", Lost)
    try:
        await daemon._execute(agent, "T-p")
        assert ledger.brain_session("B:desk", "robo") is None and "T-p" in daemon._retry
    finally:
        ledger.close()


def test_a_brain_is_told_to_update_its_handoff_before_every_run_ends(tmp_path):
    """Sandbox run (T-20261002103910-305ca252): the brain's last run delivered but left HANDOFF saying it still
    waited on its subs; the next batch would have started from that."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.runtime import TaskContext, worker_prompt
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"))
    agent = AgentConfig(id="desk", runtime="claude-code", workdir=str(tmp_path))
    ctx = TaskContext("T-1", Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-1",
                                      body=request_body("x", "y")), agent, node)
    prompt = worker_prompt(ctx)
    assert "Before every run ends" in prompt and "HANDOFF.md" in prompt
    assert "what you did, what comes next and whom you wait for" in prompt


def _age_brains(ledger):
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    ledger.db.execute("UPDATE brains SET updated_at=?", (old,))


async def test_a_brain_still_running_or_queued_keeps_its_batch(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)                                      # T-p RUNNING in robo: the brain is at it
    try:
        ledger.set_brain_session("B:desk", "robo", "S-1")
        _age_brains(ledger)
        await daemon._expire_brains()
        assert ledger.brain_session("B:desk", "robo") == "S-1"
    finally:
        ledger.close()


async def test_busy_and_idle_are_per_project(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    agent.home("other").mkdir()
    _parent(ledger, project="other")
    try:
        ledger.update_task("T-p", "owner", status="WAITING")
        ledger.add_job("T-p", "B:desk", None, None, None, None, "training")    # other waits on a job: busy
        ledger.set_brain_session("B:desk", "robo", "S-robo")
        ledger.set_brain_session("B:desk", "other", "S-other")
        _age_brains(ledger)
        await daemon._expire_brains()
        assert ledger.brain_session("B:desk", "robo") is None          # idle robo ends
        assert ledger.brain_session("B:desk", "other") == "S-other"    # busy other stays
    finally:
        ledger.close()


async def _deliver(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    await daemon._handle(agent, env)


async def _waiting_brain(ledger, hub):
    """B:desk's task T-p waits (children job) on its child C1, which B:desk asked of C:far."""
    owned_task(ledger, "T-p", "WAITING", claim="worker", ingest=True)
    child = await tools.send_request(hub, "B:desk", "C:far", "train it", "part of T-p", parent_task="T-p")
    ledger.add_job("T-p", "B:desk", None, None, None, None, "children of T-p", children=True)
    return child["task_id"]


@pytest.mark.parametrize("kind", ["QUESTION", "BLOCKED"])
async def test_a_child_asking_or_blocked_wakes_the_waiting_brain(tmp_path, kind):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        body = {"question": "which camera?"} if kind == "QUESTION" else {"reason": "need the calibration"}
        await _deliver(daemon, agent, Envelope(type=kind, sender="C:far", to="B:desk", task_id=child,
                                               body={**body, "next": "B:desk"}))
        task = ledger.task("T-p", "owner")
        assert task["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
        assert ledger.jobs("T-p") == []                                   # the wait ended, saying why
        [job] = ledger.jobs("T-p", open_only=False)
        assert child in job["ended"] and kind in job["ended"]
    finally:
        ledger.close()


async def test_a_brain_whose_session_is_online_is_left_to_the_session(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        ledger.session_beat("B:desk", os.getpid(), str(tmp_path), session_pid=os.getpid())
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
    finally:
        ledger.close()


async def test_an_update_naming_the_brain_next_wakes_its_blocked_task(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-b", "BLOCKED", claim="worker", ingest=True)
        await _deliver(daemon, agent, Envelope(type="ANSWER", sender="A:sender", to="B:desk", task_id="T-b",
                                               body={"answer": "use camera 2", "next": "B:desk"}))
        assert ledger.task("T-b", "owner")["status"] == "ACCEPTED" and "T-b" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_paused_brain_is_not_woken(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        ledger.update_task("T-p", "owner", paused=1)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
    finally:
        ledger.close()


def test_the_brain_is_told_to_wait_on_children_not_on_a_done_file(tmp_path):
    from mutmuas.protocol import request_body
    from mutmuas.runtime import TaskContext, worker_prompt
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        req = Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-1", body=request_body("x", "y"))
        text = worker_prompt(TaskContext("T-1", req, agent, cfg))
        assert "children=True" in text and "not a done_file" in text and "reminder" in text
    finally:
        ledger.close()


@pytest.mark.parametrize("session", ["off", "other project"])
async def test_a_session_that_leaves_the_work_to_the_worker_does_not_stop_the_wake(tmp_path, session):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        cwd = agent.workdir_path
        if session == "other project":
            cwd = agent.workdir_path / "other"
            cwd.mkdir(parents=True)
        ledger.session_beat("B:desk", os.getpid(), str(cwd), session_pid=os.getpid())
        if session == "off":
            ledger.set_session_accepting("B:desk", False)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "ACCEPTED" and "T-p" in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_the_wake_tells_the_brain_its_wait_has_ended(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        child = await _waiting_brain(ledger, hub)
        await _deliver(daemon, agent, Envelope(type="QUESTION", sender="C:far", to="B:desk", task_id=child,
                                               body={"question": "which camera?", "next": "B:desk"}))
        [job] = ledger.jobs("T-p", open_only=False)
        assert "add_job again" in job["ended"]
    finally:
        ledger.close()


async def test_a_resume_of_a_paused_task_waiting_on_a_job_keeps_it_waiting(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        owned_task(ledger, "T-j", "WAITING", claim="worker", ingest=True)
        ledger.add_job("T-j", "B:desk", 999999, None, "/tmp/log", None, "training")
        ledger.update_task("T-j", "owner", paused=1)
        await _deliver(daemon, agent, Envelope(type="UPDATE", sender="A:sender", to="B:desk", task_id="T-j",
                                               body={"message": "resume", "resume": True, "next": "B:desk"}))
        task = ledger.task("T-j", "owner")
        assert not task["paused"] and task["status"] == "WAITING" and ledger.jobs("T-j")
        assert "T-j" not in daemon._queued["B:desk"]
    finally:
        ledger.close()


async def test_a_childs_result_leaves_the_parent_waiting_for_its_other_children(tmp_path):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        c1 = await _waiting_brain(ledger, hub)
        await tools.send_request(hub, "B:desk", "C:far", "eval", "part of T-p", parent_task="T-p")
        await _deliver(daemon, agent, Envelope(type="RESULT", sender="C:far", to="B:desk", task_id=c1,
                                               body={"status": "complete", "summary": "ok", "next": "B:desk"}))
        assert ledger.task("T-p", "owner")["status"] == "WAITING" and ledger.jobs("T-p")
        assert "T-p" not in daemon._queued["B:desk"]
    finally:
        ledger.close()
