"""D-073 brain batches: a post's worker runs share one conversation per project, resumed while work keeps coming,
forgotten after an idle spell (unless a task of that project still runs or waits on a job)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from conftest import auto_worker_node

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


# --------------------------------------------------------------------------- internal subtasks


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
    from mutmuas.runtime import RunOutcome
    agent, _, ledger, hub, daemon = _node(tmp_path)
    agent.runtime = "claude-code"                                     # brain batches are claude-code's
    seen = []

    class Brain:
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
    from mutmuas.runtime import RunOutcome
    agent, _, ledger, hub, daemon = _node(tmp_path)
    agent.runtime = "claude-code"
    _parent(ledger)
    ledger.update_task("T-p", "owner", status="ACCEPTED")
    ledger.set_brain_session("B:desk", "robo", "S-gone")

    class Lost:
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


# --------------------------------------------------------------------------- Codex light review of 09456a9


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

