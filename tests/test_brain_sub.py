"""D-073 batch 1: internal subtasks (a brain's long work: always a worker, never the session; a sub sees no mail and
does not write PLAN/HANDOFF, the node marks its line on the brain's plan) and brain batches (one conversation,
resumed while work keeps coming; forgotten after an idle spell)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest
from conftest import Orphan, auto_worker_node

from mutmuas import tools
from mutmuas.protocol import Envelope, request_body, result_body


def _node(tmp_path, **extra):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path, **extra)
    (agent.workdir_path / "robo").mkdir(parents=True)
    daemon._internal["B:desk"] = __import__("asyncio").PriorityQueue()
    return agent, cfg, ledger, hub, daemon


def _parent(ledger, task_id="T-p", project="robo"):
    body = request_body("build the detector", "robo-desk", kind="query")
    body["project"] = project
    env = Envelope(type="REQUEST", sender="A:lead", to="B:desk", task_id=task_id, body=body)
    ledger.ingest(env)
    ledger.create_owned_task(env)
    ledger.update_task(task_id, "owner", status="RUNNING")
    return env


async def _sub(hub, daemon, agent, **kw):
    """The brain (B:desk, inside T-p) sends itself an internal subtask; the daemon handles its arrival."""
    sent = await tools.send_request(hub, "B:desk", "B:desk", "train the detector", "long", internal=True,
                                    parent_task="T-p", **kw)
    [env] = [e for e in hub.ledger.outbox() if e.task_id == sent["task_id"] and e.type == "REQUEST"]
    hub.ledger.ingest(env)
    state = await daemon._on_request(agent, env)
    hub.ledger.mark_handled(env.message_id, state or "handled")
    return sent["task_id"], env


@pytest.fixture
def session():
    proc = Orphan("import time; time.sleep(60)")
    yield proc
    proc.kill()


# --------------------------------------------------------------------------- internal subtasks


async def test_an_internal_subtask_always_goes_to_the_worker_and_not_to_the_session(tmp_path, session):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        sub, env = await _sub(hub, daemon, agent)
        assert env.body["project"] == "robo"                                 # inherited from its parent
        assert ledger.task(sub, "owner")["status"] == "ACCEPTED" and sub in daemon._queued["B:desk"]
        assert daemon._internal["B:desk"].qsize() == 1 and daemon._queues["B:desk"].qsize() == 0
        assert sub not in [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True)]
    finally:
        ledger.close()


async def test_an_internal_subtask_from_someone_else_is_refused(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    body = request_body("x", "y")
    body["internal"] = True
    env = Envelope(type="REQUEST", sender="A:lead", to="B:desk", task_id="T-x", body=body)
    try:
        ledger.ingest(env)
        await daemon._on_request(agent, env)
        assert ledger.task("T-x", "owner")["status"] == "FAILED"
    finally:
        ledger.close()


async def test_a_sub_worker_has_only_its_own_three_tools(tmp_path):
    from mutmuas.mcp_server import build_server
    agent, cfg, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)
    try:
        sub, _ = await _sub(hub, daemon, agent)
        names = {t.name for t in await build_server(cfg, "B:desk", worker_task=sub).list_tools()}
        assert names == {"report_progress", "submit_result", "add_job"}
        brain = {t.name for t in await build_server(cfg, "B:desk", worker_task="T-p").list_tools()}
        assert {"inbox", "send_request", "submit_result"} <= brain
    finally:
        ledger.close()


async def test_a_sub_worker_cannot_read_mail_through_agentctl(tmp_path):
    agent, cfg, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)
    path = tmp_path / "node.yaml"
    import yaml
    path.write_text(yaml.safe_dump({"project": "p", "node": "B", "data_dir": str(tmp_path / "data"),
                                    "agents": [{"id": "desk", "mode": "interactive", "auto_worker": True,
                                                "runtime": "script", "command": ["true"],
                                                "workdir": str(agent.workdir_path)}]}))
    try:
        sub, _ = await _sub(hub, daemon, agent)
        env = {**os.environ, "MUTMUAS_CONFIG": str(path), "MUTMUAS_AGENT": "B:desk", "MUTMUAS_TASK_ID": sub}
        run = subprocess.run([sys.executable, "-m", "mutmuas.cli", "inbox", "--peek"], env=env,
                             capture_output=True, text=True)
        assert run.returncode != 0 and "internal subtask" in run.stderr
    finally:
        ledger.close()


async def test_the_node_marks_the_subs_line_on_the_brains_plan(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)
    plan = agent.workdir_path / "robo" / "PLAN.md"
    try:
        sub, _ = await _sub(hub, daemon, agent)
        plan.write_text(f"# PLAN\n\n## T-p detector\n- [ ] {sub} train the detector\n- [ ] evaluate\n")
        await tools.report_progress(hub, "B:desk", "epoch 3/20", task_id=sub)
        assert f"- [>] {sub} train the detector" in plan.read_text()
        await hub.finish(sub, result_body("complete", "mAP 0.91, weights in runs/"))
        text = plan.read_text()
        assert f"- [x] {sub} train the detector — mAP 0.91, weights in runs/" in text and "- [ ] evaluate" in text
    finally:
        ledger.close()


def test_a_sub_runs_on_its_model_writes_under_runs_and_is_told_its_limits(tmp_path):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.runtime import ClaudeCodeRuntime, TaskContext, worker_prompt
    work = tmp_path / "work"
    (work / "robo").mkdir(parents=True)
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"))
    agent = AgentConfig(id="desk", runtime="claude-code", workdir=str(work), model="opus",
                        permissions=["READ", "WRITE_WORKTREE", "RUN_EXPERIMENT"])

    def argv(**extra):
        body = {**request_body("x", "y"), "project": "robo", **extra}
        ctx = TaskContext("T-s", Envelope(type="REQUEST", sender="B:desk", to="B:desk", task_id="T-s", body=body),
                          agent, node)
        return ClaudeCodeRuntime(agent, node).command(ctx)[0], worker_prompt(ctx)
    cmd, prompt = argv(internal=True)
    assert cmd[cmd.index("--model") + 1] == "sonnet" and "runs/T-s" in prompt and "Do not edit PLAN.md" in prompt
    cmd, _ = argv(internal=True, model="haiku")
    assert cmd[cmd.index("--model") + 1] == "haiku"
    cmd, _ = argv()
    assert cmd[cmd.index("--model") + 1] == "opus"                           # a brain runs on the post's model


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


async def test_a_batch_stays_while_a_sub_still_runs(tmp_path):
    agent, _, ledger, hub, daemon = _node(tmp_path)
    _parent(ledger)
    try:
        await _sub(hub, daemon, agent)
        ledger.set_brain_session("B:desk", "robo", "S-1")
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ledger.db.execute("UPDATE brains SET updated_at=?", (old,))
        await daemon._expire_brains()
        assert ledger.brain_session("B:desk", "robo") == "S-1"
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
