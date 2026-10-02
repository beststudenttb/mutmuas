"""Project routing (D-069/D-072): one address, one directory per project under the post directory. A request may
name its project; the worker starts in that project's directory and delivers there; a session started in a project
directory takes only that project's work, and the rest goes to the worker."""

from __future__ import annotations

import pytest
from conftest import Orphan, auto_worker_node

from mutmuas import cli, tools
from mutmuas.protocol import Envelope, request_body, result_body


def _request(task_id: str, project: str | None = None) -> Envelope:
    body = request_body("label the desk images", "robo-desk needs them", kind="query")
    if project:
        body["project"] = project
    return Envelope(type="REQUEST", sender="A:lead", to="B:desk", task_id=task_id, body=body)


def _node(tmp_path, projects=("robo", "mutmuas"), **agent_extra):
    agent, cfg, ledger, hub, daemon = auto_worker_node(tmp_path, **agent_extra)
    for p in projects:
        (agent.workdir_path / p).mkdir(parents=True, exist_ok=True)
    return agent, ledger, hub, daemon


async def _arrive(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    state = await daemon._on_request(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")


# --------------------------------------------------------------------------- the request names its project


async def test_send_request_and_agentctl_ask_carry_the_project(tmp_path, monkeypatch):
    _, ledger, hub, _ = _node(tmp_path)
    try:
        sent = await tools.send_request(hub, "B:desk", "C:vision", "x", "y", project="robo")
        assert ledger.task(sent["task_id"], "requester")["request"]["project"] == "robo"
    finally:
        ledger.close()
    calls = []

    async def fake_send(hub, me, to, objective, reason, **kw):
        calls.append(kw.get("project"))
        return {"task_id": "T-x"}
    monkeypatch.setattr(tools, "send_request", fake_send)
    args = cli.agentctl_parser().parse_args(["ask", "C:vision", "do it", "--project", "robo", "--as", "B:desk"])
    await cli.cmd_ask(args, None)
    assert calls == ["robo"]


# --------------------------------------------------------------------------- where the worker runs and delivers


def test_the_worker_starts_in_the_project_directory(tmp_path):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.runtime import TaskContext, llm_start_dir
    work = tmp_path / "work"
    (work / "robo").mkdir(parents=True)
    (work / "mutmuas").mkdir()
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"))

    def ctx(project=None, **extra):
        agent = AgentConfig(id="desk", runtime="claude-code", workdir=str(work), **extra)
        req = _request("T-1", project)
        req.to = "B:desk"
        return TaskContext("T-1", req, agent, node)
    assert llm_start_dir(ctx("robo")) == work / "robo" and ctx("robo").cwd == work / "robo"
    assert llm_start_dir(ctx()) == work                                    # no project, no default: as before
    assert llm_start_dir(ctx(default_project="mutmuas")) == work / "mutmuas"
    assert llm_start_dir(ctx("robo", default_project="mutmuas")) == work / "robo"


async def test_a_request_for_a_project_without_a_directory_is_refused(tmp_path):
    agent, ledger, _, daemon = _node(tmp_path)
    try:
        await _arrive(daemon, agent, _request("T-n", "nope"))
        assert ledger.task("T-n", "owner")["status"] == "FAILED"
        [reject] = [e for e in ledger.outbox() if e.type == "REJECT" and e.task_id == "T-n"]
        assert "post-init" in reject.body["reason"]
        await _arrive(daemon, agent, _request("T-bad", "../etc"))
        assert ledger.task("T-bad", "owner")["status"] == "FAILED"
    finally:
        ledger.close()


async def test_delivery_uses_the_projects_plan_and_log(tmp_path):
    agent, ledger, hub, daemon = _node(tmp_path)
    project = agent.workdir_path / "robo"
    (project / "PLAN.md").write_text("# PLAN\n\n## T-d labelling\n- [x] labelled 120 images\n")
    try:
        await _arrive(daemon, agent, _request("T-d", "robo"))
        await hub.finish("T-d", result_body("complete", "labelled"))
        assert "T-d" in (project / "worker-log.md").read_text()
        assert not (agent.workdir_path / "worker-log.md").exists()
        assert "labelled 120 images" in ledger.task("T-d", "owner")["result"]["outputs"]["plan"]
        assert "T-d" not in (project / "PLAN.md").read_text()
    finally:
        ledger.close()


# --------------------------------------------------------------------------- which work a session takes


@pytest.fixture
def session():
    proc = Orphan("import time; time.sleep(60)")
    yield proc
    proc.kill()


async def test_a_session_in_a_project_directory_takes_only_that_projects_work(tmp_path, session):
    agent, ledger, hub, daemon = _node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path / "robo"), session_pid=session.pid)
    try:
        await _arrive(daemon, agent, _request("T-r", "robo"))
        await _arrive(daemon, agent, _request("T-m", "mutmuas"))
        assert ledger.task("T-r", "owner")["status"] == "PENDING" and "T-r" not in daemon._queued["B:desk"]
        assert ledger.task("T-m", "owner")["status"] == "ACCEPTED" and "T-m" in daemon._queued["B:desk"]
        listed = [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)]
        assert listed == ["T-r"]                                      # the session is not woken for T-m
    finally:
        ledger.close()


async def test_a_session_in_the_post_directory_takes_all_work(tmp_path, session):
    agent, ledger, hub, daemon = _node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        await _arrive(daemon, agent, _request("T-r", "robo"))
        await _arrive(daemon, agent, _request("T-m", "mutmuas"))
        assert {ledger.task(t, "owner")["status"] for t in ("T-r", "T-m")} == {"PENDING"}
        assert len(await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)) == 2
    finally:
        ledger.close()


async def test_the_worker_picks_up_other_projects_work_left_pending(tmp_path, session):
    """Work that arrived while the session was in the post directory, then the session moved into a project."""
    agent, ledger, _, daemon = _node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    try:
        await _arrive(daemon, agent, _request("T-m", "mutmuas"))
        assert ledger.task("T-m", "owner")["status"] == "PENDING"
        ledger.session_beat("B:desk", session.pid, str(agent.workdir_path / "robo"), session_pid=session.pid)
        await daemon._auto_dispatch()
        assert "T-m" in daemon._queued["B:desk"]
        refused = ledger.claim_task("T-m", "worker", ("ACCEPTED",),
                                    refuse_if=lambda: daemon._session_takes(agent, "B:desk", "T-m"))
        assert refused is None                                      # _execute's claim lets the worker run it
    finally:
        ledger.close()


# --------------------------------------------------------------------------- Codex light review of fe64cee


async def test_without_a_worker_a_project_session_still_sees_and_takes_other_projects_work(tmp_path, session):
    """auto_worker off: nobody else would take it, so it is neither filtered from the session nor left hidden."""
    agent, ledger, hub, daemon = _node(tmp_path)
    agent.auto_worker = False
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path / "robo"), session_pid=session.pid)
    try:
        await _arrive(daemon, agent, _request("T-m", "mutmuas"))
        assert [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)] == ["T-m"]
        assert ledger.task("T-m", "owner")["status"] == "PENDING"
    finally:
        ledger.close()


async def test_a_linked_project_directory_is_refused(tmp_path):
    """A project is a real directory right under the post directory: a link inside or out is not one."""
    agent, ledger, _, daemon = _node(tmp_path)
    (agent.workdir_path / "alias").symlink_to(agent.workdir_path / "robo", target_is_directory=True)
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (agent.workdir_path / "linked").symlink_to(outside, target_is_directory=True)
    try:
        for task_id, project in (("T-a", "alias"), ("T-l", "linked")):
            await _arrive(daemon, agent, _request(task_id, project))
            assert ledger.task(task_id, "owner")["status"] == "FAILED"
        assert agent.session_project(str(agent.workdir_path / "alias")) == "robo"    # the real directory
    finally:
        ledger.close()


async def test_a_session_in_a_case_variant_of_its_project_directory_is_that_project(tmp_path, session):
    agent, ledger, hub, daemon = _node(tmp_path)
    variant = agent.workdir_path / "ROBO"
    if not variant.is_dir():
        pytest.skip("case-sensitive file system")
    ledger.session_beat("B:desk", session.pid, str(variant), session_pid=session.pid)
    try:
        assert agent.session_project(str(variant)) == "robo"
        await _arrive(daemon, agent, _request("T-r", "robo"))
        assert ledger.task("T-r", "owner")["status"] == "PENDING"                      # the session's
        assert [m["task_id"] for m in await tools.inbox(hub, "B:desk", peek=True, types=tools.WAKE)] == ["T-r"]
        await _arrive(daemon, agent, _request("T-R", "ROBO"))                          # not the on-disk name
        assert ledger.task("T-R", "owner")["status"] == "FAILED"
    finally:
        ledger.close()
