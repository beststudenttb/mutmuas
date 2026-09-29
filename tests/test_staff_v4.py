"""Staff system v4 (D-029..D-032): project directories, worker settings, dual mode."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from mutmuas.node import session_fields


def _live(cwd) -> dict:
    return {"pid": os.getpid(), "cwd": str(cwd), "last_seen": datetime.now(timezone.utc).isoformat()}


# --------------------------------------------------------------------------- C1: sub-directories of the workdir


def test_session_in_a_subdirectory_of_the_workdir_is_not_warned(tmp_path):
    workdir = tmp_path / "paper"
    (workdir / "visualrl" / "notes").mkdir(parents=True)
    assert "session_warning" not in session_fields(_live(workdir / "visualrl"), workdir)
    assert "session_warning" not in session_fields(_live(workdir / "visualrl" / "notes"), workdir)
    link = tmp_path / "shortcut"
    link.symlink_to(workdir / "visualrl")
    assert "session_warning" not in session_fields(_live(link), workdir)


def test_session_outside_the_workdir_is_still_warned(tmp_path):
    workdir = tmp_path / "paper"
    workdir.mkdir()
    sibling = tmp_path / "paper-2"            # shares the prefix, but is not inside the workdir
    sibling.mkdir()
    assert "not in its workdir" in session_fields(_live(sibling), workdir)["session_warning"]
    assert "not in its workdir" in session_fields(_live(tmp_path), workdir)["session_warning"]


# --------------------------------------------------------------------------- C2: worker tools come from node.yaml only


def _claude_ctx(tmp_path, kind="query", permissions=("READ",)):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    from mutmuas.runtime import ClaudeCodeRuntime, TaskContext
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path / "data"))
    workdir = tmp_path / "work" / "paper" / "visualrl"
    workdir.mkdir(parents=True, exist_ok=True)
    agent = AgentConfig(id="paper-visualrl", runtime="claude-code", workdir=str(workdir),
                        permissions=list(permissions))
    req = Envelope(type="REQUEST", sender="B:sec", to="C:paper-visualrl", task_id="T-1",
                   body=request_body("x", "y", kind=kind))
    return ClaudeCodeRuntime(agent, node), TaskContext("T-1", req, agent, node), workdir


def test_worker_loads_only_project_settings(tmp_path):
    """Approvals given in a session ("don't ask again") land in .claude/settings.local.json of the directory, and
    user settings may allow tools too (Codex review of 6c2a60a); a worker must inherit neither (C2 experiment
    2026-09-28: with local loaded, a rule there let a worker run a Bash command node.yaml never granted).
    "project" alone still loads the function CLAUDE.md above and the project's memory (experiment E6)."""
    runtime, ctx, _ = _claude_ctx(tmp_path)
    argv, _ = runtime.command(ctx)
    assert argv[argv.index("--setting-sources") + 1] == "project"


def test_available_tools_are_exactly_what_node_yaml_grants(tmp_path):
    """--allowedTools only pre-approves; --tools limits what exists (experiment E7: with --tools, a local allow
    rule could not run Bash; E9: MCP tools stay available)."""
    runtime, ctx, _ = _claude_ctx(tmp_path)
    argv, _ = runtime.command(ctx)
    assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep"
    runtime, ctx, _ = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    argv, _ = runtime.command(ctx)
    assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Edit,Write,Bash"
    assert "Bash(git:*)" in argv[argv.index("--allowedTools") + 1].split(",")


def test_worker_refuses_grants_in_an_ancestor_settings_file(tmp_path):
    """Any .claude/settings.json from the project directory up could widen Bash(git:*) to all of Bash."""
    import json

    import pytest
    runtime, ctx, workdir = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    (workdir.parent / ".claude").mkdir()
    (workdir.parent / ".claude" / "settings.json").write_text(json.dumps({"permissions": {"allow": ["Bash"]}}))
    with pytest.raises(PermissionError, match="settings.json"):
        runtime.command(ctx)


def test_worker_may_not_edit_the_handoff(tmp_path):
    """HANDOFF.md belongs to the interactive session (D-030); the worker only appends to worker-log.md."""
    import json
    runtime, ctx, workdir = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    argv, _ = runtime.command(ctx)
    settings = json.loads(open(argv[argv.index("--settings") + 1]).read())
    handoff = "/" + os.path.realpath(workdir / "HANDOFF.md")          # //abs: an absolute path in a rule
    assert {f"Edit({handoff})", f"Write({handoff})"} <= set(settings["permissions"]["deny"])


def test_worker_refuses_a_project_settings_file_that_grants_tools(tmp_path):
    """.claude/settings.json of the project directory is loaded (source "project"); tools must come from
    node.yaml alone (D-032 item 4), so a grant there stops the run instead of silently widening it."""
    import json

    import pytest
    runtime, ctx, workdir = _claude_ctx(tmp_path)
    (workdir / ".claude").mkdir()
    (workdir / ".claude" / "settings.json").write_text(json.dumps({"permissions": {"allow": ["Bash"]}}))
    with pytest.raises(PermissionError, match="settings.json"):
        runtime.command(ctx)
    (workdir / ".claude" / "settings.json").write_text(json.dumps({"permissions": {"deny": ["Bash"]}}))
    runtime.command(ctx)                                              # restricting is fine


# --------------------------------------------------------------------------- C3: start in the project directory


def _code_ctx(tmp_path, runtime="claude-code", code_mode="copy", workdir=None, code_dirs=(), kind="code"):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    from mutmuas.runtime import TaskContext
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path / "data"))
    repo = tmp_path / "repo"
    repo.mkdir(exist_ok=True)
    workdir = workdir or tmp_path / "work" / "paper" / "visualrl"
    workdir.mkdir(parents=True, exist_ok=True)
    agent = AgentConfig(id="paper-visualrl", runtime=runtime, workdir=str(workdir), repo=str(repo),
                        code_mode=code_mode, code_dirs=[str(d) for d in code_dirs],
                        permissions=["READ", "WRITE_WORKTREE"])
    agent.validate()
    req = Envelope(type="REQUEST", sender="B:sec", to="C:paper-visualrl", task_id="T-1",
                   body=request_body("x", "y", kind=kind))
    wt = tmp_path / "worktrees" / "C-paper-visualrl-T-1"
    ctx = TaskContext("T-1", req, agent, node, workdir=wt if code_mode == "copy" else None,
                      git_branch="mm/C-paper-visualrl/T-1" if code_mode == "copy" else None)
    return agent, node, ctx, workdir, repo, wt


def _add_dirs(argv):
    return [argv[i + 1] for i, a in enumerate(argv) if a == "--add-dir"]


def test_copy_mode_worker_starts_in_the_project_directory_with_the_worktree_added(tmp_path):
    """The function CLAUDE.md and the project's memory belong to the start directory (D-031): a code task starts
    there and reaches its private worktree through --add-dir, never the main repo."""
    from mutmuas.runtime import ClaudeCodeRuntime, CodexRuntime
    agent, node, ctx, workdir, repo, wt = _code_ctx(tmp_path)
    for runtime in (ClaudeCodeRuntime(agent, node), CodexRuntime(agent, node)):
        assert runtime.start_dir(ctx) == workdir
        argv, _ = runtime.command(ctx)
        assert _add_dirs(argv) == [str(wt)]
    argv, _ = CodexRuntime(agent, node).command(ctx)
    assert argv[argv.index("-C") + 1] == str(workdir)


def test_workdir_inside_the_repo_keeps_starting_in_the_worktree(tmp_path):
    """Starting in (or below) the main repo would open it to the task (the b461307 MERGE bypass)."""
    from mutmuas.runtime import ClaudeCodeRuntime, CodexRuntime
    inside = tmp_path / "repo" / "sub"
    agent, node, ctx, _, repo, wt = _code_ctx(tmp_path, workdir=inside)
    for runtime in (ClaudeCodeRuntime(agent, node), CodexRuntime(agent, node)):
        assert runtime.start_dir(ctx) == wt
        assert _add_dirs(runtime.command(ctx)[0]) == []


def test_direct_mode_adds_the_project_code_and_its_claude_md(tmp_path):
    """Project-level work edits the project code in place (D-031); its CLAUDE.md loads as well."""
    from mutmuas.runtime import ClaudeCodeRuntime
    code = tmp_path / "paper-src"
    code.mkdir()
    agent, node, ctx, workdir, _, _ = _code_ctx(tmp_path, code_mode="direct", code_dirs=[code])
    runtime = ClaudeCodeRuntime(agent, node)
    assert runtime.start_dir(ctx) == workdir and not agent.copies_code
    assert _add_dirs(runtime.command(ctx)[0]) == [str(code)]
    assert ctx.env()["CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD"] == "1"


def test_code_mode_is_validated(tmp_path):
    import pytest

    from mutmuas.config import ConfigError
    with pytest.raises(ConfigError, match="code_mode"):
        _code_ctx(tmp_path, code_mode="sometimes")
    with pytest.raises(ConfigError, match="code_dirs"):
        _code_ctx(tmp_path, code_mode="direct")


def test_worker_prompt_names_the_code_and_the_worker_log(tmp_path):
    from mutmuas.runtime import worker_prompt
    _, _, ctx, workdir, _, wt = _code_ctx(tmp_path)
    prompt = worker_prompt(ctx)
    assert str(wt) in prompt and "worker-log.md" in prompt and "HANDOFF.md" in prompt


# --------------------------------------------------------------------------- C4: every owner-side change checks the actor


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


async def test_restart_stops_a_surviving_worker_and_delivers_its_draft(tmp_path):
    """Option B (Codex review of f8c105e): no adoption. A worker that submitted its result and still runs at the
    restart is stopped, and the result it submitted is delivered rather than run again."""
    import asyncio
    import subprocess
    import sys

    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.hub import Hub
    from mutmuas.ledger import Ledger
    from mutmuas.node import NodeDaemon, proc_start
    from mutmuas.protocol import Envelope, request_body
    agent = AgentConfig(id="desk", mode="interactive", auto_worker=True, runtime="script", command=["true"],
                        workdir=str(tmp_path / "work"))
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    ledger.create_owned_task(Envelope(type="REQUEST", sender="A:x", to="B:desk", task_id="T-orphan",
                                      body=request_body("t", "t")))
    ledger.update_task("T-orphan", "owner", status="RUNNING")
    assert ledger.claim_task("T-orphan", "worker", ("RUNNING",)) is None
    from conftest import Orphan
    orphan = Orphan("import time; time.sleep(30)")                     # as after a crash: reaped by init
    ledger.set_runner_pid("T-orphan", orphan.pid, proc_start(orphan.pid))
    ledger.update_task("T-orphan", "owner", result_draft={"status": "complete", "summary": "done by the orphan"})
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    daemon._queues["B:desk"] = asyncio.Queue()
    daemon._queued["B:desk"] = set()
    try:
        await daemon.recover()
        orphan.wait(5)                                                   # stopped at the restart
        task = ledger.task("T-orphan", "owner")
        assert task["status"] == "COMPLETED" and task["result"]["summary"] == "done by the orphan"
        assert not daemon._queued["B:desk"] and not daemon._background
    finally:
        if orphan.poll() is None:
            orphan.kill()
        ledger.close()


def test_a_reused_pid_is_not_the_recorded_worker():
    import os

    from mutmuas.node import proc_start, same_process
    assert same_process(os.getpid(), proc_start(os.getpid()))
    assert not same_process(os.getpid(), "a start time of another process")
    assert not same_process(None, None)
