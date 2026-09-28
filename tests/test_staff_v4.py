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


def test_worker_does_not_load_local_settings(tmp_path):
    """Approvals given in a session ("don't ask again") land in .claude/settings.local.json of the directory; a
    worker started there must not inherit them (C2 experiment 2026-09-28: with local loaded, a rule there let a
    worker run a Bash command node.yaml never granted). user,project keeps the function CLAUDE.md and memory."""
    runtime, ctx, _ = _claude_ctx(tmp_path)
    argv, _ = runtime.command(ctx)
    assert argv[argv.index("--setting-sources") + 1] == "user,project"


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
