"""How a worker run is set up (D-029..D-032): the project directory it starts in, only the settings and
tools node.yaml grants, its prompt, and its turn and cost limits."""

from __future__ import annotations

import json

import pytest
from conftest import owned_task
from test_job_wake import _node

from mutmuas import tools
from mutmuas.config import AgentConfig, NodeConfig, load_config
from mutmuas.protocol import Envelope, request_body
from mutmuas.runtime import ClaudeCodeRuntime, TaskContext, worker_prompt


# --------------------------------------------------------------------------- worker tools come from node.yaml only


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
    assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Skill"
    runtime, ctx, _ = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    argv, _ = runtime.command(ctx)
    assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Skill,Edit,Write,Bash"
    assert "Bash(git:*)" in argv[argv.index("--allowedTools") + 1].split(",")


def test_query_task_gets_the_tools_its_post_is_granted(tmp_path):
    """D-064: a query or artifact task on a post with WRITE_WORKTREE gets Edit/Write/Bash (r20: query workers had
    no write tool, so they could not keep their log or PLAN.md); without the permission it still gets none."""
    for kind in ("query", "artifact"):
        runtime, ctx, _ = _claude_ctx(tmp_path, kind=kind, permissions=("READ", "WRITE_WORKTREE"))
        argv, _ = runtime.command(ctx)
        assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Skill,Edit,Write,Bash"
        runtime, ctx, _ = _claude_ctx(tmp_path, kind=kind, permissions=("READ", "RUN_EXPERIMENT"))
        argv, _ = runtime.command(ctx)
        assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Skill,Bash"
        runtime, ctx, _ = _claude_ctx(tmp_path, kind=kind, permissions=("READ", "PUBLISH_ARTIFACT"))
        argv, _ = runtime.command(ctx)
        assert argv[argv.index("--tools") + 1] == "Read,Glob,Grep,Skill"

# --------------------------------------------------------------------------- start in the project directory


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


def _read_only_claude_worker(tmp_path):
    workdir = tmp_path / "work" / "paper" / "visualrl"
    workdir.mkdir(parents=True)
    agent = AgentConfig(id="paper-visualrl", runtime="claude-code", workdir=str(workdir),
                        permissions=["READ"])
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path / "data"))
    request = Envelope(type="REQUEST", sender="B:secretary", to="C:paper-visualrl",
                       task_id="T-review", body=request_body("read a document", "review", kind="query"))
    return ClaudeCodeRuntime(agent, node), TaskContext("T-review", request, agent, node)


def test_user_approval_cannot_grant_bash_to_read_only_worker(tmp_path, monkeypatch):
    """Claude's --allowedTools approves tools; it does not restrict tools allowed by user settings."""
    config_dir = tmp_path / "claude-config"
    config_dir.mkdir()
    (config_dir / "settings.json").write_text(json.dumps({"permissions": {"allow": ["Bash"]}}))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config_dir))
    runtime, ctx = _read_only_claude_worker(tmp_path)

    try:
        argv, _ = runtime.command(ctx)
    except PermissionError:
        return  # Refusing a pre-existing grant is a valid fail-closed policy.

    sources = argv[argv.index("--setting-sources") + 1].split(",") if "--setting-sources" in argv else []
    available = argv[argv.index("--tools") + 1].split(",") if "--tools" in argv else None
    denied = argv[argv.index("--disallowedTools") + 1].split(",") if "--disallowedTools" in argv else []
    assert "user" not in sources or "Bash" in denied or (available is not None and "Bash" not in available)


def test_relative_code_dirs_are_relative_to_node_yaml(tmp_path, monkeypatch):
    """A daemon may start in any cwd; configured code paths must still point to the same project."""
    config_dir = tmp_path / "node"
    config_dir.mkdir()
    config_path = config_dir / "node.yaml"
    config_path.write_text("""project: p
node: C
agents:
  - id: paper-visualrl
    runtime: claude-code
    workdir: ../work/paper/visualrl
    code_mode: direct
    code_dirs: [../repos/visualrl]
""")
    elsewhere = tmp_path / "other" / "cwd"
    elsewhere.mkdir(parents=True)
    monkeypatch.chdir(elsewhere)

    agent = load_config(config_path).agents[0]
    assert agent.code_paths == [(config_dir / "../repos/visualrl").resolve()]


def test_the_worker_prompt_says_the_task_is_already_accepted(tmp_path):
    _, ctx, _ = _claude_ctx(tmp_path)
    assert "do not call accept_task" in worker_prompt(ctx)


def test_the_worker_prompt_says_how_to_run_git(tmp_path):
    """The pilot's worker ran 'cd <worktree> && git ...', which Bash(git:*) does not match."""
    _, ctx, _ = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    ctx.git_branch = "mutmuas/C/paper-visualrl/T-1"
    prompt = worker_prompt(ctx)
    assert "git -C <dir>" in prompt and "cd <dir> && git" in prompt


def test_agents_json_shows_auto_worker():
    """The card carries auto_worker (CARD_KEYS); the summary agents and find_agent print dropped it."""
    card = {"address": "B:desk", "mode": "interactive", "auto_worker": True}
    assert tools.card_summary(card)["auto_worker"] is True


def test_llm_workers_get_skill(tmp_path):
    """The pilot showed a worker uses project skills once Skill is in --tools (it was not by default)."""
    runtime, ctx, _ = _claude_ctx(tmp_path)
    argv, _ = runtime.command(ctx)
    assert "Skill" in argv[argv.index("--tools") + 1].split(",")
    assert "Skill" in argv[argv.index("--allowedTools") + 1].split(",")


def _claude(tmp_path, agent_extra=None, node_extra=None):
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import request_body
    from mutmuas.runtime import ClaudeCodeRuntime, TaskContext
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path / "data"), **(node_extra or {}))
    agent = AgentConfig(id="w", runtime="claude-code", workdir=str(tmp_path), **(agent_extra or {}))
    req = Envelope(type="REQUEST", sender="A:m", to="C:w", task_id="T-1", body=request_body("x", "y"))
    return ClaudeCodeRuntime(agent, node), TaskContext("T-1", req, agent, node)


def _flag(argv, name):
    return argv[argv.index(name) + 1] if name in argv else None


def test_worker_limits_come_from_the_node_default_or_the_agent(tmp_path):
    runtime, ctx = _claude(tmp_path)
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") is None and _flag(argv, "--max-budget-usd") is None
    runtime, ctx = _claude(tmp_path, node_extra={"worker_max_turns": 150, "worker_max_cost_usd": 15})
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") == "150" and _flag(argv, "--max-budget-usd") == "15"
    runtime, ctx = _claude(tmp_path, {"max_turns": 40, "max_cost_usd": 2.5}, {"worker_max_turns": 150})
    argv, _ = runtime.command(ctx)
    assert _flag(argv, "--max-turns") == "40" and _flag(argv, "--max-budget-usd") == "2.5"


@pytest.mark.parametrize("subtype", ["error_max_turns", "error_max_budget_usd"])
def test_the_claude_result_names_the_limit_it_hit(tmp_path, subtype):
    runtime, ctx = _claude(tmp_path)
    tail = json.dumps({"type": "result", "subtype": subtype, "is_error": True, "num_turns": 41,
                       "total_cost_usd": 3.2, "session_id": "s"})
    outcome = runtime.parse(ctx, 0, tail)
    assert outcome.limit == subtype


async def test_a_run_stopped_at_a_limit_is_a_failed_run_laid_out_once_more(tmp_path, monkeypatch):
    """R5.4: recorded in the failures, run once more; the second time the task fails."""
    from mutmuas import node as node_module
    from mutmuas.runtime import RunOutcome
    agent, ledger, daemon = _node(tmp_path)
    owned_task(ledger, "T-l", "ACCEPTED", ingest=True)

    class Limited:
        def __init__(self, *_):
            pass

        async def run(self, ctx):
            return RunOutcome(0, "stopped", limit="error_max_turns")
    monkeypatch.setattr(node_module, "make_runtime", Limited)
    try:
        await daemon._execute(agent, "T-l")
        assert ledger.task("T-l", "owner")["status"] == "ACCEPTED" and "T-l" in daemon._retry
        [failure] = ledger.failures()
        assert "error_max_turns" in failure["error"]
        daemon._retry.clear()
        await daemon._execute(agent, "T-l")
        task = ledger.task("T-l", "owner")
        assert task["status"] == "FAILED" and "error_max_turns" in task["result"]["summary"]
    finally:
        ledger.close()


def test_the_worker_prompt_sends_long_commands_to_the_background(tmp_path):
    """D-104 item 6: a command expected to run longer than N minutes (node.yaml background_after_min, default 10)
    is never run in the foreground of a worker run; Claude Code's own settings are left alone."""
    _, ctx, _ = _claude_ctx(tmp_path)
    prompt = worker_prompt(ctx)
    assert "longer than 10 minutes" in prompt and "start_job" in prompt and "foreground" in prompt
    ctx.node.background_after_min = 30
    assert "longer than 30 minutes" in worker_prompt(ctx)
