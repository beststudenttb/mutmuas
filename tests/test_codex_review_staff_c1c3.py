"""Regression checks for the C1–C3 staff worker review."""

import json

from mutmuas.config import load_config
from mutmuas.runtime import ClaudeCodeRuntime, TaskContext
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.protocol import Envelope, request_body


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
