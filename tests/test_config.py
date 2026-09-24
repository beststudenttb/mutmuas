"""Example configs stay loadable; config errors are caught early; generated server config is valid."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from conftest import ROOT, nats_binary

from mutmuas.config import ConfigError, load_config
from mutmuas.server_config import generate


@pytest.mark.parametrize("name", ["example-node-A.yaml", "example-node-B.yaml"])
def test_example_configs_load(tmp_path, name):
    raw = yaml.safe_load((ROOT / "config" / name).read_text())
    creds = tmp_path / "node.env"
    creds.write_text("MUTMUAS_NATS_USER=node_X\nMUTMUAS_NATS_PASSWORD=secret\n")
    raw["nats"]["credentials_file"] = str(creds)
    path = tmp_path / name
    path.write_text(yaml.safe_dump(raw))
    cfg = load_config(path)
    assert cfg.nats.user == "node_X" and cfg.nats.password == "secret"
    assert all(a.workdir_path.is_absolute() for a in cfg.agents)
    assert any(a.mode == "worker" for a in cfg.agents)


@pytest.mark.parametrize("patch, message", [
    ({"node": "B.1"}, "invalid node id"),
    ({"agents": [{"id": "x", "mode": "worker"}]}, "need runtime"),
    ({"agents": [{"id": "x", "mode": "interactive", "permissions": ["GOD"]}]}, "unknown permission"),
    ({"agents": [{"id": "x", "mode": "interactive"}, {"id": "x", "mode": "interactive"}]}, "duplicate"),
    ({"agnets": []}, "unknown key"),
])
def test_config_errors(tmp_path, patch, message):
    path = tmp_path / "n.yaml"
    path.write_text(yaml.safe_dump({"project": "p", "node": "A", **patch}))
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_relative_paths_resolve_against_config_file(tmp_path):
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "A.env").write_text("MUTMUAS_NATS_USER=node_A\nMUTMUAS_NATS_PASSWORD=pw\n")
    path = tmp_path / "cfg" / "node.yaml"
    path.write_text(yaml.safe_dump({"project": "p", "node": "A", "data_dir": "./data",
                                    "nats": {"credentials_file": "A.env"},
                                    "agents": [{"id": "x", "mode": "interactive", "workdir": "work"}]}))
    cfg = load_config(path)
    assert cfg.data_path == (tmp_path / "cfg" / "data").resolve()
    assert cfg.agents[0].workdir_path == (tmp_path / "cfg" / "work").resolve()
    assert cfg.nats.user == "node_A"


def test_generated_server_config_is_valid(tmp_path):
    written = generate("visual_rl", ["A", "B", "C"], tmp_path, store_dir=str(tmp_path / "js"),
                       tls_hosts=["127.0.0.1", "gpu.example.org"])
    assert set(written) == {"server", "A", "B", "C", "admin", "ca"}
    assert oct(written["A"].stat().st_mode)[-3:] == "600"
    text = written["server"].read_text()
    assert '"mm.visual_rl.msg.*.*.A"' in text and '"mm.visual_rl.msg.*.*.B"' in text
    for conf in (written["server"], generate("p", ["A"], tmp_path / "plain")["server"]):
        out = subprocess.run([nats_binary(), "-c", str(conf), "-t"], capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
    san = subprocess.run(["openssl", "x509", "-in", str(tmp_path / "tls/server.crt"), "-noout", "-ext",
                          "subjectAltName"], capture_output=True, text=True).stdout
    assert "127.0.0.1" in san and "gpu.example.org" in san
    ca_before = written["ca"].read_bytes()
    assert generate("visual_rl", ["A", "B", "C", "D"], tmp_path, tls_hosts=["127.0.0.1"])["ca"].read_bytes() == ca_before


def test_adding_a_node_keeps_existing_credentials(tmp_path):
    first = generate("p", ["A", "B"], tmp_path)
    old = {n: first[n].read_text() for n in ("A", "B", "admin")}
    second = generate("p", ["A", "B", "C"], tmp_path)
    assert {n: second[n].read_text() for n in ("A", "B", "admin")} == old
    assert "node_C" in second["server"].read_text()


def test_service_units_do_not_clobber_other_nodes(tmp_path, monkeypatch):
    """Two nodes on one Mac (A = Claude, C = Codex) get distinct launchd labels; a foreign unit is not overwritten."""
    import sys

    from mutmuas import cli
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(cli.Path, "home", lambda: tmp_path)
    paths = {}
    for node in ("A", "C"):
        cfg = tmp_path / f"{node}.yaml"
        cfg.write_text(yaml.safe_dump({"project": "p", "node": node, "agents": []}))
        cli.agent_node(["service", "--write", "--config", str(cfg)])
        paths[node] = tmp_path / f"Library/LaunchAgents/dev.mutmuas.p.{node}.plist"
        assert str(cfg) in paths[node].read_text()
    assert paths["A"] != paths["C"]

    other = tmp_path / "other" / "A.yaml"          # a different config claiming node A of project p
    other.parent.mkdir()
    other.write_text(yaml.safe_dump({"project": "p", "node": "A", "agents": []}))
    with pytest.raises(SystemExit):
        cli.agent_node(["service", "--write", "--config", str(other)])
    assert str(tmp_path / "A.yaml") in paths["A"].read_text()


@pytest.mark.parametrize("kind, sandbox, claude_can_edit", [
    ("query", "read-only", False), ("artifact", "read-only", False),
    ("code", "workspace-write", True), ("experiment", "workspace-write", True)])
def test_runtime_sandbox_follows_request_kind(tmp_path, kind, sandbox, claude_can_edit):
    """A question never gets a writable sandbox, even on an agent that holds WRITE_WORKTREE."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    from mutmuas.runtime import ClaudeCodeRuntime, CodexRuntime, TaskContext
    node = NodeConfig(project="p", node="C", data_dir=str(tmp_path))
    agent = AgentConfig(id="w", runtime="codex", workdir=str(tmp_path),
                        permissions=["READ", "WRITE_WORKTREE", "RUN_EXPERIMENT", "PUBLISH_ARTIFACT"])
    req = Envelope(type="REQUEST", sender="A:main", to="C:w", task_id="T-1", body=request_body("x", "y", kind=kind))
    ctx = TaskContext("T-1", req, agent, node)
    argv, _ = CodexRuntime(agent, node).command(ctx)
    assert argv[argv.index("--sandbox") + 1] == sandbox
    argv, _ = ClaudeCodeRuntime(agent, node).command(ctx)
    assert ("Edit" in argv[argv.index("--allowedTools") + 1]) is claude_can_edit


def test_bare_init_add_agent_and_watch_unit(tmp_path, monkeypatch):
    """Two assistants share one node: init --bare, then each deploy adds its own agent; notifier units are distinct."""
    import sys

    from mutmuas import cli
    cfg = tmp_path / "A.yaml"
    cli.agent_node(["init", "--bare", "--config", str(cfg), "--project", "p", "--node", "A"])
    cli.agent_node(["add-agent", "--config", str(cfg), "--id", "claude", "--provider", "anthropic"])
    cli.agent_node(["add-agent", "--config", str(cfg), "--id", "codex", "--provider", "openai"])
    cli.agent_node(["add-agent", "--config", str(cfg), "--id", "codex-worker", "--mode", "worker",
                    "--runtime", "codex", "--notify", "A:codex"])
    cli.agent_node(["add-agent", "--config", str(cfg), "--id", "claude", "--role", "ignored"])   # idempotent
    loaded = load_config(cfg)
    assert [a.id for a in loaded.agents] == ["claude", "codex", "codex-worker"]
    assert loaded.agent("claude").role == "" and loaded.agent("codex-worker").notify == ["A:codex"]
    with pytest.raises(SystemExit):                     # an invalid agent never corrupts the config
        cli.agent_node(["add-agent", "--config", str(cfg), "--id", "bad", "--mode", "worker"])
    assert [a.id for a in load_config(cfg).agents] == ["claude", "codex", "codex-worker"]

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(cli.Path, "home", lambda: tmp_path)
    for who in ("claude", "codex"):
        cli.agent_node(["service", "--write", "--watch", who, "--config", str(cfg)])
        plist = (tmp_path / f"Library/LaunchAgents/dev.mutmuas.p.A.watch-{who}.plist").read_text()
        assert "<string>watch</string>" in plist and f"<string>A:{who}</string>" in plist
    with pytest.raises(SystemExit):
        cli.agent_node(["service", "--watch", "B:main", "--config", str(cfg)])


def test_code_task_sandbox_never_gets_the_main_repos_git_dir(tmp_path):
    """b461307 opened <repo>/.git to the sandbox (A:codex found it bypasses MERGE); code tasks now use a private clone."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    from mutmuas.runtime import ClaudeCodeRuntime, CodexRuntime, TaskContext
    node = NodeConfig(project="p", node="A", data_dir=str(tmp_path / "data"))
    agent = AgentConfig(id="w", runtime="codex", workdir=str(tmp_path), repo=str(tmp_path),
                        permissions=["READ", "WRITE_WORKTREE"])
    req = Envelope(type="REQUEST", sender="A:c", to="A:w", task_id="T-1", body=request_body("x", "y", kind="code"))
    ctx = TaskContext("T-1", req, agent, node, workdir=tmp_path / "wt", git_branch="mm/A-w/T-1")
    for runtime in (CodexRuntime, ClaudeCodeRuntime):
        assert "--add-dir" not in runtime(agent, node).command(ctx)[0]
