"""Node configuration (one YAML file per machine).

See config/example-node-A.yaml for an annotated example.
"""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .ids import Address, InvalidAddress, check_token

PERMISSIONS = ("READ", "WRITE_WORKTREE", "RUN_EXPERIMENT", "PUBLISH_ARTIFACT", "REQUEST_TASK", "MERGE", "ADMIN")
RUNTIMES = ("claude-code", "codex", "script")
MODES = ("worker", "interactive")

DEFAULT_HOME = Path(os.environ.get("MUTMUAS_HOME", "~/.mutmuas")).expanduser()


class ConfigError(ValueError):
    pass


@dataclass
class AgentConfig:
    id: str
    mode: str = "worker"                 # worker: the node daemon runs incoming tasks itself
                                         # interactive: a human-driven session picks tasks up via MCP/CLI
    runtime: str | None = None           # claude-code | codex | script (worker mode only)
    role: str = ""
    display: str = ""                    # human alias, e.g. "B:a1"
    provider: str = ""
    model: str = ""
    description: str = ""
    workdir: str = "."
    repo: str = ""                       # git repo for kind=code tasks (each task gets its own worktree)
    capabilities: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=lambda: ["READ", "REQUEST_TASK"])
    accept_from: list[str] = field(default_factory=lambda: ["*"])   # glob patterns over "NODE:agent"
    max_concurrent: int = 1
    task_timeout_s: float = 3600
    max_attempts: int = 2                # how often a task is (re)started after crashes before FAILED
    command: list[str] = field(default_factory=list)                # script runtime
    extra_args: list[str] = field(default_factory=list)             # appended to the runtime CLI call
    inherit_user_config: bool = False    # codex: also load ~/.codex/config.toml (model, other MCP servers)
    network: bool = False                # codex: allow network access inside the workspace-write sandbox
    notify: list[str] = field(default_factory=list)   # e.g. ["B:main"]: told whenever this worker takes or
                                                      # finishes a task, so a node's lead knows what runs there
    env: dict[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        check_token(self.id, "agent id")
        if self.mode not in MODES:
            raise ConfigError(f"agent {self.id}: mode must be one of {MODES}")
        if self.mode == "worker":
            if self.runtime not in RUNTIMES:
                raise ConfigError(f"agent {self.id}: worker agents need runtime in {RUNTIMES}")
            if self.runtime == "script" and not self.command:
                raise ConfigError(f"agent {self.id}: script runtime needs 'command'")
        bad = [p for p in self.permissions if p not in PERMISSIONS]
        if bad:
            raise ConfigError(f"agent {self.id}: unknown permission(s) {bad}; known: {PERMISSIONS}")

    def has(self, permission: str) -> bool:
        return "ADMIN" in self.permissions or permission in self.permissions

    def accepts(self, sender: str) -> bool:
        return any(fnmatch.fnmatchcase(sender, pattern) for pattern in self.accept_from)

    @property
    def workdir_path(self) -> Path:
        return Path(os.path.expandvars(self.workdir)).expanduser().resolve()


@dataclass
class NatsConfig:
    servers: list[str] = field(default_factory=lambda: ["nats://127.0.0.1:4222"])
    user: str | None = None
    password: str | None = None
    password_env: str | None = None      # prefer keeping secrets out of the YAML
    token: str | None = None
    token_env: str | None = None
    credentials_file: str | None = None  # KEY=VALUE file from `agent-node server-config` (user + password)
    tls_ca: str | None = None
    tls_cert: str | None = None
    tls_key: str | None = None

    def __post_init__(self) -> None:
        if self.credentials_file:
            creds = read_env_file(Path(self.credentials_file).expanduser())
            self.user = self.user or creds.get("MUTMUAS_NATS_USER")
            self.password = self.password or creds.get("MUTMUAS_NATS_PASSWORD")

    def resolved_password(self) -> str | None:
        return self.password or (os.environ.get(self.password_env) if self.password_env else None)

    def resolved_token(self) -> str | None:
        return self.token or (os.environ.get(self.token_env) if self.token_env else None)


@dataclass
class NodeConfig:
    project: str
    node: str
    nats: NatsConfig = field(default_factory=NatsConfig)
    data_dir: str = ""
    description: str = ""
    resources: dict[str, Any] = field(default_factory=dict)
    heartbeat_s: float = 5.0
    message_retention_days: float = 30
    artifact_max_mb: float = 2048         # upload cap for the NATS object store backend
    agents: list[AgentConfig] = field(default_factory=list)
    path: Path | None = None              # where this config was loaded from

    def validate(self) -> NodeConfig:
        check_token(self.project, "project")
        check_token(self.node, "node id")
        seen = set()
        for agent in self.agents:
            agent.validate()
            if agent.id in seen:
                raise ConfigError(f"duplicate agent id {agent.id!r}")
            seen.add(agent.id)
        return self

    @property
    def data_path(self) -> Path:
        base = self.data_dir or str(DEFAULT_HOME / self.project / self.node)
        path = Path(os.path.expandvars(base)).expanduser()
        if not path.is_absolute() and self.path is not None:
            path = self.path.parent / path
        return path.resolve()

    @property
    def db_path(self) -> Path:
        return self.data_path / "ledger.sqlite3"

    def agent(self, agent_id: str) -> AgentConfig:
        for agent in self.agents:
            if agent.id == agent_id:
                return agent
        raise ConfigError(f"agent {agent_id!r} is not configured on node {self.node}")

    def address(self, agent_id: str) -> Address:
        return Address(self.node, self.agent(agent_id).id)


def _build(cls, raw: dict[str, Any], where: str):
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    known = cls.__dataclass_fields__
    unknown = sorted(set(raw) - set(known))
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {unknown}")
    return cls(**raw)


def load_config(path: str | Path) -> NodeConfig:
    path = Path(path).expanduser().resolve()
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    raw = yaml.safe_load(path.read_text()) or {}

    def rel(value: str | None) -> str | None:
        # Relative paths are relative to the config file, not to wherever a service manager starts us.
        if not value:
            return value
        p = Path(os.path.expandvars(value)).expanduser()
        return str(p if p.is_absolute() else (path.parent / p).resolve())

    raw_agents = raw.pop("agents", []) or []
    for a in raw_agents:
        if isinstance(a, dict):
            a["workdir"] = rel(a.get("workdir", "."))
            if a.get("repo"):
                a["repo"] = rel(a["repo"])
    raw_nats = raw.pop("nats", {}) or {}
    for key in ("credentials_file", "tls_ca", "tls_cert", "tls_key"):
        if isinstance(raw_nats, dict) and raw_nats.get(key):
            raw_nats[key] = rel(raw_nats[key])
    agents = [_build(AgentConfig, a, f"agents[{i}]") for i, a in enumerate(raw_agents)]
    nats = _build(NatsConfig, raw_nats, "nats")
    cfg = _build(NodeConfig, raw, str(path))
    cfg.agents, cfg.nats, cfg.path = agents, nats, path
    try:
        return cfg.validate()
    except InvalidAddress as e:
        raise ConfigError(f"{path}: {e}") from e


def find_config(explicit: str | None = None) -> Path:
    """--config flag > $MUTMUAS_CONFIG > ~/.mutmuas/node.yaml"""
    candidate = explicit or os.environ.get("MUTMUAS_CONFIG") or str(DEFAULT_HOME / "node.yaml")
    return Path(candidate).expanduser()


def read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ConfigError(f"credentials file not found: {path}")
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip()
    return out


def dump_config(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
