"""Agent runtimes: how a worker agent actually executes a task.

The messaging core never talks to Claude/Codex directly; it hands a
``TaskContext`` to a runtime and gets a ``RunOutcome`` back. Every runtime is a
subprocess, so the shared base class handles timeouts, cancellation (whole
process group), and logs; subclasses only build the command line.

Inside the subprocess the agent reaches the system through the ``mutmuas`` MCP
server (LLM agents) or the ``agentctl`` CLI (scripts). Both read
MUTMUAS_CONFIG / MUTMUAS_AGENT / MUTMUAS_TASK_ID from the environment, so
``submit_result`` binds to the right task automatically.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import AgentConfig, NodeConfig
from .protocol import Envelope

log = logging.getLogger(__name__)

TAIL_BYTES = 64 * 1024


@dataclass
class TaskContext:
    task_id: str
    request: Envelope
    agent: AgentConfig
    node: NodeConfig
    attempt: int = 1
    workdir: Path | None = None          # isolated git worktree for code tasks
    git_branch: str | None = None

    @property
    def cwd(self) -> Path:
        return self.workdir or self.agent.workdir_path

    def allows(self, permission: str) -> bool:
        """Least privilege per task: the agent's permission AND one this kind of request needs.

        A query or artifact request runs read-only even on an agent that could write, so a question
        can never modify the checkout the agent (or its node daemon) lives in.
        """
        kind = self.request.body.get("kind", "query")
        needed = {"code": {"WRITE_WORKTREE", "RUN_EXPERIMENT"}, "experiment": {"RUN_EXPERIMENT", "WRITE_WORKTREE"}}
        return permission in needed.get(kind, set()) and self.agent.has(permission)

    @property
    def address(self) -> str:
        return f"{self.node.node}:{self.agent.id}"

    def env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.update(self.agent.env)
        env.update(MUTMUAS_CONFIG=str(self.node.path or ""), MUTMUAS_AGENT=self.address,
                   MUTMUAS_TASK_ID=self.task_id, MUTMUAS_PROJECT=self.node.project)
        return env

    def payload(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "agent": self.address, "attempt": self.attempt,
                "workdir": str(self.cwd), "git_branch": self.git_branch, "request": self.request.to_dict()}


@dataclass
class RunOutcome:
    exit_code: int
    output_tail: str
    result: dict[str, Any] | None = None       # parsed from the agent's final output, if any
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    session_id: str | None = None
    log_path: str | None = None


class SubprocessRuntime:
    name = "subprocess"

    def __init__(self, agent: AgentConfig, node: NodeConfig):
        self.agent = agent
        self.node = node

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        raise NotImplementedError

    def parse(self, ctx: TaskContext, exit_code: int, tail: str) -> RunOutcome:
        return RunOutcome(exit_code, tail, result=_last_json(tail))

    async def run(self, ctx: TaskContext) -> RunOutcome:
        argv, stdin = self.command(ctx)
        runs = self.node.data_path / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        log_path = runs / f"{ctx.task_id}.attempt{ctx.attempt}.log"
        workdir = ctx.cwd
        workdir.mkdir(parents=True, exist_ok=True)
        log.info("task %s: starting %s in %s", ctx.task_id, argv[0], workdir)
        with open(log_path, "wb") as logf:
            logf.write(f"$ {' '.join(argv)}\n".encode())
            logf.flush()
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=str(workdir), env=ctx.env(), stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=logf, start_new_session=True)
            tail = bytearray()
            try:
                if stdin is not None:
                    proc.stdin.write(stdin)
                    await proc.stdin.drain()
                proc.stdin.close()
                while chunk := await proc.stdout.read(65536):
                    logf.write(chunk)
                    logf.flush()
                    tail.extend(chunk)
                    del tail[:-TAIL_BYTES]
                code = await proc.wait()
            except BaseException:          # timeout, cancel, shutdown: never leave orphans behind
                await _kill_group(proc)
                raise
        outcome = self.parse(ctx, code, tail.decode(errors="replace"))
        outcome.log_path = str(log_path)
        return outcome


class ScriptRuntime(SubprocessRuntime):
    """Any executable. Gets the task JSON on stdin; may print a RESULT body as its last JSON line."""

    name = "script"

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        argv = [sys.executable if a == "{python}" else a for a in self.agent.command]
        return argv + list(self.agent.extra_args), json.dumps(ctx.payload()).encode()


def _mcp_server_spec(ctx: TaskContext) -> dict[str, Any]:
    return {"command": sys.executable, "args": ["-m", "mutmuas.cli", "mcp"],
            "env": {"MUTMUAS_CONFIG": str(ctx.node.path or ""), "MUTMUAS_AGENT": ctx.address,
                    "MUTMUAS_TASK_ID": ctx.task_id}}


def worker_prompt(ctx: TaskContext) -> str:
    req = ctx.request
    git_note = (f"\nYou are in an isolated git worktree on branch {ctx.git_branch}. Commit your changes there "
                "(git add + git commit); only committed work is delivered, as a patch.\n") if ctx.git_branch else ""
    return f"""You are {ctx.address} (role: {ctx.agent.role or ctx.agent.id}) in the mutmuas multi-agent system,
project "{ctx.node.project}", running on node {ctx.node.node}. Another agent delegated a task to you.

Task id: {ctx.task_id}   (attempt {ctx.attempt})
From: {req.sender}
REQUEST:
{json.dumps(req.body, indent=2, ensure_ascii=False)}
Attached artifact references: {json.dumps([a.to_dict() for a in req.artifacts], ensure_ascii=False)}

You have the MCP server "mutmuas" with tools: report_progress, publish_artifact, fetch_artifact,
submit_result, list_agents, find_agent, send_request, wait_for_result, check_task.

{git_note}
Rules:
1. Work only inside your working directory ({ctx.cwd}) unless the request says otherwise.
2. Never paste large data into text. Put files/datasets/logs into artifacts with publish_artifact
   and pass the returned references to submit_result.
3. Call report_progress for meaningful milestones of long work.
4. Finish by calling submit_result exactly once. status must be honest:
   complete = every acceptance criterion met; partial = some output but not all criteria;
   failed = nothing usable. Never report partial work as complete. List limitations.
   Back your claims with evidence items {{claim, how, verified, source}}: 'how' is the command, test or
   file:line you checked, so the requester can repeat it; set verified=false for what you did not check.
   A complete code/experiment/artifact result without a verified item is downgraded to partial.
5. If you need something only the requester can provide, say so in follow_up and use status partial or failed.
"""


class ClaudeCodeRuntime(SubprocessRuntime):
    name = "claude-code"

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        cfg_path = self.node.data_path / "runs" / f"{ctx.task_id}.mcp.json"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(json.dumps({"mcpServers": {"mutmuas": _mcp_server_spec(ctx)}}))
        tools = ["mcp__mutmuas", "Read", "Glob", "Grep"]
        if ctx.allows("WRITE_WORKTREE"):
            tools += ["Edit", "Write", "Bash(git:*)"]
        if ctx.allows("RUN_EXPERIMENT"):
            tools += ["Bash"]
        argv = ["claude", "-p", "--output-format", "json", "--mcp-config", str(cfg_path), "--strict-mcp-config",
                "--allowedTools", ",".join(tools)]
        if self.agent.model:
            argv += ["--model", self.agent.model]
        return argv + list(self.agent.extra_args), worker_prompt(ctx).encode()

    def parse(self, ctx: TaskContext, exit_code: int, tail: str) -> RunOutcome:
        data = _last_json(tail) or {}
        text = data.get("result") if isinstance(data.get("result"), str) else tail
        return RunOutcome(exit_code, text[-4000:], result=_last_json(text) if text else None,
                          session_id=data.get("session_id"))


class CodexRuntime(SubprocessRuntime):
    name = "codex"

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        spec = _mcp_server_spec(ctx)
        env_toml = "{" + ", ".join(f'{k} = {json.dumps(v)}' for k, v in spec["env"].items()) + "}"
        writable = ctx.allows("WRITE_WORKTREE") or ctx.allows("RUN_EXPERIMENT")
        argv = ["codex", "exec", "--skip-git-repo-check", "-C", str(ctx.cwd),
                *([] if self.agent.inherit_user_config else ["--ignore-user-config"]),
                "--sandbox", "workspace-write" if writable else "read-only",
                "-c", 'approval_policy="never"',
                "-c", f"mcp_servers.mutmuas.command={json.dumps(spec['command'])}",
                "-c", f"mcp_servers.mutmuas.args={json.dumps(spec['args'])}",
                "-c", f"mcp_servers.mutmuas.env={env_toml}",
                # exec mode cannot prompt; pre-approve only our own server's tools
                "-c", 'mcp_servers.mutmuas.default_tools_approval_mode="approve"',
                *(["-c", "sandbox_workspace_write.network_access=true"] if writable and self.agent.network else [])]
        if self.agent.model:
            argv += ["-m", self.agent.model]
        return argv + list(self.agent.extra_args) + ["-"], worker_prompt(ctx).encode()


RUNTIMES = {"script": ScriptRuntime, "claude-code": ClaudeCodeRuntime, "codex": CodexRuntime}


def make_runtime(agent: AgentConfig, node: NodeConfig) -> SubprocessRuntime:
    return RUNTIMES[agent.runtime](agent, node)


def _last_json(text: str) -> dict[str, Any] | None:
    """The last line (or fenced block) of text that parses as a JSON object."""
    if not text:
        return None
    candidates: list[str] = []
    if "```" in text:
        for block in text.split("```")[1::2]:
            candidates.append(block.removeprefix("json").strip())
    candidates.extend(line.strip() for line in text.splitlines() if line.strip().startswith("{"))
    candidates.append(text.strip())
    for cand in reversed(candidates):
        try:
            value = json.loads(cand)
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


async def _kill_group(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    for sig, wait in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(asyncio.shield(proc.wait()), wait)
            return
        except asyncio.TimeoutError:
            continue
