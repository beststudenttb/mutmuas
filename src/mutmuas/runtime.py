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
import contextlib
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    on_spawn: Any = None                 # called with the process id once the agent process exists
    jobs: list[dict[str, Any]] = field(default_factory=list)   # background jobs of this task that have ended
    resume: str | None = None            # a brain batch's conversation to continue (D-073)
    interrupts: list[str] = field(default_factory=list)   # what interrupted the previous run (D-089)
    session_id: str | None = None        # a new brain conversation's id, chosen by the node before the run

    @property
    def internal(self) -> bool:
        """An internal subtask (D-073): a brain's own long work, run by its worker."""
        return bool(self.request.body.get("internal"))

    @property
    def home(self) -> Path:
        """The directory of this task's project (D-069), or the post directory without one."""
        return self.agent.home(self.agent.project_of(self.request.body))

    @property
    def cwd(self) -> Path:
        return self.workdir or self.home

    def allows(self, permission: str) -> bool:
        """Tools follow the post's permissions alone, whatever the kind (D-064): a query on a post that may write
        can keep its log and PLAN.md (r20). The kind only shapes the prompt and whether a code task gets a
        worktree; a post whose workdir must stay untouched does not get WRITE_WORKTREE/RUN_EXPERIMENT."""
        return self.agent.has(permission)

    @property
    def address(self) -> str:
        return f"{self.node.node}:{self.agent.id}"

    def env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.update(self.agent.env)
        env.update(MUTMUAS_CONFIG=str(self.node.path or ""), MUTMUAS_AGENT=self.address,
                   MUTMUAS_TASK_ID=self.task_id, MUTMUAS_PROJECT=self.node.project,
                   CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD="1")   # the project code's own CLAUDE.md
        # (the documented files are CLAUDE.md, .claude/CLAUDE.md, .claude/rules/*.md, CLAUDE.local.md; an
        # AGENTS.md is read only if one of them imports it)
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
    limit: str | None = None                   # the run was stopped at a worker limit (e.g. "error_max_turns")


class SubprocessRuntime:
    name = "subprocess"

    def __init__(self, agent: AgentConfig, node: NodeConfig):
        self.agent = agent
        self.node = node

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        raise NotImplementedError

    def start_dir(self, ctx: TaskContext) -> Path:
        return ctx.cwd

    def parse(self, ctx: TaskContext, exit_code: int, tail: str) -> RunOutcome:
        return RunOutcome(exit_code, tail, result=_last_json(tail))

    async def run(self, ctx: TaskContext) -> RunOutcome:
        argv, stdin = self.command(ctx)
        runs = self.node.data_path / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        # a new name for every run: a task woken after a background job starts again at attempt 1 (r19 F1)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        log_path = runs / f"{ctx.task_id}.{stamp}.attempt{ctx.attempt}.log"
        workdir = self.start_dir(ctx)
        workdir.mkdir(parents=True, exist_ok=True)
        if ctx.internal:
            (ctx.home / "runs" / ctx.task_id).mkdir(parents=True, exist_ok=True)     # a sub's outputs (D-073)
        log.info("task %s: starting %s in %s", ctx.task_id, argv[0], workdir)
        with open(log_path, "wb") as logf:
            logf.write(f"$ {' '.join(argv)}\n".encode())
            logf.flush()
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=str(workdir), env=ctx.env(), stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=logf, start_new_session=True)
            if ctx.on_spawn:
                ctx.on_spawn(proc.pid)
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
    # --worker-task: this MCP server serves a daemon-run task, not a session (it must not hold the session)
    return {"command": sys.executable, "args": ["-m", "mutmuas.cli", "mcp", "--worker-task", ctx.task_id],
            "env": {"MUTMUAS_CONFIG": str(ctx.node.path or ""), "MUTMUAS_AGENT": ctx.address,
                    "MUTMUAS_TASK_ID": ctx.task_id}}


def llm_start_dir(ctx: TaskContext) -> Path:
    """An LLM worker starts in its project directory, where the function CLAUDE.md and the project's memory are
    (D-031), and reaches a code task's worktree through extra_dirs. Legacy: a workdir inside the repo would open
    the main repo to the task (the b461307 MERGE bypass), so such an agent still starts in the worktree."""
    if ctx.workdir is not None and ctx.agent.repo:
        repo = Path(os.path.realpath(Path(os.path.expandvars(ctx.agent.repo)).expanduser()))
        if Path(os.path.realpath(ctx.agent.workdir_path)).is_relative_to(repo):
            return ctx.workdir
    return ctx.home


def extra_dirs(ctx: TaskContext) -> list[Path]:
    """The code an LLM worker may reach besides its start directory: the task's worktree (copy mode) or the
    project code it edits in place (direct mode)."""
    dirs = [ctx.workdir] if ctx.workdir is not None and llm_start_dir(ctx) != ctx.workdir else []
    return dirs + (ctx.agent.code_paths if ctx.agent.code_mode == "direct" else [])


def sub_prompt(ctx: TaskContext) -> str:
    """An internal subtask (D-073): one piece of its brain's work, no mail, no plan of its own."""
    req = ctx.request
    out = ctx.home / "runs" / ctx.task_id
    return f"""You are a sub-worker of {ctx.address} in the mutmuas multi-agent system (project "{ctx.node.project}",
node {ctx.node.node}). Your brain gave you one piece of its work.

Task id: {ctx.task_id}   (attempt {ctx.attempt})
TASK:
{json.dumps(req.body, indent=2, ensure_ascii=False)}

You have the MCP server "mutmuas" with three tools, all for this task only: report_progress, submit_result, add_job.
Rules:
1. Work in {llm_start_dir(ctx)}. Write your outputs (files, logs, results) under {out}.
2. Do not edit PLAN.md or HANDOFF.md: the node marks your line on your brain's plan when you report progress
   and when you finish. Do not write worker-log.md either.
3. Call report_progress for meaningful milestones of long work.
4. Finish by calling submit_result exactly once, as the very last step. status must be honest: complete = every
   acceptance criterion met; partial = some output but not all; failed = nothing usable. Put the paths of your
   outputs and the key numbers in it; how you did it in `how`, anything worth noting in `notes`.
5. Work that runs long (e.g. training): start it detached, `nohup <command> > <log> 2>&1 < /dev/null &` (on a GPU
   machine through gpu-run), register it with add_job (pid and done_file, log, a one-line note) and end this run
   without submit_result. You are started again when the job ends.
"""


def interrupt_note(ctx: TaskContext) -> str:
    """The messages that interrupted (or paused and resumed) this task's previous run (D-089): first thing to read."""
    if not ctx.interrupts:
        return ""
    lines = "".join(f"\n- {text}" for text in ctx.interrupts)
    return (f"\nYour previous run of this task was interrupted (D-089) by the message(s) below. Act on them first: "
            f"they may stop or change what you were doing.{lines}\n")


def worker_prompt(ctx: TaskContext) -> str:
    return interrupt_note(ctx) + _worker_prompt(ctx)


def _worker_prompt(ctx: TaskContext) -> str:
    if ctx.internal:
        return sub_prompt(ctx)
    req = ctx.request
    code = extra_dirs(ctx)
    git_note = (f"\nThe code for this task is in an isolated git worktree{' at ' + str(ctx.workdir) if code else ''} "
                f"on branch {ctx.git_branch}. Commit your changes there (git -C <worktree> add + commit); only "
                "committed work is delivered, as a patch.\n") if ctx.git_branch else ""
    if ctx.agent.code_mode == "direct" and code:
        git_note += (f"\nThe project code is in {', '.join(map(str, code))}. Edit it in place and commit promptly "
                     "with a clear message (git -C <dir>); a mistake is undone with git.\n")
    if git_note:
        git_note += ("Run git as `git -C <dir> ...`: only commands that start with git are allowed, so "
                     "`cd <dir> && git ...` is refused.\n")
    jobs = "".join(f"\n- job {j['job_id']} ({j['note'] or '-'}): {j['ended']}; log {j['log'] or '-'}"
                   for j in ctx.jobs)
    if jobs:
        jobs = ("\nThis task is being resumed: background jobs you registered have ended. Read your PLAN.md and "
                f"the logs, judge whether each job succeeded, and carry on.{jobs}\n")
    return f"""You are {ctx.address} (role: {ctx.agent.role or ctx.agent.id}) in the mutmuas multi-agent system,
project "{ctx.node.project}", running on node {ctx.node.node}. Another agent delegated a task to you.

Task id: {ctx.task_id}   (attempt {ctx.attempt})
From: {req.sender}
REQUEST:
{json.dumps(req.body, indent=2, ensure_ascii=False)}
Attached artifact references: {json.dumps([a.to_dict() for a in req.artifacts], ensure_ascii=False)}

You have the MCP server "mutmuas" with tools: report_progress, publish_artifact, fetch_artifact,
submit_result, list_agents, find_agent, send_request, wait_for_result, check_task, add_job.
{jobs}
{git_note}
Rules:
1. Work only inside your working directory ({llm_start_dir(ctx)}){' and ' + ', '.join(map(str, code)) if code else ''}
   unless the request says otherwise. Keep PLAN.md and HANDOFF.md there up to date as a session would
   (R7.11, D-066). Before every run ends (before submit_result, and before ending to wait on a job or on
   subtasks), rewrite HANDOFF.md's current state: what you did, what comes next and whom you wait for. A later
   run may start afresh from it. Do not write worker-log.md: the node writes the line when the task is delivered.
2. Never paste large data into text. Put files/datasets/logs into artifacts with publish_artifact
   and pass the returned references to submit_result.
3. Call report_progress for meaningful milestones of long work.
4. Finish by calling submit_result exactly once, as the very last step; put how you did it in `how` and
   anything worth noting in `notes`. The node writes the worker-log.md line and hands over this task's section
   of PLAN.md (the heading with the task id), if there is one. status must be honest:
   complete = every acceptance criterion met; partial = some output but not all criteria;
   failed = nothing usable. Never report partial work as complete. List limitations.
5. If you need something only the requester can provide, say so in follow_up and use status partial or failed.
6. This task was accepted for you when this run started: do not call accept_task.
7. Work that runs long (e.g. training): start it detached, `nohup <command> > <log> 2>&1 < /dev/null &`
   (then `echo $!` is its pid), register it with add_job (pid and/or done_file, log, a one-line note), note in
   PLAN.md what you wait for, and end this run without submit_result. You are started again when the job ends.
8. Parts you delegate with send_request are this task's child tasks (parent_task is set for you). To wait for
   them, call add_job(children=True) and end this run without submit_result: you are started again once
   each has a result, was refused or cancelled, or is past its deadline, and told how each ended.
"""


class ClaudeCodeRuntime(SubprocessRuntime):
    name = "claude-code"

    @staticmethod
    def request_model(ctx: TaskContext) -> str | None:
        return ctx.request.body.get("model")

    def start_dir(self, ctx: TaskContext) -> Path:
        return llm_start_dir(ctx)

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        cfg_path = self.node.data_path / "runs" / f"{ctx.task_id}.mcp.json"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(json.dumps({"mcpServers": {"mutmuas": _mcp_server_spec(ctx)}}))
        # Skill: the project's skills (pilot 2026-09-29: without it in --tools a worker could not use them)
        tools = ["mcp__mutmuas", "Read", "Glob", "Grep", "Skill"]   # pre-approved (--allowedTools)
        available = ["Read", "Glob", "Grep", "Skill"]                # all that exists (--tools; MCP tools stay)
        if ctx.allows("WRITE_WORKTREE"):
            tools += ["Edit", "Write", "Bash(git:*)"]
            available += ["Edit", "Write", "Bash"]
        if ctx.allows("RUN_EXPERIMENT"):
            tools += ["Bash"]
            available += [] if "Bash" in available else ["Bash"]
        # Tools come from node.yaml alone (D-032; Codex review of 6c2a60a): --tools limits what exists, since
        # --allowedTools only pre-approves. Setting source "project" only: not "user" (its allow rules and
        # plugin hooks) nor "local" (a session's "don't ask again" approvals); "project" still loads the
        # function CLAUDE.md above the project directory and the project's memory.
        argv = ["claude", "-p", "--output-format", "json", "--mcp-config", str(cfg_path), "--strict-mcp-config",
                "--setting-sources", "project",
                "--tools", ",".join(available), "--allowedTools", ",".join(tools)]
        for d in extra_dirs(ctx):
            argv += ["--add-dir", str(d)]
        model = (self.request_model(ctx) or "sonnet") if ctx.internal else self.agent.model   # a sub: latest Sonnet
        if model:
            argv += ["--model", model]
        if ctx.resume:
            argv += ["--resume", ctx.resume]
        elif ctx.session_id:
            argv += ["--session-id", ctx.session_id]       # named up front: a stopped run can still be resumed
        # Worker limits (D-066): the agent's own, else the node's default; --max-turns is accepted by claude -p
        # although its --help does not list it (checked in the 2.1.285 binary)
        turns = self.agent.max_turns if self.agent.max_turns is not None else ctx.node.worker_max_turns
        cost = self.agent.max_cost_usd if self.agent.max_cost_usd is not None else ctx.node.worker_max_cost_usd
        if turns is not None:
            argv += ["--max-turns", str(turns)]
        if cost is not None:
            argv += ["--max-budget-usd", f"{cost:g}"]
        return argv + list(self.agent.extra_args), worker_prompt(ctx).encode()

    def parse(self, ctx: TaskContext, exit_code: int, tail: str) -> RunOutcome:
        data = _last_json(tail) or {}
        text = data.get("result") if isinstance(data.get("result"), str) else tail
        limit = data.get("subtype") if data.get("subtype") in ("error_max_turns", "error_max_budget_usd") else None
        return RunOutcome(exit_code, text[-4000:], result=_last_json(text) if text else None,
                          session_id=data.get("session_id"), limit=limit)


class CodexRuntime(SubprocessRuntime):
    name = "codex"

    def start_dir(self, ctx: TaskContext) -> Path:
        return llm_start_dir(ctx)

    def command(self, ctx: TaskContext) -> tuple[list[str], bytes | None]:
        spec = _mcp_server_spec(ctx)
        env_toml = "{" + ", ".join(f'{k} = {json.dumps(v)}' for k, v in spec["env"].items()) + "}"
        writable = ctx.allows("WRITE_WORKTREE") or ctx.allows("RUN_EXPERIMENT")
        argv = ["codex", "exec", "--skip-git-repo-check", "-C", str(self.start_dir(ctx)),
                *[a for d in (extra_dirs(ctx) if writable else []) for a in ("--add-dir", str(d))],
                *([] if self.agent.inherit_user_config else ["--ignore-user-config"]),
                "--sandbox", "workspace-write" if writable else "read-only",
                "-c", 'approval_policy="never"',
                "-c", f"mcp_servers.mutmuas.command={json.dumps(spec['command'])}",
                "-c", f"mcp_servers.mutmuas.args={json.dumps(spec['args'])}",
                "-c", f"mcp_servers.mutmuas.env={env_toml}",
                # exec mode cannot prompt; pre-approve only our own server's tools
                "-c", 'mcp_servers.mutmuas.default_tools_approval_mode="approve"',
                *(["-c", "sandbox_workspace_write.network_access=true"] if writable and self.agent.network else [])]
        model = ctx.request.body.get("model") if ctx.internal else None
        if model or self.agent.model:
            argv += ["-m", model or self.agent.model]
        return argv + list(self.agent.extra_args) + ["-"], worker_prompt(ctx).encode()


RUNTIMES = {"script": ScriptRuntime, "claude-code": ClaudeCodeRuntime, "codex": CodexRuntime}


def make_runtime(agent: AgentConfig, node: NodeConfig) -> SubprocessRuntime:
    return RUNTIMES[agent.runtime](agent, node)


def _last_json(text: str) -> dict[str, Any] | None:
    """The last line of text that starts a JSON object, if it is one (a script's result line, claude -p's output).
    A result is normally submitted through submit_result; this is the fallback."""
    for line in reversed((text or "").splitlines()):
        if line.strip().startswith("{"):
            try:
                value = json.loads(line)
            except ValueError:
                return None
            return value if isinstance(value, dict) else None
    return None


async def stop_group(pgid: int, grace_s: float = 5.0) -> bool:
    """Stop a run's whole process group: SIGTERM, then SIGKILL to whatever is left after grace_s. The group is
    waited on, not just its leader: a child that ignores SIGTERM would keep running and keep its locks (a GPU
    flock) (Codex review of 9f39ff0). True once the group is gone."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if _group_gone(pgid, sig):
            return True
        deadline = time.monotonic() + grace_s
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            if _group_gone(pgid, 0):
                return True
    return False


def _group_gone(pgid: int, sig: int) -> bool:
    """Whether the group is gone after sending it sig. Members that are zombies (dead, not yet reaped by their
    parent) count as gone: Linux lets kill() reach a zombie, macOS answers EPERM for a group of zombies only
    (secretary's run on B, Codex re-review of cb77a33). A live member we may not signal is not gone."""
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return True
    except PermissionError:
        return all(state.startswith("Z") for state in _group_states(pgid))
    return sig == 0 and all(state.startswith("Z") for state in _group_states(pgid))


def _group_states(pgid: int) -> list[str]:
    """The process states (ps STAT) of the members of a process group; ["?"] (unknown, not a zombie) when ps
    cannot run or fails (a sandbox): then the group is not shown gone and the stop counts as failed."""
    try:
        ps = next(p for p in ("/bin/ps", "/usr/bin/ps") if os.path.exists(p))       # by absolute path, as node.py
        done = subprocess.run([ps, "-A", "-o", "pgid=,stat="], capture_output=True, text=True)       # POSIX flags
    except (OSError, subprocess.SubprocessError, StopIteration):
        return ["?"]
    if done.returncode != 0:
        return ["?"]
    out = done.stdout
    return [stat for line in out.splitlines() if len(parts := line.split()) == 2 and parts[0] == str(pgid)
            for stat in [parts[1]]]


async def _kill_group(proc: asyncio.subprocess.Process, grace_s: float = 5.0) -> None:
    if not await stop_group(proc.pid, grace_s):  # the run starts its own session: its group id is its pid
        log.error("could not stop process group %s: some of it still runs", proc.pid)
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(proc.wait()), grace_s)
