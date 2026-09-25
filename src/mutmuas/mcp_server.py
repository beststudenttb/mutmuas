"""MCP server exposing the agent-facing tools (stdio).

Run as ``agentctl mcp --as A:a1`` (or with MUTMUAS_AGENT set). Register it in
Claude Code with ``claude mcp add mutmuas -- agentctl mcp --as A:a1`` and in
Codex under ``[mcp_servers.mutmuas]`` — see docs/DEPLOYMENT.md.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.mcpserver import MCPServer

from . import tools
from .config import NodeConfig
from .hub import Hub

INSTRUCTIONS = """You are connected to the mutmuas multi-agent network. Other agents live on other machines
(nodes) and are addressed as NODE:agent (e.g. B:representation). Use these tools instead of asking the
human to relay messages:
- find_agent / list_agents to discover who can do something (by capability or role).
- send_request to delegate work (state objective, reason, expected outputs and acceptance criteria),
  then wait_for_result or check_task. Results carry artifact references; use fetch_artifact to get data.
- inbox shows requests and questions addressed to you. accept_task / reject_task / report_progress /
  submit_result are for tasks you own.
- Large data never goes into messages: publish_artifact and send the reference.
- RESULT status must be honest: complete, partial or failed.
- Accept a task (accept_task) before working on it, so others see you as WORKING on it.
- Handle new messages at safe points (between steps), never by interrupting running work.
  Priority high: handle at the next safe point. CANCEL/ANSWER for your current task: next safe point.
  A new REQUEST while busy: it waits in your inbox; the requester already sees it as delivered."""


def build_server(cfg: NodeConfig, me: str | None) -> MCPServer:
    state: dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(_server):
        hub = await Hub.open(cfg, "mcp", require_bus=False)
        state["hub"] = hub
        state["me"] = str(hub.local_agent(me)[0])
        try:
            yield {}
        finally:
            await hub.close()

    server = MCPServer("mutmuas", instructions=INSTRUCTIONS, lifespan=lifespan)

    def hub() -> Hub:
        return state["hub"]

    def dump(value: Any) -> str:
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)

    @server.tool()
    async def whoami() -> str:
        """Your own address, node and permissions."""
        addr, agent = hub().local_agent(state["me"])
        return dump({"address": str(addr), "project": cfg.project, "role": agent.role, "mode": agent.mode,
                     "permissions": agent.permissions, "capabilities": agent.capabilities})

    @server.tool()
    async def list_agents(capability: str | None = None, online_only: bool = False) -> str:
        """List agents in the network, optionally filtered by capability/role. Best candidates first."""
        return dump(await tools.list_agents(hub(), capability, online_only))

    @server.tool()
    async def find_agent(capability: str) -> str:
        """Find the best agent for a capability or role (e.g. 'isaac_lab', 'gpu_training')."""
        return dump(await tools.find_agent(hub(), capability))

    @server.tool()
    async def send_request(to: str, objective: str, reason: str, kind: str = "query",
                           inputs: dict[str, Any] | None = None, expected_outputs: list[str] | None = None,
                           constraints: list[str] | None = None, acceptance_criteria: list[str] | None = None,
                           timeout_s: float | None = None, artifacts: list[dict[str, Any]] | None = None,
                           priority: str = "normal") -> str:
        """Delegate a task to another agent. kind: query | artifact | experiment | code.
        Returns a task_id; the message is durable even if the target is offline."""
        return dump(await tools.send_request(
            hub(), state["me"], to, objective, reason, kind=kind, inputs=inputs, expected_outputs=expected_outputs,
            constraints=constraints, acceptance_criteria=acceptance_criteria, timeout_s=timeout_s,
            artifacts=artifacts, priority=priority))

    @server.tool()
    async def check_task(task_id: str) -> str:
        """Current state of a task, its result if finished, and its latest messages."""
        return dump(await tools.check_task(hub(), task_id))

    @server.tool()
    async def wait_for_result(task_id: str, timeout_s: float = 600) -> str:
        """Block until the task finishes (or timeout_s passes) and return the outcome."""
        return dump(await tools.wait_for_result(hub(), task_id, timeout_s))

    @server.tool()
    async def cancel_task(task_id: str, reason: str = "") -> str:
        """Withdraw a task you requested."""
        return dump(await tools.cancel_task(hub(), state["me"], task_id, reason))

    @server.tool()
    async def inbox(include_seen: bool = False, peek: bool = False) -> str:
        """Messages addressed to you (new requests, questions, answers, results).
        peek=True leaves them unread (for a watcher that only decides whether to wake you)."""
        return dump(await tools.inbox(hub(), state["me"], include_seen, peek=peek))

    @server.tool()
    async def wait_for_message(timeout_s: float = 600, peek: bool = False, actionable_only: bool = True,
                               new: bool = False) -> str:
        """Block until at least one unread message arrives for you (or timeout_s passes), then return them.
        actionable_only ignores ACKs and progress UPDATEs. new: only messages arriving from now on (older
        unread ones are ignored). Returns [] on timeout. Use it instead of polling."""
        return dump(await tools.inbox(hub(), state["me"], peek=peek, wait_s=timeout_s, new=new,
                                      types=tools.ACTIONABLE if actionable_only else None))

    @server.tool()
    async def accept_task(task_id: str) -> str:
        """Accept a task that was sent to you (interactive agents)."""
        return dump(await tools.accept_task(hub(), state["me"], task_id))

    @server.tool()
    async def reject_task(task_id: str, reason: str) -> str:
        """Refuse a task sent to you, with the reason."""
        return dump(await tools.reject_task(hub(), state["me"], task_id, reason))

    @server.tool()
    async def report_progress(message: str, task_id: str | None = None, state_: str | None = None) -> str:
        """Tell the requester about progress on a task you own. state_: RUNNING | WAITING | BLOCKED."""
        return dump(await tools.report_progress(hub(), state["me"], message, task_id, state_))

    @server.tool()
    async def submit_result(status: str, summary: str, task_id: str | None = None,
                            outputs: dict[str, Any] | None = None, artifacts: list[dict[str, Any]] | None = None,
                            evidence: list[str] | None = None, limitations: list[str] | None = None,
                            follow_up: list[str] | None = None) -> str:
        """Finish a task you own. status: complete | partial | failed — be honest; never call partial complete.
        artifacts: references returned by publish_artifact."""
        return dump(await tools.submit_result(
            hub(), state["me"], status, summary, task_id=task_id, outputs=outputs, artifacts=artifacts,
            evidence=evidence, limitations=limitations, follow_up=follow_up))

    @server.tool()
    async def ask_question(task_id: str, question: str) -> str:
        """Ask the other party of a task a question."""
        return dump(await tools.ask_question(hub(), state["me"], task_id, question))

    @server.tool()
    async def answer_question(task_id: str, answer: str) -> str:
        """Answer a QUESTION about a task."""
        return dump(await tools.answer(hub(), state["me"], task_id, answer))

    @server.tool()
    async def publish_artifact(path: str, description: str = "", backend: str = "object",
                               task_id: str | None = None) -> str:
        """Upload a file or directory and get a reference to put in messages.
        backend='object' copies it into the shared store; backend='file' only references a local path.
        task_id files it under that task (defaults to the task you are running, if any)."""
        return dump(await tools.publish_artifact(hub(), state["me"], path, description=description,
                                                 backend=backend, task_id=task_id))

    @server.tool()
    async def fetch_artifact(uri: str, dest_dir: str | None = None, sha256: str | None = None) -> str:
        """Download an artifact reference (artifact://, file://, http(s)://) to a local directory."""
        return dump(await tools.fetch_artifact(hub(), uri, dest_dir, sha256))

    return server


def run(cfg: NodeConfig, me: str | None) -> None:
    logging.basicConfig(level=logging.WARNING)   # stdout belongs to the MCP protocol
    build_server(cfg, me).run("stdio")
