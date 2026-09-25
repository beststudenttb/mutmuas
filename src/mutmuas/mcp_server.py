"""MCP server exposing the agent-facing tools (stdio).

Run as ``agentctl mcp --as A:a1`` (or with MUTMUAS_AGENT set). Register it in
Claude Code with ``claude mcp add mutmuas -- agentctl mcp --as A:a1`` and in
Codex under ``[mcp_servers.mutmuas]`` — see docs/DEPLOYMENT.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import anyio
from mcp.server.mcpserver import MCPServer
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp_types import JSONRPCNotification

from . import tools
from .config import NodeConfig
from .hub import Hub
from .ids import now_iso
from .node import _code_version, session_alive

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
  A new REQUEST while busy: it waits in your inbox; the requester already sees it as delivered.
- Say whether you need a reply: send_request(reply="none") for a notice (it closes when the other session
  reads it), otherwise the other side owes you a RESULT, by `deadline` if you set one. When a thread's next
  step belongs to someone, name them with next=<address>: that wakes them.
- A channel message "mutmuas: new ..." means mail arrived: read it with inbox (that marks it read).
- Privacy: you see other agents' status, not their work. A task's content is for its requester, owner and
  observers only. Your reasoning, memory and logs are private: if asked, reply with a condensed summary."""

CHANNEL = "notifications/claude/channel"
HEARTBEAT_S = float(os.environ.get("MUTMUAS_SESSION_BEAT_S", "15"))   # tests shorten it
PUSH_POLL_S = 1.0
CODE_CHECK_S = 60.0


def stale_notice(started: str, now: str) -> dict[str, Any]:
    """This process cannot swap its own code: an MCP session must be re-initialized by the client."""
    return {"content": f"mutmuas: the mail program was updated ({started} -> {now}) but this session still runs "
                       "the old one. Ask the leader to run /mcp -> Reconnect (mutmuas) in this session.",
            "meta": {"mcp_code": started, "disk_code": now}}


SUMMARY_CHARS = 80


def _summary(m: dict[str, Any]) -> str:
    body = m.get("body") or {}
    text = next((body[k] for k in ("objective", "question", "answer", "summary", "message", "reason")
                 if body.get(k)), None)
    if text is None:            # unknown body shape: the first text value, never an empty summary
        text = next((v for v in body.values() if isinstance(v, str) and v.strip()), "")
    text = " ".join(str(text).split())
    return text[:SUMMARY_CHARS] + ("…" if len(text) > SUMMARY_CHARS else "")


def channel_notice(m: dict[str, Any]) -> dict[str, Any]:
    """The line pushed into the session for one new message (Claude Code channel: {content, meta})."""
    summary = _summary(m)
    note = f" [{m['note']}]" if m.get("note") else ""
    return {"content": f"mutmuas: new {m['type']} from {m['from']} (task {m['task_id']}){note}: {summary} "
                       "- read it with the mutmuas inbox tool.",
            "meta": {"task_id": str(m["task_id"]), "msg_type": m["type"], "sender": m["from"],
                     "summary": summary}}


def duplicate_notice(addr: str, holder: int) -> dict[str, Any]:
    return {"content": f"mutmuas: another session already acts as {addr} (process {holder}). One agent has one "
                       "session: this one gets no mail pushes and must not read or answer its mail. Close one of "
                       "the two sessions, or register this one as its own agent.",
            "meta": {"session": "duplicate", "holder_pid": str(holder)}}


def contender_notice(addr: str, contender: dict[str, Any]) -> dict[str, Any]:
    return {"content": f"mutmuas: a second session tried to act as {addr} (process {contender['pid']}, "
                       f"directory {contender.get('cwd')}). This session keeps the mail; tell the leader.",
            "meta": {"session": "contender", "contender_pid": str(contender["pid"])}}


def reminder_notice(r: dict[str, Any]) -> dict[str, Any]:
    return {"content": f"mutmuas reminder (set {r['created_at']}): {r['text']}",
            "meta": {"reminder": str(r["id"]), "due": r["due"]}}


def build_server(cfg: NodeConfig, me: str | None, io: dict[str, Any] | None = None,
                 channel: bool = False) -> MCPServer:
    state: dict[str, Any] = {}
    io = io if io is not None else {}

    async def heartbeat(hub: Hub, addr: str) -> None:
        """This process lives exactly as long as the session that started it: its beat is the session's.
        One agent, one session: a second session gets told and receives no pushes until the first is gone."""
        me, told = os.getpid(), set()
        while True:
            holder = hub.ledger.session_claim(addr, me, os.getcwd(), session_alive)
            if holder != me and state.get("duplicate_of") != holder:
                state["duplicate_of"] = holder
                await push_now(duplicate_notice(addr, holder))
            elif holder == me:
                if state.pop("duplicate_of", None):
                    await push_now({"content": f"mutmuas: the other session has gone; this session now holds "
                                               f"{addr} and receives its mail.", "meta": {"session": "holder"}})
                for c in hub.ledger.session_contenders(addr):
                    if c["pid"] not in told and session_alive({**c, "pid": c["pid"]}):
                        told.add(c["pid"])
                        await push_now(contender_notice(addr, c))
            await asyncio.sleep(HEARTBEAT_S)

    async def push_now(params: dict[str, Any]) -> None:
        if channel and io.get("write") is not None:
            await io["write"].send(SessionMessage(message=JSONRPCNotification(
                jsonrpc="2.0", method=CHANNEL, params=params)))

    async def push(hub: Hub, addr: str, cursor: int) -> None:
        """Wake the session on new mail that needs it (Claude Code channels). Never marks anything read."""
        await asyncio.sleep(PUSH_POLL_S)          # let the handshake finish before the first notification

        async def send(params: dict[str, Any]) -> None:
            if io.get("write") is not None:
                await io["write"].send(SessionMessage(message=JSONRPCNotification(
                    jsonrpc="2.0", method=CHANNEL, params=params)))
        last_code_check = asyncio.get_running_loop().time()
        while True:
            if asyncio.get_running_loop().time() - last_code_check >= CODE_CHECK_S:
                last_code_check = asyncio.get_running_loop().time()
                disk = await asyncio.to_thread(_code_version)
                if disk != state["code"] and not state.get("stale_told"):
                    state["stale_told"] = True
                    await send(stale_notice(state["code"], disk))
            for m in await tools.inbox(hub, addr, peek=True, types=tools.WAKE, since=str(cursor), limit=20):
                cursor = max(cursor, m["seq"] or cursor)
                if not state.get("duplicate_of"):          # only the session holding the agent is woken
                    await send(channel_notice(m))
            for r in hub.ledger.due_reminders(addr, now_iso()):
                await send(reminder_notice(r))
                hub.ledger.fire_reminder(r["id"])
            await asyncio.sleep(PUSH_POLL_S)

    @asynccontextmanager
    async def lifespan(_server):
        hub = await Hub.open(cfg, "mcp", require_bus=False)
        state["hub"] = hub
        state["code"] = await asyncio.to_thread(_code_version)
        state["me"] = str(hub.local_agent(me)[0])
        background = []
        if not os.environ.get("MUTMUAS_TASK_ID"):          # an interactive session, not a daemon-run task
            background.append(asyncio.create_task(heartbeat(hub, state["me"])))
            if channel:
                background.append(asyncio.create_task(push(hub, state["me"], hub.ledger.last_rowid())))
        try:
            yield {}
        finally:
            for task in background:
                task.cancel()
            with contextlib.suppress(Exception):
                hub.ledger.session_end(state["me"], os.getpid())
            await hub.close()

    server = MCPServer("mutmuas", instructions=INSTRUCTIONS, lifespan=lifespan)

    def hub() -> Hub:
        return state["hub"]

    def dump(value: Any) -> str:
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)

    @server.tool()
    async def whoami() -> str:
        """Your own address, permissions, open tasks, unread count and session state (private to you)."""
        disk = await asyncio.to_thread(_code_version)
        extra = {"mcp_code": state["code"], "disk_code": disk, "mcp_stale": disk != state["code"]}
        if state.get("duplicate_of"):
            extra["session_duplicate_of"] = state["duplicate_of"]
        return dump(await tools.whoami(hub(), state["me"]) | extra)

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
                           priority: str = "normal", reply: str = "required", deadline: str | None = None,
                           observers: list[str] | None = None) -> str:
        """Delegate a task to another agent. kind: query | artifact | experiment | code.
        reply: required (the default: they owe you a RESULT) | none (a notice; closed once they read it).
        deadline: ISO time with timezone by which you need the reply; overdue replies are followed up.
        Returns a task_id; the message is durable even if the target is offline."""
        return dump(await tools.send_request(
            hub(), state["me"], to, objective, reason, kind=kind, inputs=inputs, expected_outputs=expected_outputs,
            constraints=constraints, acceptance_criteria=acceptance_criteria, timeout_s=timeout_s,
            artifacts=artifacts, priority=priority, reply=reply, deadline=deadline, observers=observers))

    @server.tool()
    async def check_task(task_id: str) -> str:
        """Current state of a task, its result if finished, and its latest messages."""
        return dump(await tools.check_task(hub(), task_id, me=state["me"]))

    @server.tool()
    async def wait_for_result(task_id: str, timeout_s: float = 600) -> str:
        """Block until the task finishes (or timeout_s passes) and return the outcome."""
        return dump(await tools.wait_for_result(hub(), task_id, timeout_s, me=state["me"]))

    @server.tool()
    async def cancel_task(task_id: str, reason: str = "") -> str:
        """Withdraw a task you requested."""
        return dump(await tools.cancel_task(hub(), state["me"], task_id, reason))

    @server.tool()
    async def inbox(include_seen: bool = False, peek: bool = False, only: str = "wake") -> str:
        """Messages addressed to you. only: "wake" (the default: what needs you - requests, questions, answers,
        refusals, anything naming you as next), "actionable" (also results of your requests) or "all" (also
        ACKs and progress). peek=True leaves them unread. A row with "note" (e.g. rejected: ...) is FYI.
        Old mail you have dealt with elsewhere: look at inbox(only="all", peek=True), then clear_inbox."""
        types = {"wake": tools.WAKE, "actionable": tools.ACTIONABLE}.get(only)
        return dump(await tools.inbox(hub(), state["me"], include_seen, peek=peek, types=types))

    @server.tool()
    async def add_observer(task_id: str, observer: str) -> str:
        """Let another agent read a task you take part in (copies of its request and result). Task content is
        otherwise only for its requester, owner and observers; other people's tasks are not yours to read."""
        return dump(await tools.add_observer(hub(), state["me"], task_id, observer))

    @server.tool()
    async def clear_inbox(before_seq: int) -> str:
        """Mark all your unread mail up to seq (the "seq" field inbox shows) as read, after you have looked at it."""
        return dump(await tools.clear_inbox(hub(), state["me"], before_seq))

    @server.tool()
    async def remind_me(at: str, text: str) -> str:
        """Come back to something later: at (ISO time with timezone, or +10m / +2h) the text is pushed into this
        session like new mail. Use it instead of promising to "check again in a while"."""
        return dump(await tools.remind_me(hub(), state["me"], at, text))

    @server.tool()
    async def wait_for_message(timeout_s: float = 600, peek: bool = False, actionable_only: bool = True) -> str:
        """Block until at least one unread message arrives for you (or timeout_s passes), then return them.
        actionable_only ignores ACKs and progress UPDATEs. Returns [] on timeout. Use it instead of polling."""
        return dump(await tools.inbox(hub(), state["me"], peek=peek, wait_s=timeout_s,
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
    async def report_progress(message: str, task_id: str | None = None, state_: str | None = None,
                              next: str | None = None) -> str:
        """Tell the requester about progress on a task you own. state_: RUNNING | WAITING | BLOCKED.
        next: the address whose move it is now (wakes them)."""
        return dump(await tools.report_progress(hub(), state["me"], message, task_id, state_, next=next))

    @server.tool()
    async def submit_result(status: str, summary: str, task_id: str | None = None,
                            outputs: dict[str, Any] | None = None, artifacts: list[dict[str, Any]] | None = None,
                            evidence: list[str] | None = None, limitations: list[str] | None = None,
                            follow_up: list[str] | None = None, next: str | None = None) -> str:
        """Finish a task you own. status: complete | partial | failed — be honest; never call partial complete.
        artifacts: references returned by publish_artifact. next: who moves next, if anyone (wakes them)."""
        return dump(await tools.submit_result(
            hub(), state["me"], status, summary, task_id=task_id, outputs=outputs, artifacts=artifacts,
            evidence=evidence, limitations=limitations, follow_up=follow_up, next=next))

    @server.tool()
    async def ask_question(task_id: str, question: str, next: str | None = None) -> str:
        """Ask the other party of a task a question."""
        return dump(await tools.ask_question(hub(), state["me"], task_id, question, next=next))

    @server.tool()
    async def answer_question(task_id: str, answer: str, next: str | None = None) -> str:
        """Answer a QUESTION about a task."""
        return dump(await tools.answer(hub(), state["me"], task_id, answer, next=next))

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
        return dump(await tools.fetch_artifact(hub(), uri, dest_dir, sha256, me=state["me"]))

    return server


def run(cfg: NodeConfig, me: str | None, channel: bool = False) -> None:
    """channel: declare the Claude Code channel capability and push new mail into the session
    (start the session with --channels / --dangerously-load-development-channels server:mutmuas)."""
    logging.basicConfig(level=logging.WARNING)   # stdout belongs to the MCP protocol
    anyio.run(serve_stdio, cfg, me, channel)


async def serve_stdio(cfg: NodeConfig, me: str | None, channel: bool = False) -> None:
    io: dict[str, Any] = {}
    server = build_server(cfg, me, io=io, channel=channel)
    low = server._lowlevel_server
    options = low.create_initialization_options(
        experimental_capabilities={"claude/channel": {}} if channel else None)
    async with stdio_server() as (read_stream, write_stream):
        io["write"] = write_stream
        await low.run(read_stream, write_stream, options)
