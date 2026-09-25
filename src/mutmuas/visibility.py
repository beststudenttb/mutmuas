"""Who may see what (leader, 2026-09-25; STANDARD visibility v1, step 1).

Four layers:
  public       directory: address, role, capabilities, online/offline, session on duty, available/busy.
  task status  coordinators + participants: task id, a shortened objective, status, requester -> owner, times.
  task content participants only (requester, owner, observers): inputs, reason, thread, RESULT, artifacts.
  private      nobody: session reasoning, memory, work logs, transcripts, raw run logs. Never over mutmuas;
               ask the person, who answers with a condensed summary.

Step 1 is "minimal exposure by default", a soft boundary: shared stores (registry cards, the task KV) now
carry only the public and status layers, and every tool filters by participant. It is not a security
boundary: a process holding the node credential can still read the message stream. Enforcement (the daemon
as the only NATS principal, or NATS accounts) is step 2.
"""

from __future__ import annotations

import json
from typing import Any

from .config import NodeConfig
from .ledger import Ledger

OBJECTIVE_CHARS = 80
STATUS_KEYS = ("task_id", "requester", "owner", "status", "updated_at")
# The public registry card: who someone is and whether they can take work now, nothing about the work.
CARD_KEYS = ("address", "node", "agent_id", "display", "role", "capabilities", "provider", "mode", "accepts_kinds",
             "state", "availability", "session", "session_seen", "heartbeat_s", "last_heartbeat")


def short(text: str | None, n: int = OBJECTIVE_CHARS) -> str:
    text = " ".join(str(text or "").split())
    return text[:n] + ("…" if len(text) > n else "")


def status_layer(record: dict[str, Any]) -> dict[str, Any]:
    """What coordinators may see about any task, and all that goes into the shared task KV."""
    request = record.get("request") or {}
    out = {k: record.get(k) for k in STATUS_KEYS if record.get(k) not in (None, "", [])}
    out["objective"] = short(record.get("objective") or request.get("objective"))
    return out


def observer_role(agent: str) -> str:
    """Task-row role for an observer (one row per observing agent: the table's key is (task_id, role))."""
    return f"observer:{agent}"


def acl(ledger: Ledger, task_id: str) -> set[str]:
    """Who takes part in a task, from what this node persisted about it: requester, owner, the observers
    listed on the request, and agents holding an observer row. Never inferred from who sent mail on it."""
    people: set[str] = set()
    for row in ledger.db.execute("SELECT role, local_agent, requester, owner, request FROM tasks WHERE task_id=?",
                                 (task_id,)):
        people |= {row["requester"], row["owner"]}
        people |= set((json.loads(row["request"]) if row["request"] else {}).get("observers") or [])
        if row["role"].startswith("observer:"):
            people.add(row["local_agent"])
    return people


def is_coordinator(cfg: NodeConfig, viewer: str) -> bool:
    """Set by HR in node.yaml (coordinators: [B:claude-secretary]); an agent cannot name itself one."""
    return viewer in (cfg.coordinators or [])


def is_participant(ledger: Ledger, viewer: str, task_id: str, record: dict[str, Any] | None = None) -> bool:
    """Requester, owner or observer, per the persisted ACL (and the owner's published status record)."""
    if record and viewer in (record.get("requester"), record.get("owner")):
        return True
    return viewer in acl(ledger, task_id)


def accepts_kinds(permissions: list[str]) -> list[str]:
    """Request kinds an agent may take, from its permissions: public, so senders pick a kind it accepts."""
    from .protocol import REQUEST_KINDS
    return sorted(kind for kind, needed in REQUEST_KINDS.items() if needed in permissions)


def artifact_visible(ledger: Ledger, viewer: str, uri: str) -> bool:
    """An artifact is for whoever published it and whoever it was sent to (its URI in their mail)."""
    parts = uri.removeprefix("artifact://").split("/")
    if len(parts) >= 3 and f"{parts[1]}:{parts[2]}" == viewer:
        return True
    row = ledger.db.execute("SELECT 1 FROM messages WHERE local_agent=? AND envelope LIKE ? LIMIT 1",
                            (viewer, f"%{uri}%")).fetchone()
    return row is not None


def message_visible(viewer: str, env: dict[str, Any]) -> bool:
    """A message's body is for its sender and its recipient (observers get their own copies)."""
    return viewer in (env.get("from"), env.get("to"))
