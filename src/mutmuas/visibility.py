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

from typing import Any

from .config import NodeConfig
from .ledger import Ledger

OBJECTIVE_CHARS = 80
STATUS_KEYS = ("task_id", "requester", "owner", "parent_task", "kind", "status", "result_status", "attempts",
               "created_at", "updated_at")


def short(text: str | None, n: int = OBJECTIVE_CHARS) -> str:
    text = " ".join(str(text or "").split())
    return text[:n] + ("…" if len(text) > n else "")


def status_layer(record: dict[str, Any]) -> dict[str, Any]:
    """What coordinators may see about any task, and all that goes into the shared task KV."""
    request = record.get("request") or {}
    out = {k: record.get(k) for k in STATUS_KEYS if record.get(k) not in (None, "", [])}
    out.setdefault("kind", request.get("kind"))
    out["objective"] = short(record.get("objective") or request.get("objective"))
    return out


def is_coordinator(cfg: NodeConfig, viewer: str) -> bool:
    """Set by HR in node.yaml (coordinators: [B:claude-secretary]); an agent cannot name itself one."""
    return viewer in (cfg.coordinators or [])


def is_participant(ledger: Ledger, viewer: str, task_id: str, record: dict[str, Any] | None = None) -> bool:
    """Requester, owner, or someone the task's mail was sent to (observers receive copies)."""
    if record and viewer in (record.get("requester"), record.get("owner")):
        return True
    row = ledger.db.execute(
        "SELECT 1 FROM tasks WHERE task_id=? AND local_agent=? UNION ALL "
        "SELECT 1 FROM messages WHERE task_id=? AND local_agent=? LIMIT 1",
        (task_id, viewer, task_id, viewer)).fetchone()
    return row is not None


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
