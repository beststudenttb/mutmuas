"""Per-node local ledger (SQLite, WAL mode).

Responsibilities:
  * outbox   — every outbound message is written here *before* publishing, so a
               network outage or crash never loses a RESULT; a flusher retries.
  * inbox    — inbound messages are committed here *before* the JetStream ack,
               keyed by message_id, which makes redelivery harmless (dedup).
  * tasks    — the local view of every task this node requested or owns,
               including attempts, drafts of results and the message thread.

The daemon, the CLI and the MCP server of the same node share this file.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .ids import now_iso
from .protocol import TERMINAL_STATES, Envelope

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    message_id      TEXT NOT NULL,
    direction       TEXT NOT NULL,           -- in | out  (same-node messages have one row of each)
    local_agent     TEXT NOT NULL,           -- NODE:agent on this node
    peer            TEXT NOT NULL,
    type            TEXT NOT NULL,
    task_id         TEXT,
    conversation_id TEXT,
    envelope        TEXT NOT NULL,
    state           TEXT NOT NULL,           -- out: queued|sent   in: new|handled|rejected|dropped
    seen            INTEGER NOT NULL DEFAULT 0,   -- surfaced to an interactive agent
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (message_id, direction)
);
CREATE INDEX IF NOT EXISTS messages_state ON messages(direction, state);
CREATE INDEX IF NOT EXISTS messages_task ON messages(task_id);

CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT NOT NULL,
    role            TEXT NOT NULL,           -- requester | owner
    local_agent     TEXT NOT NULL,
    requester       TEXT NOT NULL,
    owner           TEXT NOT NULL,
    parent_task     TEXT,
    conversation_id TEXT,
    status          TEXT NOT NULL,
    result_status   TEXT,                    -- complete | partial | failed
    request         TEXT,                    -- REQUEST body (json)
    result          TEXT,                    -- RESULT body (json)
    result_draft    TEXT,                    -- owner side: result submitted by the agent, not yet sent
    input_refs      TEXT NOT NULL DEFAULT '[]',
    output_refs     TEXT NOT NULL DEFAULT '[]',
    last_message    TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (task_id, role)
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks(role, status);
"""

TASK_JSON_FIELDS = ("request", "result", "result_draft", "input_refs", "output_refs")


class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self._migrate_v1()
        self.db.executescript(SCHEMA)

    def _migrate_v1(self) -> None:
        """v1 keyed messages on message_id alone, which dropped same-node deliveries. Re-key in place."""
        pk = [r["name"] for r in self.db.execute("PRAGMA table_info(messages)") if r["pk"]]
        if pk != ["message_id"]:
            return
        with self.tx() as db:
            db.execute("ALTER TABLE messages RENAME TO messages_v1")
            db.execute("DROP INDEX IF EXISTS messages_state")
            db.execute("DROP INDEX IF EXISTS messages_task")
            for stmt in SCHEMA.split("CREATE TABLE IF NOT EXISTS tasks")[0].split(";"):
                if stmt.strip():        # executescript would commit implicitly; stay inside this transaction
                    db.execute(stmt)
            db.execute("INSERT INTO messages SELECT * FROM messages_v1")
            db.execute("DROP TABLE messages_v1")

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    # ---- outbox -------------------------------------------------------

    def queue_outgoing(self, env: Envelope) -> None:
        now = now_iso()
        with self.tx() as db:
            db.execute(
                "INSERT OR IGNORE INTO messages (message_id, direction, local_agent, peer, type, task_id,"
                " conversation_id, envelope, state, seen, created_at, updated_at)"
                " VALUES (?, 'out', ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?)",
                (env.message_id, env.sender, env.to, env.type, env.task_id, env.conversation_id,
                 env.to_json().decode(), now, now))
            if env.type == "REQUEST":
                self._insert_task(db, env, role="requester", local_agent=env.sender, status="PENDING")

    def outbox(self, limit: int = 100) -> list[Envelope]:
        rows = self.db.execute("SELECT envelope FROM messages WHERE direction='out' AND state='queued'"
                               " ORDER BY rowid LIMIT ?", (limit,)).fetchall()
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def mark_sent(self, message_id: str) -> None:
        self.db.execute("UPDATE messages SET state='sent', attempts=attempts+1, last_error=NULL, updated_at=?"
                        " WHERE message_id=? AND direction='out'", (now_iso(), message_id))

    def mark_send_error(self, message_id: str, error: str) -> None:
        self.db.execute("UPDATE messages SET attempts=attempts+1, last_error=?, updated_at=?"
                        " WHERE message_id=? AND direction='out'", (error[:500], now_iso(), message_id))

    # ---- inbox --------------------------------------------------------

    def ingest(self, env: Envelope) -> bool:
        """Commit an inbound message. Returns False if we have already seen this message_id."""
        now = now_iso()
        cur = self.db.execute(
            "INSERT OR IGNORE INTO messages (message_id, direction, local_agent, peer, type, task_id,"
            " conversation_id, envelope, state, created_at, updated_at)"
            " VALUES (?, 'in', ?, ?, ?, ?, ?, ?, 'new', ?, ?)",
            (env.message_id, env.to, env.sender, env.type, env.task_id, env.conversation_id,
             env.to_json().decode(), now, now))
        return cur.rowcount == 1

    def unhandled(self, local_agent: str) -> list[Envelope]:
        rows = self.db.execute("SELECT envelope FROM messages WHERE direction='in' AND state='new'"
                               " AND local_agent=? ORDER BY rowid", (local_agent,)).fetchall()
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def request_envelope(self, task_id: str) -> Envelope:
        row = self.db.execute("SELECT envelope FROM messages WHERE task_id=? AND type='REQUEST'"
                              " ORDER BY direction='in' DESC, rowid LIMIT 1", (task_id,)).fetchone()
        if row is None:
            raise KeyError(f"no REQUEST recorded for task {task_id}")
        return Envelope.from_json(row["envelope"])

    def mark_handled(self, message_id: str, state: str = "handled", error: str | None = None) -> None:
        self.db.execute("UPDATE messages SET state=?, last_error=?, updated_at=? WHERE message_id=? AND direction='in'",
                        (state, error, now_iso(), message_id))

    def unseen(self, local_agent: str, limit: int = 50, mark: bool = True) -> list[Envelope]:
        """Inbound messages an interactive agent has not looked at yet.

        Only messages the dispatcher has fully handled: a REQUEST shows up once its task exists
        (so accept_task always works), and requests rejected by policy never show up.
        """
        with self.tx() as db:
            rows = db.execute("SELECT message_id, envelope FROM messages WHERE direction='in' AND seen=0"
                              " AND state='handled' AND local_agent=? ORDER BY rowid LIMIT ?",
                              (local_agent, limit)).fetchall()
            if mark and rows:
                db.executemany("UPDATE messages SET seen=1 WHERE message_id=? AND direction='in'",
                               [(r["message_id"],) for r in rows])
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def unseen_count(self, local_agent: str) -> int:
        return self.db.execute("SELECT COUNT(*) FROM messages WHERE direction='in' AND seen=0 AND state='handled'"
                               " AND local_agent=?", (local_agent,)).fetchone()[0]

    def count(self, direction: str, state: str, local_agent: str | None = None) -> int:
        sql = "SELECT COUNT(*) FROM messages WHERE direction=? AND state=?"
        args: list[Any] = [direction, state]
        if local_agent:
            sql += " AND local_agent=?"
            args.append(local_agent)
        return self.db.execute(sql, args).fetchone()[0]

    def thread(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT direction, state, envelope, last_error FROM messages WHERE task_id=?"
                               " ORDER BY created_at, rowid", (task_id,)).fetchall()
        out, seen = [], set()
        for r in rows:                      # a same-node message has an out and an in row: show it once
            env = json.loads(r["envelope"])
            if env["message_id"] in seen:
                continue
            seen.add(env["message_id"])
            out.append({"direction": r["direction"], "delivery": r["state"], "error": r["last_error"], **env})
        out.sort(key=lambda m: m["timestamp"])
        return out

    # ---- tasks --------------------------------------------------------

    def _insert_task(self, db: sqlite3.Connection, req: Envelope, role: str, local_agent: str,
                     status: str) -> bool:
        now = now_iso()
        cur = db.execute(
            "INSERT OR IGNORE INTO tasks (task_id, role, local_agent, requester, owner, parent_task,"
            " conversation_id, status, request, input_refs, last_message, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (req.task_id, role, local_agent, req.sender, req.to, req.body.get("parent_task"),
             req.conversation_id, status, json.dumps(req.body, ensure_ascii=False),
             json.dumps([a.to_dict() for a in req.artifacts]), req.message_id, now, now))
        return cur.rowcount == 1

    def create_owned_task(self, req: Envelope) -> bool:
        with self.tx() as db:
            return self._insert_task(db, req, role="owner", local_agent=req.to, status="PENDING")

    def task(self, task_id: str, role: str | None = None) -> dict[str, Any] | None:
        if role:
            row = self.db.execute("SELECT * FROM tasks WHERE task_id=? AND role=?", (task_id, role)).fetchone()
        else:  # prefer the owner view when a node is both sides (A:x -> A:y)
            row = self.db.execute("SELECT * FROM tasks WHERE task_id=? ORDER BY role='owner' DESC",
                                  (task_id,)).fetchone()
        return _task_row(row) if row else None

    def tasks(self, role: str | None = None, statuses: tuple[str, ...] | None = None,
              local_agent: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM tasks WHERE 1=1", []
        if role:
            sql += " AND role=?"
            args.append(role)
        if local_agent:
            sql += " AND local_agent=?"
            args.append(local_agent)
        if statuses:
            sql += f" AND status IN ({','.join('?' * len(statuses))})"
            args.extend(statuses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        return [_task_row(r) for r in self.db.execute(sql, args).fetchall()]

    def update_task(self, task_id: str, role: str, *, status: str | None = None, force: bool = False,
                    **fields: Any) -> bool:
        """Apply a state transition. Terminal states are sticky unless ``force``. Returns False if refused."""
        with self.tx() as db:
            row = db.execute("SELECT status FROM tasks WHERE task_id=? AND role=?", (task_id, role)).fetchone()
            if row is None:
                return False
            if row["status"] in TERMINAL_STATES and not force and status is not None:
                return False
            sets, args = ["updated_at=?"], [now_iso()]
            if status:
                sets.append("status=?")
                args.append(status)
            for key, value in fields.items():
                sets.append(f"{key}=?")
                args.append(json.dumps(value, ensure_ascii=False) if key in TASK_JSON_FIELDS else value)
            args.extend([task_id, role])
            db.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id=? AND role=?", args)
            return True

    def bump_attempts(self, task_id: str) -> int:
        with self.tx() as db:
            db.execute("UPDATE tasks SET attempts=attempts+1, updated_at=? WHERE task_id=? AND role='owner'",
                       (now_iso(), task_id))
            return db.execute("SELECT attempts FROM tasks WHERE task_id=? AND role='owner'",
                              (task_id,)).fetchone()[0]


def _task_row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for key in TASK_JSON_FIELDS:
        if d.get(key):
            d[key] = json.loads(d[key])
    return d
