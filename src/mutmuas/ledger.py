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
    shown           INTEGER NOT NULL DEFAULT 0,   -- an inbox listing showed it: clear_inbox may mark it read
    pushed          INTEGER NOT NULL DEFAULT 0,   -- push state (spec v1.1 §5.1)
    pushed_at       TEXT,
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
    runner          TEXT,                    -- auto_worker: 'worker' | 'session', claimed in one transaction
    runner_pid      INTEGER,                 -- the worker's process ...
    runner_start    TEXT,                    -- ... and its start time (pids get reused)
    stuck_pgid      INTEGER,                 -- a process group a stop could not end: nothing runs beside it
    eta             TEXT,                    -- the owner's estimate (D-076)
    interrupts      TEXT,                    -- messages that stopped a run, for the next run (D-089)
    paused          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, role)
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks(role, status);

-- Is the interactive agent's session there? Written by the session's own mutmuas MCP process, which lives
-- exactly as long as the session; read by the daemon for the registry card (session: online|offline).
CREATE TABLE IF NOT EXISTS sessions (
    local_agent     TEXT PRIMARY KEY,
    pid             INTEGER NOT NULL,           -- the session's mutmuas MCP process
    session_pid     INTEGER,                    -- its parent: the session itself (Claude Code, Codex, ...)
    cwd             TEXT,
    started_at      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    accepting       INTEGER NOT NULL DEFAULT 1   -- 0: online but takes no work (`off`)
);

-- Artifacts an agent of this node published itself: with inbound mail, the only source of artifact access
-- (not the URI's path, which the publisher chooses; not outgoing mail, which anyone can fill with any URI).
CREATE TABLE IF NOT EXISTS artifact_publishers (
    uri             TEXT NOT NULL,
    local_agent     TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    PRIMARY KEY (uri, local_agent)
);

-- "Remind me at …" (D-066): the node delivers the text into the agent's inbox when it is due; a repeating one
-- (every_s) is then set to come back after its interval.
CREATE TABLE IF NOT EXISTS reminders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    local_agent     TEXT NOT NULL,
    due             TEXT NOT NULL,
    text            TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    fired_at        TEXT,
    every_s         REAL,                    -- repeats after this many seconds
    cancelled_at    TEXT,
    task_id         TEXT                     -- set in a worker run: the task it wakes (D-098)
);

-- Follow-ups already sent (overdue reply, session gone), so each is sent once.
CREATE TABLE IF NOT EXISTS notices (
    task_id         TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (task_id, reason)
);

-- Supervision (D-040): errors are skipped and recorded here, not defended against; `agentctl failures` lists them.
CREATE TABLE IF NOT EXISTS failures (
    at              TEXT NOT NULL,
    stage           TEXT NOT NULL,           -- heartbeat | outbox | receive | handle | recover | run | ...
    address         TEXT,
    task_id         TEXT,
    attempt         INTEGER,
    error           TEXT NOT NULL            -- "<ExceptionType>: <message>"
);

-- A post being retired (agent-node retire-agent): no session may take its lease from the check on. Cleared by
-- --undo.
CREATE TABLE IF NOT EXISTS retiring (
    local_agent     TEXT PRIMARY KEY,
    at              TEXT NOT NULL
);

-- A post's brain batch (D-073): worker runs of (agent, project) resume this conversation until an idle spell.
CREATE TABLE IF NOT EXISTS brains (
    local_agent     TEXT NOT NULL,
    project         TEXT NOT NULL,           -- "" for the post directory itself
    session_id      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (local_agent, project)
);

-- Background jobs a task waits on (D-050): the heartbeat wakes the post when one ends. No time limit.
CREATE TABLE IF NOT EXISTS jobs (
    job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL,
    owner           TEXT NOT NULL,
    pid             INTEGER,                 -- ends when this process is gone (same machine) ...
    pid_start       TEXT,                    -- ... its start time, since pids get reused
    done_file       TEXT,                    -- ... or when this file appears
    log             TEXT,
    note            TEXT,
    created_at      TEXT NOT NULL,
    ended_at        TEXT,
    ended           TEXT,                    -- how it ended: process gone / done-file and its first lines
    children        INTEGER NOT NULL DEFAULT 0   -- waits on the task's child tasks instead (D-066)
);
"""

TASK_JSON_FIELDS = ("request", "result", "result_draft", "input_refs", "output_refs", "interrupts")


# Informational inbound mail (D-098): an ACK, or an UPDATE that names nobody next and is none of the special kinds
# (nudge, observer copy or grant, follow-up, an FYI copy to a node lead). It is read as it is handled, so it
# never piles up as unread.
INFO_KEYS = ("next", "nudge", "copy_of", "observers_add", "follow_up", "fyi", "pause", "resume", "interrupt")
# A key counts when it holds something: absent, null, false, "", [] and {} are all "not set", in SQL as in Python.
INFO_SQL = ("(type='ACK' OR (type='UPDATE' AND "
            + " AND ".join(f"COALESCE(json_extract(envelope,'$.body.{k}'), 0) IN (0, '', '[]', '{{}}')"
                           for k in INFO_KEYS) + "))")


def is_info(env) -> bool:
    return env.type == "ACK" or (env.type == "UPDATE" and not any(env.body.get(k) for k in INFO_KEYS))

class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript(SCHEMA)

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
        with self.tx() as db:
            self._queue(db, env)

    def _queue(self, db: sqlite3.Connection, env: Envelope) -> None:
        now = now_iso()
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

    @staticmethod
    def _type_filter(types: tuple[str, ...] | None, next_to: str | None,
                     own_results: bool = False) -> tuple[str, tuple]:
        """types, plus (with next_to, i.e. the wake view of that agent) any message that hands it the baton
        (body.next); with own_results also the RESULT of a request it made itself that wants a reply
        (no-stall design, G2: decided here on the requester's node, whatever version the owner runs)."""
        if not types:
            return "", ()
        in_types = f"type IN ({','.join('?' * len(types))})"
        if next_to and own_results:
            own_result = ("(type = 'RESULT' AND EXISTS (SELECT 1 FROM tasks t WHERE t.task_id = messages.task_id"
                          " AND t.role = 'requester' AND t.local_agent = ?"
                          " AND COALESCE(json_extract(t.request, '$.reply'), 'required') != 'none'))")
            return (f" AND ({in_types} OR json_extract(envelope, '$.body.next') = ? OR {own_result})",
                    (*types, next_to, next_to))
        if next_to:
            return f" AND ({in_types} OR json_extract(envelope, '$.body.next') = ?)", (*types, next_to)
        return f" AND {in_types}", tuple(types)

    @staticmethod
    def _project_filter(project: tuple[str, str] | None) -> tuple[str, tuple]:
        """project = (the session's project, the post's default project): leave out requests of other projects,
        which the worker takes (D-072). A request without `project` belongs to the default project."""
        if not project:
            return "", ()
        return (" AND NOT (type = 'REQUEST' AND COALESCE(json_extract(envelope, '$.body.project'), ?)"
                " IS NOT ?)", (project[1], project[0]))

    def unseen(self, local_agent: str, limit: int = 50, mark: bool = True,
               types: tuple[str, ...] | None = None, since: str | None = None,
               next_to: str | None = None, show: bool = True, own_results: bool = False,
               leader_first: bool = False, project: tuple[str, str] | None = None, before_seq: int | None = None,
               leader_before_seq: int | None = None) -> list[Envelope]:
        """Inbound messages an interactive agent has not looked at yet.
        Without `since` (a session looking at its mail) the page holds the newest (D-074): the leader's mail first
        (body.leader, D-049, when leader_first), then newest to oldest. Paging back has two stages:
        leader_before_seq continues the leader's mail older than it (then fills up with the newest other
        mail); before_seq pages through the other mail only.
        With `since` (a notifier's cursor, advanced to the last row) it stays in arrival order, oldest first, so
        nothing is skipped.

        Only messages the dispatcher has fully handled: a REQUEST shows up once its task exists
        (so accept_task always works), and requests rejected by policy never show up.
        """
        with self.tx() as db:
            type_sql, type_args = self._type_filter(types, next_to, own_results)
            project_sql, project_args = self._project_filter(project)
            type_sql, type_args = type_sql + project_sql, (*type_args, *project_args)
            # a digit-only `since` is a rowid cursor (monotonic; timestamps collide within a millisecond)
            by_row = since is not None and str(since).isdigit()
            since_sql = (" AND rowid > ?" if by_row else " AND created_at > ?") if since else ""
            since_arg = ((int(since) if by_row else since),) if since else ()
            if since:
                order = "rowid"
            else:
                order = "json_extract(envelope, '$.body.leader') IS NOT 1, rowid DESC" if leader_first \
                    else "rowid DESC"
            before_sql, before_arg = self._page_filter(before_seq, leader_before_seq)
            rows = db.execute("SELECT message_id, envelope FROM messages WHERE direction='in' AND seen=0"
                              f" AND state='handled' AND local_agent=?{type_sql}{since_sql}{before_sql}"
                              f" ORDER BY {order} LIMIT ?",
                              (local_agent, *type_args, *since_arg, *before_arg, limit)).fetchall()
            # show: a foreground listing for the session (peek too) = shown, so clear_inbox may clear it later; a
            # notifier's read (watch, push) is not. mark: read.
            sets = [x for x, on in (("shown=1", show), ("seen=1", mark)) if on]
            if rows and sets:
                db.executemany(f"UPDATE messages SET {', '.join(sets)} WHERE message_id=? AND direction='in'",
                               [(r["message_id"],) for r in rows])
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def mark_seen(self, task_id: str) -> None:
        self.db.execute("UPDATE messages SET seen=1 WHERE direction='in' AND task_id=?", (task_id,))

    _LEADER = "json_extract(envelope, '$.body.leader') IS 1"

    def _page_filter(self, before_seq: int | None, leader_before_seq: int | None,
                     leader_only: bool = False) -> tuple[str, tuple]:
        """The part of the unread mail a page cursor still covers (D-074): before_seq = the other mail older than
        it; leader_before_seq = the leader's mail older than it plus all other mail; neither = everything."""
        sql, args = (f" AND {self._LEADER}", []) if leader_only else ("", [])
        if before_seq is not None:
            sql += f" AND NOT ({self._LEADER}) AND rowid < ?"
            args.append(int(before_seq))
        elif leader_before_seq is not None:
            sql += f" AND (NOT ({self._LEADER}) OR rowid < ?)"
            args.append(int(leader_before_seq))
        return sql, tuple(args)

    def unseen_count(self, local_agent: str, types: tuple[str, ...] | None = None, since: str | None = None,
                     next_to: str | None = None, own_results: bool = False,
                     project: tuple[str, str] | None = None, before_seq: int | None = None,
                     leader_before_seq: int | None = None, leader_only: bool = False) -> int:
        sql = "SELECT COUNT(*) FROM messages WHERE direction='in' AND seen=0 AND state='handled' AND local_agent=?"
        page_sql, page_args = self._page_filter(before_seq, leader_before_seq, leader_only)
        sql += page_sql
        args: list[Any] = [local_agent, *page_args]
        if since:
            sql += " AND rowid > ?" if str(since).isdigit() else " AND created_at > ?"
            args.append(int(since) if str(since).isdigit() else since)
        type_sql, type_args = self._type_filter(types, next_to, own_results)
        project_sql, project_args = self._project_filter(project)
        return self.db.execute(sql + type_sql + project_sql, [*args, *type_args, *project_args]).fetchone()[0]

    def last_rowid(self) -> int:
        """The newest message's rowid: a cursor meaning "from now on"."""
        return self.db.execute("SELECT COALESCE(MAX(rowid), 0) FROM messages").fetchone()[0]

    # ---- sessions and follow-ups ------------------------------------------

    _BEAT_SQL = ("INSERT INTO sessions (local_agent, pid, session_pid, cwd, started_at, last_seen)"
                 " VALUES (?,?,?,?,?,?) ON CONFLICT(local_agent) DO UPDATE SET pid=excluded.pid,"
                 " session_pid=excluded.session_pid, cwd=excluded.cwd, last_seen=excluded.last_seen,"
                 " started_at=CASE WHEN sessions.pid=excluded.pid THEN sessions.started_at"
                 " ELSE excluded.started_at END")

    def session_beat(self, local_agent: str, pid: int, cwd: str | None, session_pid: int | None = None) -> None:
        now = now_iso()
        with self.tx() as db:
            if not db.execute("SELECT 1 FROM retiring WHERE local_agent=?", (local_agent,)).fetchone():
                db.execute(self._BEAT_SQL, (local_agent, pid, session_pid, cwd, now, now))

    def session_claim(self, local_agent: str, pid: int, cwd: str | None, holder_alive,
                      session_pid: int | None = None) -> int:
        """One agent, one session: beat as the holder if the lease is free, stale or already ours; otherwise return
        the holder's pid, or -1 while the agent is being retired. The check and the write are one transaction
        (BEGIN IMMEDIATE), so two sessions starting together cannot both win."""
        now = now_iso()
        with self.tx() as db:
            if db.execute("SELECT 1 FROM retiring WHERE local_agent=?", (local_agent,)).fetchone():
                return -1
            row = db.execute("SELECT * FROM sessions WHERE local_agent=?", (local_agent,)).fetchone()
            if row is not None and row["pid"] not in (0, pid) and holder_alive(dict(row)):
                return row["pid"]
            db.execute(self._BEAT_SQL, (local_agent, pid, session_pid, cwd, now, now))
        return pid

    def begin_retire(self, local_agent: str, refuse_if) -> str | None:
        """Fence an agent for retirement, or say why not: refuse_if() (e.g. its session is there) is checked in the
        same transaction that sets the fence, so no session can take the lease in between."""
        with self.tx() as db:
            if (why := refuse_if()):
                return why
            db.execute("INSERT OR REPLACE INTO retiring (local_agent, at) VALUES (?,?)", (local_agent, now_iso()))
        return None

    def retiring(self, local_agent: str) -> bool:
        return self.db.execute("SELECT 1 FROM retiring WHERE local_agent=?", (local_agent,)).fetchone() is not None

    def end_retire(self, local_agent: str) -> None:
        self.db.execute("DELETE FROM retiring WHERE local_agent=?", (local_agent,))

    def claim_task(self, task_id: str, runner: str, statuses: tuple[str, ...],
                   refuse_if=None) -> str | None:
        """Make `runner` ('worker' | 'session') the one doing an owner task, or say why not. The check and the
        write are one transaction (BEGIN IMMEDIATE). refuse_if(): a further reason to refuse, e.g. that a
        session is there, evaluated inside the same transaction."""
        with self.tx() as db:
            row = db.execute("SELECT status, runner FROM tasks WHERE task_id=? AND role='owner'",
                             (task_id,)).fetchone()
            if row is None:
                return "unknown task"
            if row["runner"] not in (None, runner):
                return f"held by the {row['runner']}"
            if row["status"] not in statuses:
                return f"status {row['status']}"
            reason = refuse_if() if refuse_if else None
            if reason:
                return reason
            db.execute("UPDATE tasks SET runner=? WHERE task_id=? AND role='owner'", (runner, task_id))
        return None

    def release_task(self, task_id: str, runner: str) -> None:
        self.db.execute("UPDATE tasks SET runner=NULL, runner_pid=NULL WHERE task_id=? AND role='owner'"
                        " AND runner=?", (task_id, runner))

    def set_runner_pid(self, task_id: str, pid: int | None, start: str | None = None) -> None:
        self.db.execute("UPDATE tasks SET runner_pid=?, runner_start=? WHERE task_id=? AND role='owner'"
                        " AND runner='worker'", (pid, start, task_id))

    def worker_runs(self, local_agent: str) -> list[tuple[int, str | None, str]]:
        """(pid, start time, task id) of the processes the daemon started for this agent's unfinished tasks."""
        rows = self.db.execute(
            "SELECT runner_pid, runner_start, task_id FROM tasks WHERE role='owner' AND local_agent=?"
            " AND runner='worker' AND runner_pid IS NOT NULL"
            f" AND status NOT IN ({','.join('?' * len(TERMINAL_STATES))})",
            (local_agent, *TERMINAL_STATES)).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

    def session_end(self, local_agent: str, pid: int) -> None:
        """The session closed cleanly. The row stays (pid 0) so the card can say offline, not unknown."""
        self.db.execute("UPDATE sessions SET pid=0, last_seen=? WHERE local_agent=? AND pid=?",
                        (now_iso(), local_agent, pid))

    def session_of(self, local_agent: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM sessions WHERE local_agent=?", (local_agent,)).fetchone()
        return dict(row) if row else None

    def mark_seen_before(self, local_agent: str, before_seq: int) -> list[Envelope]:
        """Mark everything up to a rowid as read (the session's explicit 'clear the backlog') that an inbox
        listing has shown: never mail the session has not seen.
        Informational messages (ACKs, progress) are cleared whether listed or not (D-098).
        Returns what was marked, so receipts can follow."""
        with self.tx() as db:
            rows = db.execute("SELECT message_id, envelope FROM messages WHERE direction='in' AND local_agent=?"
                              f" AND seen=0 AND (shown=1 OR {INFO_SQL}) AND state='handled' AND rowid <= ?",
                              (local_agent, before_seq)).fetchall()
            db.executemany("UPDATE messages SET seen=1 WHERE message_id=? AND direction='in'",
                           [(r["message_id"],) for r in rows])
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def list_recent_inbound(self, local_agent: str, limit: int = 50, show: bool = True) -> list[Envelope]:
        """The newest handled inbound messages, read or not (inbox --all): not ones still new, unverified or
        rejected. With show (a foreground listing) the unread ones count as shown, so
        clear_inbox may clear them later; they are not marked read."""
        with self.tx() as db:
            rows = db.execute("SELECT message_id, envelope FROM messages WHERE direction='in' AND local_agent=?"
                              " AND state='handled' ORDER BY rowid DESC LIMIT ?", (local_agent, limit)).fetchall()
            if show and rows:
                db.executemany("UPDATE messages SET shown=1 WHERE message_id=? AND direction='in' AND seen=0",
                               [(r["message_id"],) for r in rows])
        return [Envelope.from_json(r["envelope"]) for r in rows]

    def inbound_state(self, message_id: str) -> str | None:
        row = self.db.execute("SELECT state FROM messages WHERE message_id=? AND direction='in'",
                              (message_id,)).fetchone()
        return row["state"] if row else None

    def inbound_in_state(self, state: str) -> list[Envelope]:
        return [Envelope.from_json(r["envelope"]) for r in
                self.db.execute("SELECT envelope FROM messages WHERE direction='in' AND state=?", (state,))]

    def unshown_count(self, local_agent: str, before_seq: int) -> int:
        """Unread mail up to a rowid that no foreground inbox listing has shown (clear_inbox leaves it)."""
        return self.db.execute("SELECT COUNT(*) FROM messages WHERE direction='in' AND local_agent=? AND seen=0"
                               f" AND shown=0 AND NOT {INFO_SQL} AND state='handled' AND rowid <= ?",
                               (local_agent, before_seq)).fetchone()[0]

    def mark_info_read(self, message_id: str) -> None:
        """An informational message (ACK, progress) is read as it is handled: it does not count as unread."""
        self.db.execute("UPDATE messages SET seen=1 WHERE message_id=? AND direction='in'", (message_id,))

    def record_published(self, uri: str, local_agent: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO artifact_publishers (uri, local_agent, published_at) VALUES (?,?,?)",
                        (uri, local_agent, now_iso()))

    def add_reminder(self, local_agent: str, due: str, text: str, every_s: float | None = None,
                     task_id: str | None = None) -> int:
        cur = self.db.execute("INSERT INTO reminders (local_agent, due, text, created_at, every_s, task_id)"
                              " VALUES (?,?,?,?,?,?)", (local_agent, due, text, now_iso(), every_s, task_id))
        return cur.lastrowid

    def due_reminders(self, now: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM reminders WHERE fired_at IS NULL AND cancelled_at IS NULL AND due <= ? ORDER BY due",
            (now,))]

    def fire_reminder(self, reminder: dict[str, Any], next_due: str | None) -> bool:
        """Take a due reminder: True for exactly one caller, so it is delivered once. A repeating one moves on to
        next_due instead of being closed."""
        if next_due:
            cur = self.db.execute("UPDATE reminders SET due=? WHERE id=? AND due=? AND fired_at IS NULL",
                                  (next_due, reminder["id"], reminder["due"]))
        else:
            cur = self.db.execute("UPDATE reminders SET fired_at=? WHERE id=? AND fired_at IS NULL",
                                  (now_iso(), reminder["id"]))
        return cur.rowcount == 1

    def cancel_reminder(self, local_agent: str, reminder_id: int) -> bool:
        cur = self.db.execute("UPDATE reminders SET cancelled_at=? WHERE id=? AND local_agent=? AND cancelled_at IS NULL",
                              (now_iso(), reminder_id, local_agent))
        return cur.rowcount == 1

    def record_observed(self, task_id: str, observer: str, requester: str, owner: str,
                        request: dict[str, Any] | None = None, result: dict[str, Any] | None = None,
                        observers: list[str] | None = None) -> None:
        """An observer's own row for a task it was given copies of (role observer:<agent>)."""
        role, now = f"observer:{observer}", now_iso()
        with self.tx() as db:
            row = db.execute("SELECT request, result FROM tasks WHERE task_id=? AND role=?", (task_id, role)).fetchone()
            old_request = json.loads(row["request"]) if row and row["request"] else {}
            new_request = {**old_request, **(request or {})}
            if observers:
                new_request["observers"] = sorted(set(new_request.get("observers") or []) | set(observers))
            new_result = result if result is not None else (json.loads(row["result"]) if row and row["result"] else None)
            status = "COMPLETED" if new_result and new_result.get("status") != "failed" else (
                "FAILED" if new_result else "PENDING")
            db.execute("INSERT INTO tasks (task_id, role, local_agent, requester, owner, status, result_status, request,"
                       " result, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                       " ON CONFLICT(task_id, role) DO UPDATE SET request=excluded.request, result=excluded.result,"
                       " status=excluded.status, result_status=excluded.result_status, updated_at=excluded.updated_at",
                       (task_id, role, observer, requester, owner, status,
                        (new_result or {}).get("status"), json.dumps(new_request, ensure_ascii=False),
                        json.dumps(new_result, ensure_ascii=False) if new_result is not None else None, now, now))

    def add_observers(self, task_id: str, observers: list[str]) -> None:
        """Extend the observer list on every row this node keeps for the task."""
        with self.tx() as db:
            for row in db.execute("SELECT role, request FROM tasks WHERE task_id=?", (task_id,)).fetchall():
                request = json.loads(row["request"]) if row["request"] else {}
                request["observers"] = sorted(set(request.get("observers") or []) | set(observers))
                db.execute("UPDATE tasks SET request=?, updated_at=? WHERE task_id=? AND role=?",
                           (json.dumps(request, ensure_ascii=False), now_iso(), task_id, row["role"]))

    def record_failure(self, stage: str, error: BaseException | str, address: str | None = None,
                       task_id: str | None = None, attempt: int | None = None) -> None:
        text = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        self.db.execute("INSERT INTO failures (at, stage, address, task_id, attempt, error) VALUES (?,?,?,?,?,?)",
                        (now_iso(), stage, address, task_id, attempt, text))

    def add_job(self, task_id: str, owner: str, pid: int | None, pid_start: str | None, done_file: str | None,
                log: str | None, note: str | None, children: bool = False) -> int:
        cur = self.db.execute("INSERT INTO jobs (task_id, owner, pid, pid_start, done_file, log, note, created_at,"
                              " children) VALUES (?,?,?,?,?,?,?,?,?)",
                              (task_id, owner, pid, pid_start, done_file, log, note, now_iso(), int(children)))
        return cur.lastrowid

    def mark_pushed(self, message_ids: list[str]) -> None:
        now = now_iso()
        self.db.executemany("UPDATE messages SET pushed=pushed+1, pushed_at=? WHERE message_id=? AND direction='in'",
                            [(now, m) for m in message_ids])

    def push_state(self, local_agent: str) -> dict[str, Any]:
        """The oldest unread message's arrival and the last push into the session (spec §5.1)."""
        row = self.db.execute("SELECT MIN(created_at) oldest, MAX(pushed_at) last_push FROM messages"
                              " WHERE direction='in' AND local_agent=? AND state='handled' AND seen=0",
                              (local_agent,)).fetchone()
        last = self.db.execute("SELECT MAX(pushed_at) FROM messages WHERE direction='in' AND local_agent=?",
                               (local_agent,)).fetchone()[0]
        return {"oldest_unread_at": row["oldest"], "last_push_at": last}

    def set_session_accepting(self, local_agent: str, accepting: bool) -> bool:
        cur = self.db.execute("UPDATE sessions SET accepting=? WHERE local_agent=?", (int(accepting), local_agent))
        return cur.rowcount == 1

    def brain_session(self, local_agent: str, project: str | None) -> str | None:
        row = self.db.execute("SELECT session_id FROM brains WHERE local_agent=? AND project=?",
                              (local_agent, project or "")).fetchone()
        return row["session_id"] if row else None

    def set_brain_session(self, local_agent: str, project: str | None, session_id: str) -> None:
        self.db.execute("INSERT INTO brains (local_agent, project, session_id, updated_at) VALUES (?,?,?,?)"
                        " ON CONFLICT(local_agent, project) DO UPDATE SET session_id=excluded.session_id,"
                        " updated_at=excluded.updated_at", (local_agent, project or "", session_id, now_iso()))

    def forget_brain(self, local_agent: str, project: str | None) -> None:
        self.db.execute("DELETE FROM brains WHERE local_agent=? AND project=?", (local_agent, project or ""))

    def brains(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.execute("SELECT * FROM brains")]

    def children(self, task_id: str) -> list[dict[str, Any]]:
        """The tasks this node requested on behalf of task_id (their parent_task), oldest first."""
        return [_task_row(r) for r in self.db.execute(
            "SELECT * FROM tasks WHERE role='requester' AND parent_task=? ORDER BY created_at, rowid", (task_id,))]

    def jobs(self, task_id: str | None = None, owner: str | None = None,
             open_only: bool = True) -> list[dict[str, Any]]:
        sql, args = "SELECT * FROM jobs WHERE 1=1", []
        if task_id:
            sql += " AND task_id=?"
            args.append(task_id)
        if owner:
            sql += " AND owner=?"
            args.append(owner)
        if open_only:
            sql += " AND ended_at IS NULL"
        return [dict(r) for r in self.db.execute(sql + " ORDER BY job_id", args)]

    def end_job(self, job_id: int, ended: str) -> None:
        self.db.execute("UPDATE jobs SET ended_at=?, ended=? WHERE job_id=?", (now_iso(), ended, job_id))

    def failures(self, limit: int = 50) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.execute("SELECT * FROM failures ORDER BY rowid DESC LIMIT ?", (limit,))]

    def noticed(self, task_id: str, reason: str) -> bool:
        return self.db.execute("SELECT 1 FROM notices WHERE task_id=? AND reason=?", (task_id, reason)).fetchone() \
            is not None

    def notice_once(self, task_id: str, reason: str) -> bool:
        """True the first time (task_id, reason) is recorded: send that follow-up now, and never again."""
        cur = self.db.execute("INSERT OR IGNORE INTO notices (task_id, reason, created_at) VALUES (?,?,?)",
                              (task_id, reason, now_iso()))
        return cur.rowcount == 1

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
              local_agent: str | None = None, limit: int | None = 200) -> list[dict[str, Any]]:
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
        sql += " ORDER BY created_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            args.append(limit)
        return [_task_row(r) for r in self.db.execute(sql, args).fetchall()]

    def update_task(self, task_id: str, role: str, *, status: str | None = None, force: bool = False,
                    queue: Envelope | None = None, **fields: Any) -> bool:
        """Apply a state transition. Terminal states are sticky unless ``force``. Returns False if refused.

        ``queue``: a message announcing this transition, put in the outbox in the *same* transaction, so a
        crash can never leave a finished task whose RESULT was not queued.
        """
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
            if queue is not None:
                self._queue(db, queue)
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
