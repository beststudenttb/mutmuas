BEGIN TRANSACTION;
CREATE TABLE artifact_publishers (
    uri             TEXT NOT NULL,
    local_agent     TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    PRIMARY KEY (uri, local_agent)
);
CREATE TABLE brains (
    local_agent     TEXT NOT NULL,
    project         TEXT NOT NULL,           -- "" for the post directory itself
    session_id      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (local_agent, project)
);
CREATE TABLE failures (
    at              TEXT NOT NULL,
    stage           TEXT NOT NULL,           -- heartbeat | outbox | receive | handle | recover | run | ...
    address         TEXT,
    task_id         TEXT,
    attempt         INTEGER,
    error           TEXT NOT NULL            -- "<ExceptionType>: <message>"
);
CREATE TABLE jobs (
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
INSERT INTO "jobs" VALUES(1,'T-compat-1','B:desk',99999,'start',NULL,'/tmp/log','training','2026-10-07T07:52:20.620+00:00',NULL,NULL,0);
CREATE TABLE messages (
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
INSERT INTO "messages" VALUES('msg-5ef64f012fe64868b44ad4e9194aa8cd','in','B:desk','A:main','REQUEST','T-compat-1','conv-0cb482ca21d24fd1','{"protocol": "mutmuas/1", "message_id": "msg-5ef64f012fe64868b44ad4e9194aa8cd", "conversation_id": "conv-0cb482ca21d24fd1", "task_id": "T-compat-1", "from": "A:main", "to": "B:desk", "type": "REQUEST", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "high", "body": {"objective": "train it", "reason": "compat", "kind": "experiment", "inputs": {"steps": 3}, "deadline": "2030-01-01T00:00:00+00:00", "observers": ["C:obs"], "leader": true}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.619+00:00','2026-10-07T07:52:20.619+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-ac543bf4a1a04a00bc38ca1880402e5f','in','A:main','B:desk','ACK','T-compat-1','conv-c22fb068645c4ed3','{"protocol": "mutmuas/1", "message_id": "msg-ac543bf4a1a04a00bc38ca1880402e5f", "conversation_id": "conv-c22fb068645c4ed3", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "ACK", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "normal", "body": {"state": "ACCEPTED", "message": "accepted into queue", "eta": "2030-01-01T00:00:00+00:00"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-4dffb7160e8141c4b54f753425bf5730','in','A:main','B:desk','UPDATE','T-compat-1','conv-a94685812b98433c','{"protocol": "mutmuas/1", "message_id": "msg-4dffb7160e8141c4b54f753425bf5730", "conversation_id": "conv-a94685812b98433c", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "UPDATE", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "normal", "body": {"state": "WAITING", "message": "waiting on a job"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-1fe23fda56ab45c8b90936b039dfe456','in','A:main','B:desk','QUESTION','T-compat-1','conv-7d8fc4415dcf4a3a','{"protocol": "mutmuas/1", "message_id": "msg-1fe23fda56ab45c8b90936b039dfe456", "conversation_id": "conv-7d8fc4415dcf4a3a", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "QUESTION", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "normal", "body": {"question": "which dataset?", "next": "A:main"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-dd9de4ad65374542b3c274fc16cec11e','in','B:desk','A:main','ANSWER','T-compat-1','conv-9028ff4034564385','{"protocol": "mutmuas/1", "message_id": "msg-dd9de4ad65374542b3c274fc16cec11e", "conversation_id": "conv-9028ff4034564385", "task_id": "T-compat-1", "from": "A:main", "to": "B:desk", "type": "ANSWER", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "normal", "body": {"answer": "v2", "next": "B:desk"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-6d030716dd5f4d2787472626b17c33de','in','A:main','B:desk','RESULT','T-compat-1','conv-cb46729898ee435e','{"protocol": "mutmuas/1", "message_id": "msg-6d030716dd5f4d2787472626b17c33de", "conversation_id": "conv-cb46729898ee435e", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "RESULT", "timestamp": "2026-10-07T07:52:20.617+00:00", "priority": "normal", "body": {"status": "complete", "summary": "trained", "outputs": {"loss": 0.2}, "next": "A:main"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-12fed4e15f304770b4f719a196283917','out','B:desk','C:far','REQUEST','T-compat-3','conv-439232f8c3ad4800','{"protocol": "mutmuas/1", "message_id": "msg-12fed4e15f304770b4f719a196283917", "conversation_id": "conv-439232f8c3ad4800", "task_id": "T-compat-3", "from": "B:desk", "to": "C:far", "type": "REQUEST", "timestamp": "2026-10-07T07:52:20.620+00:00", "priority": "normal", "body": {"objective": "label it", "reason": "compat", "kind": "query"}, "artifacts": [], "reply_to": null}','queued',1,0,NULL,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',0,0,NULL);
CREATE TABLE notices (
    task_id         TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (task_id, reason)
);
CREATE TABLE reminders (
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
CREATE TABLE sessions (
    local_agent     TEXT PRIMARY KEY,
    pid             INTEGER NOT NULL,           -- the session's mutmuas MCP process
    session_pid     INTEGER,                    -- its parent: the session itself (Claude Code, Codex, ...)
    cwd             TEXT,
    started_at      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    accepting       INTEGER NOT NULL DEFAULT 1,  -- 0: online but takes no work (`off`)
    activity        TEXT,                       -- busy | idle, as the session's hooks report it (D-108)
    activity_at     TEXT                        -- when it last did
);
CREATE TABLE tasks (
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
    priority        TEXT NOT NULL DEFAULT 'normal',   -- the REQUEST's: high runs first and stops normal (D-104)
    wait_reason     TEXT,                    -- why a paused task waits: "quota" = the account's usage limit (D-104)
    run_log         TEXT,                    -- the latest run's log: its result is read from it after a deploy
    PRIMARY KEY (task_id, role)
);
INSERT INTO "tasks" VALUES('T-compat-1','owner','B:desk','A:main','B:desk',NULL,'conv-0cb482ca21d24fd1','WAITING',NULL,'{"objective": "train it", "reason": "compat", "kind": "experiment", "inputs": {"steps": 3}, "deadline": "2030-01-01T00:00:00+00:00", "observers": ["C:obs"], "leader": true}',NULL,NULL,'[]','[]','msg-5ef64f012fe64868b44ad4e9194aa8cd',0,'2026-10-07T07:52:20.619+00:00','2026-10-07T07:52:20.620+00:00',NULL,NULL,NULL,NULL,NULL,'["hold on"]',1,'high',NULL,NULL);
INSERT INTO "tasks" VALUES('T-compat-3','requester','B:desk','B:desk','C:far',NULL,'conv-439232f8c3ad4800','PENDING',NULL,'{"objective": "label it", "reason": "compat", "kind": "query"}',NULL,NULL,'[]','[]','msg-12fed4e15f304770b4f719a196283917',0,'2026-10-07T07:52:20.620+00:00','2026-10-07T07:52:20.620+00:00',NULL,NULL,NULL,NULL,NULL,NULL,0,'normal',NULL,NULL);
CREATE INDEX messages_state ON messages(direction, state);
CREATE INDEX messages_task ON messages(task_id);
CREATE INDEX tasks_status ON tasks(role, status);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('jobs',1);
COMMIT;
