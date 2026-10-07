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
INSERT INTO "jobs" VALUES(1,'T-compat-1','B:desk',99999,'start',NULL,'/tmp/log','training','2026-10-07T04:43:34.696+00:00',NULL,NULL,0);
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
INSERT INTO "messages" VALUES('msg-2436d34fcd93464fb405a199e8893a0a','in','B:desk','A:main','REQUEST','T-compat-1','conv-9bdcd26a51304d5e','{"protocol": "mutmuas/1", "message_id": "msg-2436d34fcd93464fb405a199e8893a0a", "conversation_id": "conv-9bdcd26a51304d5e", "task_id": "T-compat-1", "from": "A:main", "to": "B:desk", "type": "REQUEST", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "high", "body": {"objective": "train it", "reason": "compat", "kind": "experiment", "inputs": {"steps": 3}, "deadline": "2030-01-01T00:00:00+00:00", "observers": ["C:obs"], "leader": true}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.695+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-039f286219b1445da685442ce71607de','in','A:main','B:desk','ACK','T-compat-1','conv-b159136587054dd4','{"protocol": "mutmuas/1", "message_id": "msg-039f286219b1445da685442ce71607de", "conversation_id": "conv-b159136587054dd4", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "ACK", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "normal", "body": {"state": "ACCEPTED", "message": "accepted into queue", "eta": "2030-01-01T00:00:00+00:00"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-00e1600f7cf14f539f84a559e7981043','in','A:main','B:desk','UPDATE','T-compat-1','conv-5167f72bcc334387','{"protocol": "mutmuas/1", "message_id": "msg-00e1600f7cf14f539f84a559e7981043", "conversation_id": "conv-5167f72bcc334387", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "UPDATE", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "normal", "body": {"state": "WAITING", "message": "waiting on a job"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-d7c0f01e176e4ef8acba245f8389805b','in','A:main','B:desk','QUESTION','T-compat-1','conv-62b010cd734b4cbc','{"protocol": "mutmuas/1", "message_id": "msg-d7c0f01e176e4ef8acba245f8389805b", "conversation_id": "conv-62b010cd734b4cbc", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "QUESTION", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "normal", "body": {"question": "which dataset?", "next": "A:main"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-965b220cc56d43a0a0125104376a1530','in','B:desk','A:main','ANSWER','T-compat-1','conv-fd37c7ad1bf74b89','{"protocol": "mutmuas/1", "message_id": "msg-965b220cc56d43a0a0125104376a1530", "conversation_id": "conv-fd37c7ad1bf74b89", "task_id": "T-compat-1", "from": "A:main", "to": "B:desk", "type": "ANSWER", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "normal", "body": {"answer": "v2", "next": "B:desk"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-fc9af5b235f34824a57044178366b293','in','A:main','B:desk','RESULT','T-compat-1','conv-ac558a4484d0420b','{"protocol": "mutmuas/1", "message_id": "msg-fc9af5b235f34824a57044178366b293", "conversation_id": "conv-ac558a4484d0420b", "task_id": "T-compat-1", "from": "B:desk", "to": "A:main", "type": "RESULT", "timestamp": "2026-10-07T04:43:34.693+00:00", "priority": "normal", "body": {"status": "complete", "summary": "trained", "outputs": {"loss": 0.2}, "next": "A:main"}, "artifacts": [], "reply_to": null}','handled',0,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
INSERT INTO "messages" VALUES('msg-1b60589f29924efeba7b81075ff7521a','out','B:desk','C:far','REQUEST','T-compat-3','conv-9e1698deddb94e6f','{"protocol": "mutmuas/1", "message_id": "msg-1b60589f29924efeba7b81075ff7521a", "conversation_id": "conv-9e1698deddb94e6f", "task_id": "T-compat-3", "from": "B:desk", "to": "C:far", "type": "REQUEST", "timestamp": "2026-10-07T04:43:34.696+00:00", "priority": "normal", "body": {"objective": "label it", "reason": "compat", "kind": "query"}, "artifacts": [], "reply_to": null}','queued',1,0,NULL,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',0,0,NULL);
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
    accepting       INTEGER NOT NULL DEFAULT 1   -- 0: online but takes no work (`off`)
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
    PRIMARY KEY (task_id, role)
);
INSERT INTO "tasks" VALUES('T-compat-1','owner','B:desk','A:main','B:desk',NULL,'conv-9bdcd26a51304d5e','WAITING',NULL,'{"objective": "train it", "reason": "compat", "kind": "experiment", "inputs": {"steps": 3}, "deadline": "2030-01-01T00:00:00+00:00", "observers": ["C:obs"], "leader": true}',NULL,NULL,'[]','[]','msg-2436d34fcd93464fb405a199e8893a0a',0,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',NULL,NULL,NULL,NULL,NULL,'["hold on"]',1);
INSERT INTO "tasks" VALUES('T-compat-3','requester','B:desk','B:desk','C:far',NULL,'conv-9e1698deddb94e6f','PENDING',NULL,'{"objective": "label it", "reason": "compat", "kind": "query"}',NULL,NULL,'[]','[]','msg-1b60589f29924efeba7b81075ff7521a',0,'2026-10-07T04:43:34.696+00:00','2026-10-07T04:43:34.696+00:00',NULL,NULL,NULL,NULL,NULL,NULL,0);
CREATE INDEX messages_state ON messages(direction, state);
CREATE INDEX messages_task ON messages(task_id);
CREATE INDEX tasks_status ON tasks(role, status);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('jobs',1);
COMMIT;
