# Architecture V1 (Phase 1)

Goal of Phase 1: **agents on different machines reliably find each other,
delegate tasks, reply, exchange artifacts, and lose no messages while offline.**
The success criterion is that you sit at A, talk only to A's agent, and it finds B's agent,
delegates, gets the result back and carries on.

## 1. Components

```
                 you ── Claude Code session (A:lead, interactive)
                              │ MCP tools (agentctl mcp)
 ┌──────────── node A (macOS) ┼──────────────┐      ┌──────────── node B (Linux GPU) ─────────────┐
 │  agent-node daemon         │              │      │  agent-node daemon                          │
 │   ├ receiver/dispatcher per agent         │      │   ├ receiver/dispatcher per agent           │
 │   ├ runners (worker agents) ── codex exec │      │   ├ runners ── claude -p / codex exec       │
 │   ├ heartbeat, outbox flusher             │      │   ├ heartbeat, outbox flusher               │
 │   └ ledger.sqlite3 (outbox/inbox/tasks)   │      │   └ ledger.sqlite3                          │
 └───────────────────┬───────────────────────┘      └──────────────────┬──────────────────────────┘
                     │           Tailscale / WireGuard (private)       │
                     └──────────────────┬──────────────────────────────┘
                              NATS server + JetStream
            ┌───────────────┬───────────┴─────┬──────────────┬─────────────────┐
            stream MM_p_MSG   kv mm_p_agents    kv mm_p_nodes  kv mm_p_tasks     objects mm_p_artifacts
            (mailboxes,       (AgentCards,      (NodeCards)    (task records,    (artifact payloads)
             audit log)        presence)                        owner-written)
```

| Component | Code | Responsibility |
|---|---|---|
| Protocol | `protocol.py`, `ids.py` | Envelope, 10 message types, body validation, task states, addresses `NODE:agent` |
| Bus | `bus.py` | The only module that knows NATS: subjects, stream, consumers, KV, object store |
| Ledger | `ledger.py` | Per-node SQLite: outbox (send-before-publish), inbox (commit-before-ack, dedup), task table |
| Hub | `hub.py` | Node-local service layer shared by daemon/CLI/MCP: registry, send, task views, owner transitions |
| Node daemon | `node.py` | Mailbox pulling, dispatch, task execution, heartbeats, outbox retry, crash recovery |
| Runtimes | `runtime.py` | `script`, `claude-code` (`claude -p`), `codex` (`codex exec`). All subprocesses with timeout/cancel |
| Worktrees | `worktree.py` | `kind: code` tasks run in a per-task git worktree; results come back as branch ref + patch; the sandboxed CLI may write <repo>/.git so it can commit |
| Artifacts | `artifacts.py` | `artifact://` (object store), `file://NODE/path`, `http(s)://`, `git://` refs; sha256 checks |
| Agent tools | `tools.py`, `mcp_server.py` | find/list agents, send_request, wait/check, inbox, accept/reject, progress, submit_result, publish/fetch artifact |
| CLI | `cli.py` | `agentctl` (use/inspect), `agent-node` (init/join/start/service/server-config/doctor) |
| Server config | `server_config.py` | Generates `nats-server.conf` with one user per node + per-node publish permissions |

## 2. Identity

- **Address** = `NODE:agent` (e.g. `B:representation`). This is the logical, stable identity and it
  names the mailbox. Provider and model are *attributes* on the AgentCard, so moving from Opus
  to Fable keeps the address, the inbox and the history.
- **display** (e.g. `B:a1`) is a human alias that the CLI and MCP resolve to the address.
- **Session**: each run of a worker is its own `claude -p` / `codex exec` process. The
  attempt number and run log are recorded per task.
- The project namespace prefixes every subject and bucket, so several projects can share one server.

## 3. Message flow

Send (any process on a node, e.g. the MCP server inside your Claude session):

1. Validate the envelope, then **write it to the local outbox** (SQLite).
2. Publish to `mm.<p>.msg.<to_node>.<to_agent>.<from_node>` with `Nats-Msg-Id = message_id`.
   On success the row is marked `sent`. If NATS is unreachable it stays `queued`, and the daemon's
   outbox loop retries every second.

Receive (daemon of the target node, per agent):

1. Pull from the durable consumer `inbox_<node>_<agent>`.
2. Check that the envelope's sender node equals the subject's sender node, which the server enforces
   via permissions. Invalid messages are answered with `ERROR` and terminated.
3. **Commit to the local inbox table (INSERT OR IGNORE on message_id), then ack.**
   A crash between the two causes a redelivery, and the redelivery is a no-op.
4. The dispatcher handles committed messages in order:
   - `REQUEST`: check policy (`accept_from`, permission for `kind`). Then `REJECT`, or create the
     owned task. Worker agents: `ACK` and queue it. Interactive agents: it waits in the inbox until
     `accept_task`. A duplicate REQUEST for a finished task is answered by re-sending the stored
     RESULT and is never re-run.
   - `CANCEL`: kill the running process group, then `CANCELLED`.
   - Replies (`ACK/UPDATE/QUESTION/BLOCKED/RESULT/REJECT/ERROR`): update the requester-side task.

Execution (worker agents):

`ACCEPTED`, then `RUNNING` (UPDATE), then the runtime subprocess runs with `MUTMUAS_TASK_ID` and
friends in its environment. The agent uses MCP tools (`report_progress`, `publish_artifact`, `submit_result`).
When the process exits, the daemon sends the RESULT: the agent's `submit_result` if it called it,
otherwise a JSON result on stdout, otherwise an honest fallback (`partial` on exit 0, `failed` on
non-zero exit, and a non-zero exit also downgrades a claimed `complete` to `partial`).

Sequence of TEST 2 as it is actually observed (from `agentctl task`):

```
REQUEST  A:main -> B:experimenter   objective, reason, acceptance criteria
ACK      B:experimenter -> A:main   accepted into queue
UPDATE   B:experimenter -> A:main   started (attempt 1, runtime script)
UPDATE   ... step 1/5 ... step 5/5
RESULT   B:experimenter -> A:main   complete, outputs, artifact://demo/B/experimenter/<task>/metrics.json
```

## 4. Storage

| Data | Where | Owner / writer | Lifetime |
|---|---|---|---|
| Messages | JetStream stream `MM_<p>_MSG` (file storage) | sender | `message_retention_days` (default 30), also the audit log |
| Mailbox position | durable consumer per agent | receiving node | permanent |
| Outbox / inbox / tasks | `<data_dir>/ledger.sqlite3` | that node | permanent |
| Agent & node cards | KV `mm_<p>_agents`, `mm_<p>_nodes` | the node itself (enforced) | overwritten by heartbeats |
| Task records | KV `mm_<p>_tasks`, key `<node>.<agent>.<task>` | owner node only (enforced) | 5 revisions of history |
| Artifacts | object store `mm_<p>_artifacts`, or where the file lives | publisher | until deleted (no GC yet) |
| Run logs | `<data_dir>/runs/<task>.attemptN.log` | owner node | permanent |

Why both SQLite and KV: SQLite makes each node crash-safe and able to work offline, with no
distributed transactions. The KV record is the owner's *published view*, so anyone can run
`agentctl tasks --all` and see where a task stands, even while the requester is offline.

## 5. Networking & security

- The only network service is NATS, on 4222. The monitoring port 8222 is bound to 127.0.0.1.
  Put it on a Tailscale/WireGuard address and never on a public interface. With a public endpoint,
  use the generated `tls {}` block.
- `agent-node server-config` generates one user per node. The server enforces:
  - node X may publish messages only on subjects ending in `.X`, so it cannot impersonate another node;
  - node X may write only its own registry card and its own task records;
  - receivers additionally drop envelopes whose `from` node disagrees with the subject.
- Agent-level policy on the receiver: `accept_from` globs, plus permissions per request kind
  (`query→READ`, `artifact→PUBLISH_ARTIFACT`, `experiment→RUN_EXPERIMENT`, `code→WRITE_WORKTREE`).
  Sending requires `REQUEST_TASK`, and a process can only act as agents configured on its own node.
- Runtime sandboxing follows the permissions. Claude workers get `Read/Glob/Grep` plus the mutmuas
  MCP tools, `Edit/Write/Bash(git:*)` with WRITE_WORKTREE, and `Bash` with RUN_EXPERIMENT. Codex
  gets `--sandbox read-only`, or `workspace-write` with WRITE_WORKTREE/RUN_EXPERIMENT. It ignores
  `~/.codex/config.toml` by default and pre-approves only the mutmuas MCP tools.
- `MERGE` and `ADMIN` exist in the permission model. Nothing merges automatically in Phase 1.

**Known gap:** nodes need `$JS.API.>` to manage their own consumers, and that also lets a node read
other mailboxes or delete streams. Closing it needs NATS accounts with scoped JetStream permissions
(decentralised JWT auth). That is a Phase 2 item. Until then, treat all nodes of a project as
mutually trusted machines of one lab.

## 6. Deployment

- **Server**: `nats-server` native binary with systemd (Linux) or brew/launchd (macOS), or Docker
  Compose (`deploy/docker-compose.yml`). The native binary is the default because it is one file,
  it is identical on macOS and Linux, and it needs no Docker Desktop on Macs.
- **Nodes**: `scripts/install.sh` (venv inside the repo), then `agent-node init/join`, then
  `agent-node service --write` (launchd plist on macOS, systemd user unit on Linux). macOS is a
  first-class node. Nothing assumes Linux or Docker.
- Full procedure: `DEPLOYMENT.md`.

## 7. Failure handling

| Failure | Behaviour | Tested by |
|---|---|---|
| Receiver offline / never registered | Stream keeps the message; its consumer starts from the beginning when it first comes up | `test_durable_inbox.py`, e2e 1b |
| Sender offline after sending | RESULT waits in the sender's durable mailbox; the shared task record shows progress | `test_sender_offline_after_sending` |
| Node restart mid-task | Process group killed; on start `recover()` re-queues, attempt 2, requester told "restarted" | `test_node_restart_resumes_running_task` |
| Duplicate delivery | Server dedup (`Nats-Msg-Id`), ledger dedup (message_id), task dedup (task_id) | `test_duplicate_delivery_executes_once` |
| Network interruption | Outboxes on both sides hold messages; auto-reconnect; flush | `test_network_interruption` |
| Agent process crash | RESULT `failed` with exit code and log path | `test_agent_process_crash_is_reported_failed` |
| Task timeout | Process group killed at deadline, RESULT `failed` | `test_task_timeout_kills_process` |
| Agent claims success but crashed / gave no result | Downgraded to `partial`, with limitations stating why | `test_unstructured_or_dishonest_results_are_not_complete` |
| Artifact unavailable / corrupted | `ArtifactUnavailable` naming the node that has it / checksum error | `test_artifact_unavailable_and_integrity` |
| Invalid message | ERROR to the sender; node keeps working | `test_invalid_message_does_not_break_the_node` |
| Permission denied | REJECT with the exact missing permission / sender | `test_permission_denied` |
| Impersonation | Server refuses the publish; receiver drops mismatches | `test_generated_server_auth_enforces_node_identity`, `test_spoofed_sender_is_dropped` |
| Repeated crashes | After `max_attempts`, RESULT `failed` ("gave up") | code path in `node._execute` |

## 8. Design rules applied

- **No abstraction without two real uses.** Runtimes: script + claude-code + codex. Artifact schemes:
  object + file (+ http). There is no plugin framework, no workflow DSL and no event sourcing.
- **Honesty is structural.** `RESULT.status ∈ {complete, partial, failed}` is validated, and the daemon
  never upgrades a result.
- **Observable by construction.** Every task has a thread (`agentctl task <id>`), a shared record
  (`agentctl tasks --all`), a raw audit trail (`agentctl history`) and run logs.

## 9. Replaceability

| To replace | Touch only |
|---|---|
| NATS with another bus | `bus.py` (publish, mailbox pull, KV, object store) |
| Object store with S3/MinIO/DVC | `artifacts.py`: add a scheme; the protocol is unchanged |
| SQLite with Postgres | `ledger.py` |
| Add a provider (Gemini CLI, local model, …) | a `SubprocessRuntime` subclass in `runtime.py` (build argv, parse output) |
| Agent tool surface | `tools.py` (shared by MCP and CLI) |
