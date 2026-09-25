# Message protocol `mutmuas/1`

Every message is one JSON object, the **envelope**. There is no free chat: each type has a
required body shape, and invalid messages are answered with `ERROR`. The source of truth
is `src/mutmuas/protocol.py`.

## Envelope

```yaml
protocol: mutmuas/1
message_id: msg-4f0c…            # unique; dedup key end to end
conversation_id: conv-9a1b…      # shared by all messages of one exchange
task_id: T-20260924075412-fdf21dbc   # required for everything except REQUEST (creates it) and ERROR
from: A:main                     # NODE:agent  (must match the node in the NATS subject)
to: B:experimenter
type: REQUEST
timestamp: 2026-09-24T07:54:12.207+00:00
priority: normal                 # low | normal | high
body: { … }                      # type-specific, below
artifacts: [ ArtifactRef, … ]    # references only, never payloads
reply_to: msg-…                  # message this one answers (optional)
```

Addresses and ids used in subjects allow `[A-Za-z0-9_-]` only.

## Types and bodies

| type | direction | required body | optional body | effect on task |
|---|---|---|---|---|
| `REQUEST` | requester → owner | `objective`, `reason` | `kind`, `inputs`, `expected_outputs`, `constraints`, `acceptance_criteria`, `deadline`, `timeout_s`, `parent_task`, `reply` | creates task (PENDING) |
| `ACK` | owner → requester | – | `state`, `message` | ACCEPTED (or RUNNING when an interactive agent accepts) |
| `UPDATE` | owner → requester | `message` | `state` (task state), `progress` | `state` if given |
| `QUESTION` | either | `question` | – | requester side: WAITING |
| `ANSWER` | either | `answer` | – | – |
| `BLOCKED` | owner → requester | `reason` | `needs` | BLOCKED |
| `RESULT` | owner → requester | `status`, `summary` | `outputs`, `evidence`, `limitations`, `follow_up` | COMPLETED (complete/partial) · FAILED (failed) |
| `REJECT` | owner → requester | `reason` | – | FAILED |
| `CANCEL` | requester → owner | – | `reason` | CANCELLED (running process is killed) |
| `ERROR` | either | `code`, `message` | – | requester side: FAILED |

`kind` (REQUEST) selects the permission the owner must hold:

| kind | meaning | owner needs |
|---|---|---|
| `query` (default) | answer / look something up | `READ` |
| `artifact` | find or produce data and hand it back | `PUBLISH_ARTIFACT` |
| `experiment` | run a test / training / evaluation | `RUN_EXPERIMENT` |
| `code` | change code; runs in its own git worktree, returns branch + patch | `WRITE_WORKTREE` |

### RESULT honesty rule

`status` is exactly one of `complete | partial | failed`, and anything else is rejected.

- `complete`: every acceptance criterion is met.
- `partial`: there is usable output, but not all criteria are met. The task is COMPLETED, and `result_status` shows `partial`.
- `failed`: nothing usable.

The daemon never upgrades a status. If the agent process exits non-zero while claiming
`complete`, the result is downgraded to `partial` and a limitation is added. If there is no
structured result at all, the result is `partial` (exit 0) or `failed` (non-zero).

### Replies, deadlines and the baton (borrowed from email)

- `reply` on a REQUEST: `required` (the default) means the owner owes a RESULT. `none` makes it a notice.
  The receiving session reading it, with a plain `inbox` and not `--peek`, closes the task with a read receipt
  (`RESULT complete "read by X (no reply requested)"`).
- `deadline` (ISO 8601 with a timezone) says when the reply is needed. Use `agentctl ask --due +2h`.
- `next: <address>` on RESULT, UPDATE, QUESTION or ANSWER names whose move it is. That agent is woken exactly
  as by a REQUEST, even by an UPDATE. Put it on the last message of every thread whose next step belongs to
  someone.

### Waking, presence and follow-ups

- **Push.** `agentctl mcp --channel` declares the Claude Code `claude/channel` capability. When a message
  reaches the ledger that would wake the agent (a wake type, or `next` naming it), the MCP server pushes one
  line into the running session:
  `mutmuas: new REQUEST from A:x (task T-…): <summary>`, with meta `{task_id, msg_type, sender}`.
  - Start the session with `claude --dangerously-load-development-channels server:mutmuas` until the
    channel is approved.
  - The push never marks anything read. Mail that arrived before the session started is not pushed, so
    handle the backlog with `inbox`.
  - Codex keeps `agentctl watch` → `codex queue`.
- **Presence.** The session's own mutmuas MCP process writes a heartbeat to the node ledger every 15 s. The
  registry card of an interactive agent then shows one of:
  - `session: online`, or `offline` (no heartbeat for 50 s, or the process has gone);
  - `unknown` (no mutmuas MCP server has ever run for it);
  - `session_warning` when the session was started outside the agent's workdir. Claude Code keeps memory
    per start directory, so a wrong start directory means an empty memory.
- **Follow-ups.** Every 30 s the requester's daemon checks the tasks it is owed. Each follow-up is sent
  once: an UPDATE to the requester with `next` set to the requester (so it wakes), copied as an FYI to
  `escalate_to` in node.yaml (e.g. the secretary). There are two:
  - `overdue`: a reply is required, the deadline has passed, and there is no RESULT yet;
  - `session_offline`: the owner is interactive, its node is up, its session is offline, and the request
    is still PENDING.

  Nothing is chased while the owner's node is offline (a closed laptop).
- **Reading.** Only the session marks mail read. Pushes, watchers and scripts use `--peek`.

### Example REQUEST

```yaml
type: REQUEST
from: A:lead
to: B:representation
task_id: T-20260924-…
body:
  kind: artifact
  objective: Return the latent data of representation experiment 82
  reason: A needs it to continue the probing analysis of module A
  inputs: {experiment: 82, split: val}
  expected_outputs: [latents as .npz artifact, metadata json]
  constraints: [do not re-run the experiment]
  acceptance_criteria: [all 5000 validation samples, sha256 reported]
  timeout_s: 1800
```

### Example RESULT

```yaml
type: RESULT
task_id: T-20260924-…
body:
  status: partial
  summary: Found latents for 4200/5000 samples; shard 7 is missing on disk
  outputs: {samples: 4200}
  evidence: [ls /data/exp082/latents shows shards 0-6, 8-9]
  limitations: [shard 7 missing]
  follow_up: [re-run encoder on shard 7 (≈20 min GPU)]
artifacts:
  - {id: EXP082-LATENTS, uri: "artifact://visual_rl/B/representation/T-…/latents.npz", sha256: "…", size: 88412331}
```

## ArtifactRef

```yaml
uri: artifact://<project>/<key> | file://<NODE>/<abs/path> | https://… | git://<NODE><repo>@<branch>
id: EXP092-METRICS       # short label
size: 12345              # bytes
sha256: …                # verified on fetch when present
media_type: application/json
description: …
```

Directories are published as a tarball and unpacked on fetch. `file://` references are
fetchable only where that path is visible (the same node, or a shared filesystem). Use them for data
too large to copy.

## Task states

```
PENDING ─► ACCEPTED ─► RUNNING ─┬─► COMPLETED   (terminal)
   │           │          │  ▲  ├─► FAILED      (terminal)
   │           │          ▼  │  └─► CANCELLED   (terminal)
   │           │     WAITING / BLOCKED
   └───────────┴─► FAILED (REJECT)
```

Terminal states are sticky: later messages cannot move a task out of them.

## Transport (NATS)

| What | Subject / name |
|---|---|
| Message to `B:x` from node `A` | `mm.<project>.msg.B.x.A` (header `Nats-Msg-Id: <message_id>`) |
| Mailbox of `B:x` | durable pull consumer `inbox_B_x` on stream `MM_<project>_MSG`, filter `mm.<project>.msg.B.x.*` |
| AgentCard | KV `mm_<project>_agents` key `B.x` |
| NodeCard | KV `mm_<project>_nodes` key `B` |
| Task record | KV `mm_<project>_tasks` key `B.x.<task_id>` (written by the owner) |
| Artifact payload | object store `mm_<project>_artifacts` |

## Delivery guarantees

- **At-least-once** delivery into the receiver's ledger, **effectively-once** handling:
  1. the server drops re-publishes of the same `message_id` within 10 minutes;
  2. the receiver's ledger ignores a `message_id` it has seen before (forever);
  3. a REQUEST for a `task_id` the owner already has is never executed again. If the task is
     finished, the stored RESULT is re-sent.
- The order of messages from one sender to one agent is preserved (single stream, single consumer).
- Messages are retained for `message_retention_days` (default 30). A node that stays offline
  longer loses older messages.

## Versioning

New types go in `MESSAGE_TYPES` with their required body fields. Receivers reject unknown types
with `ERROR unknown_type`. A breaking change bumps the major version (`mutmuas/2`), and receivers
reject envelopes with a different major version (`unsupported_protocol`).
