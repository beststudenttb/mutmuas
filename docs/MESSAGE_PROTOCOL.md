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
| `REQUEST` | requester → owner | `objective`, `reason` | `kind`, `inputs`, `expected_outputs`, `constraints`, `acceptance_criteria`, `deadline`, `deadline_default`, `timeout_s`, `parent_task`, `reply`, `leader`, `project` | creates task (PENDING) |
| `ACK` | owner → requester | – | `state`, `message` | ACCEPTED (or RUNNING when an interactive agent accepts) |
| `UPDATE` | owner → requester | `message` | `state` (task state), `progress` | `state` if given |
| `QUESTION` | either | `question` | – | requester side: WAITING |
| `ANSWER` | either | `answer` | – | – |
| `BLOCKED` | owner → requester | `reason` | `needs` | BLOCKED |
| `RESULT` | owner → requester | `status`, `summary` | `outputs`, `evidence`, `limitations`, `follow_up`, `how`, `notes` | COMPLETED (complete/partial) · FAILED (failed) |
| `REJECT` | owner → requester | `reason` | – | FAILED |
| `CANCEL` | requester → owner | – | `reason` | CANCELLED (running process is killed; so are its registered background jobs' processes) |
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
  - A REQUEST that wants a reply but names no deadline gets one from the requester's node:
    `default_reply_deadline_s` in node.yaml (default 4 h; `0` = no default). With `timeout_s` it is never
    earlier than `timeout_s` + 30 min, so a long task is not chased while it still runs.
  - Such a filled-in deadline is marked `deadline_default: true` in the REQUEST body, so the owner can tell it
    from one the requester chose. An explicit `deadline` is kept as given; notices (`reply: none`) get none.
- `leader: true` on a REQUEST: the leader asked for this task (D-049). The session sending it on his behalf sets it
  (`agentctl ask --leader`, `leader` in a `send` file or in the MCP `send_request`); it is not checked (D-035).
  A worker's queue runs such tasks first, the rest in arrival order; a running task is not stopped. The
  session's inbox lists them first; a watcher's `--peek` keeps arrival order (its cursor is the last row).
- **A long backlog** (D-074): a session's inbox page holds 50 messages: the leader's first, then the newest. The
  MCP `inbox` (and `agentctl inbox`) says how many unread there are, how many it listed and how many older ones
  it left out, with the `before_seq` that pages back to them (`agentctl inbox --before-seq <seq>`). Reading marks
  only what was listed; a notifier's `since` cursor is unchanged (oldest first).
- **Delivery log** (D-052): when an owned task gets its RESULT, the owner's node appends the handbook R7.11 line
  to the post's `worker-log.md` (`time | from | task | output / to whom | how | notes`), taking `how` and `notes`
  from the RESULT ('未填' when missing). If the post's `PLAN.md` has a heading naming the task id, that section goes
  into `outputs.plan` and off the board. Notices closed by being read (`reply: none`) are not logged.
- **Long jobs** (D-050): the owner of a task registers a background job it waits on (`agentctl job add --pid
  <pid> --done-file <path> --log <path> --note <line>`, or the MCP `add_job`). The task becomes WAITING, and a worker
  may end its run without a result: it is neither finished nor retried, and a restart leaves it alone. Each
  heartbeat checks every open job; a job has ended when its process is gone (same machine; the exit code is not
  known, so write it into the done-file) or its done-file exists. When the task's last job ends, a worker's task is
  queued again as a fresh run (attempts reset; the prompt says how each job ended). A session instead gets a note
  in its own inbox with `next` set to itself, which wakes it. There is no time limit; `whoami` lists
  `jobs_waiting`.
- **Projects** (D-069/D-072): one address per post, one directory per project under the post directory
  (`work/<post>/<project>/`). A REQUEST may name its `project` (`send_request(project=…)`, `agentctl ask
  --project`); without one it belongs to the post's `default_project` (node.yaml), or to the post directory
  itself when that is unset. The owner's node refuses a request whose project is not a real directory by that
  exact name right under the post directory (missing, a link, or another spelling of it: "run post-init first"). A worker starts in the project directory, and delivery reads that directory's PLAN.md and
  appends to its worker-log.md. A session started in a project directory takes only that project's requests
  (its inbox and wake-ups leave out the others, and the worker runs them); a session in the post directory
  takes all of them, as before. Which project a session is in is found by file identity, so a case variant of
  the directory or a link into it counts as that project. An agent without `auto_worker` has nobody to hand
  the rest to, so its session takes and sees all work wherever it was started.
- **Interrupt, pause, resume** (D-089: "my instruction can interrupt directly"):
  - A message marked by the leader (`leader: true`) or `priority: high` interrupts a worker in the middle of a
    run when it is an UPDATE or ANSWER about the task that worker runs, or carries `interrupt: true` (then it
    stops whatever the post's worker runs). The owner's node stops the run (the whole process group), keeps the
    message on the task (`tasks.interrupts`) and lays the task out again; the next run's prompt starts with
    it, and a brain resumes the same conversation. The node names a new brain conversation itself
    (`claude -p --session-id`) before the run, so a run stopped midway can still be resumed. The requester is
    told ("interrupted"). The attempt is not counted as a failed one.
  - `pause: true` (from the leader, priority high, or the task's requester) stops a running worker and keeps
    the task WAITING and `paused` until `resume: true`; nothing restarts a paused task (heartbeat, recover).
    A session's task is marked the same way and the session reads the message.
  - Pause and resume travel down `parent_task` to the open child tasks on whatever node they run, and from
    there further down; CANCEL already did (D-066).
- **Follow-up and receipts** (D-073 batch 2, D-076):
  - *Arriving work goes on the plan*: the owner's node adds `- [ ] <task> from <sender>: <first line>` to the
    `## 收件` section of the project's PLAN.md, and the receipt (the PENDING UPDATE, or a worker's ACK) says its
    place in the queue (`position`, an estimate: the leader's work goes first). Delivery, refusal or withdrawal
    removes the line. Internal subtasks are not added (their brain writes their lines).
  - *Push*: the session's MCP server pushes each new message that needs the session once, and when it starts
    (opening a session, /mcp reconnect) every unread one again, oldest first. It does not push again and again
    while the session works; the Stop hook has the agent look before it ends a turn. Each push is counted
    (`messages.pushed`, `pushed_at`); `whoami` shows `inbox_unread`, `oldest_unread_s` and `last_push_at`.
  - *eta*: `accept_task(eta=…)` and `report_progress(eta=…)` (ISO time with timezone) send the owner's estimate;
    the requester's node keeps it (`check_task` shows it). When it has passed, the requester's node reminds the
    owner once (an UPDATE with `next`, asking for a new eta). No new eta within an hour: the requester is told,
    and the node's `escalate_to` addresses (the secretary) get a copy. A new eta starts over. WAITING (on a job
    or subtasks) and BLOCKED work is not chased; an owner whose node is offline is not chased; a session that is
    gone with no worker behind it is reported instead of chased.
  - *depends_on*: `send_request(depends_on=[…])` names tasks this node knows. The request is held on the
    sender's node (`delivery: held`) and sent once they are all done, with their summaries in
    `body.dependencies` and their artifacts attached. Only tasks the sender takes part in can be named, and
    only artifacts it may see travel with it (checked again when it is sent). If one failed, was refused or
    withdrawn, it is not sent: it fails and its sender is told. Withdrawing a held request drops it (no CANCEL
    goes out). While held it is not chased, and its default reply deadline starts when it is sent.
  - *nudge*: `nudge(task_id, note)` reminds the owner of a task we requested: it is woken (`next`) and must
    answer what it does, where it is stuck and a new eta. At most once in three hours per task. The owner's
    node lays out again a worker's task that is neither running nor queued.
  - *off*: `agentctl session off` (the launcher's `mutmuas <post> off`) keeps the session online but gives
    it no work: the worker takes new requests and the session's inbox leaves them out; `session on` undoes it.
    A post without a worker (auto_worker) cannot switch off: nobody else would take the work.
- **Brain and subs** (D-073): a post's worker runs are its *brain*. With claude-code, its runs for one project
  share one conversation (`claude -p --resume`) while work keeps coming; after `brain_batch_idle_s` (node.yaml,
  default 1800) with no brain run, no sub running and no job waited on, the node forgets the conversation and the
  next run starts afresh from HANDOFF/PLAN (the brain updates them every run). Brain runs of a post are serial.
  Codex brains start afresh each run for now. A brain's long work goes to *internal subtasks*:
  `send_request(to=<itself>, internal=true, model=…)`, only for a post with `auto_worker`. The owner's node
  refuses one from anyone else, for a plain worker address, or for another project than its parent's (it always
  belongs to its parent's project). It is always run by the worker (never the session, even one online), from
  its own pool (`max_concurrent`), on the requested model or the latest Sonnet. It is left out of the session's
  inbox. Its worker, recognised by the process tree like the lease, gets only `report_progress`,
  `submit_result` and `add_job`: MCP, agentctl and the mail functions underneath refuse the rest. This is a
  division of work, not a sandbox: the worker runs as the same user and could read the node's files directly.
  It writes its outputs under `runs/<task>/` and does not write PLAN/HANDOFF: the node marks the line
  naming it on the brain's PLAN.md (`[>]` on progress, `[x] — summary` when done, `[!] — why` when failed or
  blocked). The brain waits for its subs with `add_job(children=True)`.
- **Waiting on child tasks** (D-066): requests an owner sends while working on a task carry `parent_task` (set
  automatically inside a worker run). `add_job(children=True)` makes the task wait on its direct children; so does
  reporting `state: WAITING` while a child is open. The wait ends once every child has a result, was refused or
  cancelled, or is past its deadline (reported as overdue and left running: the parent decides; each child is
  reported overdue once, so a wait registered again afterwards lasts until that child really ends). The task is then
  woken once, as for a background job, and the wake-up lists how each child ended. Cancelling a task sends CANCEL
  to its open children (their nodes cascade further down).
- `next: <address>` on RESULT, UPDATE, QUESTION or ANSWER names whose move it is. That agent is woken exactly
  as by a REQUEST, even by an UPDATE. Put it on the last message of every thread whose next step belongs to
  someone.

### Waking, presence and follow-ups

- **Wake view.** What wakes a session (`inbox --only wake`, a waiting watcher, the push): REQUEST, QUESTION,
  ANSWER, BLOCKED, REJECT, CANCEL, ERROR, and any message whose `next` names the agent. The RESULT of one's own
  request does not, unless the agent has `wake_on_own_results: true` in node.yaml (per agent, default off); then
  the RESULT of its own request that wants a reply (`reply` not `none`) wakes it too.
- **Push.** `agentctl mcp --channel` declares the Claude Code `claude/channel` capability. When a message
  reaches the ledger that would wake the agent (see the wake view), the MCP server pushes one
  line into the running session:
  `mutmuas: new REQUEST from A:x (task T-…): <first 80 characters>`, with meta
  `{task_id, msg_type, sender, summary}`.
  - Start the session with `claude --dangerously-load-development-channels server:mutmuas` until the
    channel is approved.
  - The push never marks anything read. When the MCP server starts (a new session, /mcp reconnect) it pushes
    every unread message that needs the session once more, oldest first (D-073 batch 2); after that each new
    one once.
  - Codex keeps `agentctl watch` → `codex queue`.
- **Presence.** The session's own mutmuas MCP process writes a heartbeat to the node ledger every 15 s. The
  registry card of an interactive agent then shows one of:
  - `session: online`, or `offline` (no heartbeat for 50 s, or the process has gone);
  - `unknown` (no mutmuas MCP server has ever run for it);
  - `session_warning` when the session was started outside the agent's workdir. Claude Code keeps memory
    per start directory, so a wrong start directory means an empty memory.
- **One agent, one session.** The first session's MCP process holds the agent (a lease in the node
  ledger). The check and the write are one transaction, so two sessions starting together cannot both win.
  - A second session acting as the same agent is told at once and receives no mail pushes.
  - Its MCP tools (all but `whoami`) refuse to act.
  - `agentctl` commands for that agent are refused too, unless they run inside the holding session: a
    descendant of the session process, such as its shell. The ancestry comes from `/proc` or `ps` by absolute
    path, and no environment variable exempts a process.
  - Exceptions, because they show no mail content: `status`, `agents`, `find`, and `watch --headers-only`
    (a notifier service outside the session: count, type and sender only). `whoami` is exempt only as an MCP
    tool; `agentctl whoami` needs the session.
  - Only a foreground `inbox` listing counts as having shown a message; `clear_inbox` marks read only messages
    shown that way. Notifier reads (`watch`, pushes) do not count.
  - The holder is told about the contender. When the holder closes, the other session takes over at its
    next heartbeat. Two projects on one machine are two agents (e.g. `C:paper` and
  `C:course`), not two sessions of one agent.
- **Follow-ups.** Every 30 s the requester's daemon checks the tasks it is owed. Each follow-up is sent
  once: an UPDATE to the requester with `next` set to the requester (so it wakes), copied as an FYI to
  `escalate_to` in node.yaml (e.g. the secretary). There are two:
  - `overdue`: a reply is required, the deadline has passed, and there is no RESULT yet;
  - `session_offline`: the owner is interactive, its node is up, its session is offline, and the request
    is still PENDING.

  Nothing is chased while the owner's node is offline (a closed laptop).
- **Reading.** Only the session marks mail read. Pushes, watchers and scripts use `--peek`.
  - The MCP `inbox` tool lists only what needs the agent by default (`only="wake"`).
  - A backlog the session has already dealt with elsewhere is cleared explicitly, after looking at it:
    `clear_inbox(before_seq)` in MCP, or `agentctl inbox --clear-before SEQ`.
- **Refused but seen.** A REQUEST that an interactive agent may not take, because of a wrong `kind` or a
  missing permission, is still refused: the requester gets REJECT and the task is FAILED. But if the sender
  is in the agent's `accept_from`, the request also shows in the agent's inbox with
  `note: "rejected: …"`, and it wakes the agent. Requests from senders outside `accept_from` stay invisible.
- **Later.** `remind_me(at, text, every=None)` in MCP stores a reminder in the node ledger. When it is due the
  node daemon puts it into the agent's inbox as a note with `next` set to the agent (task id `reminder-<id>`), so
  it wakes a session like new mail and waits there while none runs; no session lease is involved (D-066).
  `every="5h"` repeats it one interval after each delivery until `cancel_reminder(id)`. Use it instead of
  promising to come back, and instead of scripts that act as the agent on a timer.

### Visibility (step 1: minimal exposure by default)

The leader's rule: everyone may know what state a task is in; nobody may look into someone else's work.
There are four layers (`src/mutmuas/visibility.py`):

| layer | who | what |
|---|---|---|
| public | everyone | address, role, capabilities, provider, mode, `accepts_kinds`, online/offline, `session` on duty, `availability` available/busy |
| task status | coordinators + participants | task id, first 80 characters of the objective, status, requester → owner, last update |
| task content | participants only: requester, owner, `observers` | reason, inputs, the thread, the RESULT, artifacts |
| private | nobody | session reasoning, memory, work logs, transcripts, raw run logs. Never sent over mutmuas; ask the person, who answers with a condensed summary |

- Shared stores carry only the public and status layers. The registry card has no current task, queue,
  inbox counts or session directory; the agent reads those itself with `whoami`. The task KV has no reason,
  inputs, thread, result or artifact references.
- Every tool filters by viewer:
  - `task`, `result` and MCP `check_task` return nothing to non-participants, and the status layer to
    coordinators;
  - `tasks --all` lists the viewer's own tasks, or every status record for a coordinator;
  - `history` lists only messages the viewer sent or received;
  - `artifact list` and `fetch` cover only artifacts the viewer published or was sent.
- `coordinators` is set in the HR-issued node.yaml, e.g. `[B:claude-secretary]`; an agent cannot make
  itself one.
- **Participants** come from what the node persisted for the task: its requester, its owner, the `observers`
  listed on its request, and agents holding an observer row. Sending mail on a task makes nobody a
  participant. Only the requester or owner may send on a task: `reply`, `question`, `answer`, and `send`
  with `--task`.
- `observers` go on a REQUEST (`agentctl ask --observer`), or any participant adds them later with
  `add_observer` / `agentctl observe`.
  - Observers receive FYI copies of the REQUEST and RESULT. The copies never wake them.
  - Each copy lists the task's participants. The observer's node keeps a copy only if both the sender and
    the recipient are on that list.
  - Every other participant is told `observers_add`, so the requester's side also forwards a later RESULT.
- A worker's `notify` lead gets the status layer only: task id, a short objective, status.
- Artifacts: the object store keeps no description (it travels in the participants' ArtifactRef).
- A session started outside its workdir is reported to the agent and to the coordinators, not on the card.
- **Not a security boundary.** Every process holding the node credential can still read:
  - the message stream;
  - the object-store bytes;
  - its node's whole ledger (the SQLite file).

  It can also pass `--as` for another agent of its node, because there is no process identity in step 1.
  Enforcement (the daemon as the only NATS principal, per-agent identity, or NATS accounts) is step 2.

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
