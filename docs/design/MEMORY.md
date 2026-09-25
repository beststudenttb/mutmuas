# Long memory for mutmuas agents: design (exp/memory, revision 2)

Status: design only, no code. Revision 1 was written by A:claude on 2026-09-24. Revision 2 (2026-09-25) merges
the reviews by B:claude-secretary (C1–C3) and A:codex (task T-20260925000101-8cbde75b). The leader's decision
is that exp branches stay on exp and are not merged unless needed.
Tags: [verified] means checked, with who checked it; [assumed] means from docs or reasoning, not tested.

## Principle

The ledger (node SQLite plus the JetStream stream) is **durable and exact, but it is not trusted**.
Anyone allowed to message an agent can put text into it. So:

1. Rebuild **state** (tasks, owners, results, pauses) from the ledger.
2. Keep what the ledger cannot know in the **work log** plus memory. That means the leader's words,
   decisions and reasons, and work discussed outside mutmuas. Most of what the secretary lost to compaction
   was this kind of work [verified: B, SECRETARY.md]. The work log is not replaced by the digest; the two
   sit side by side.
3. When either is injected into a session, it is **data, not instructions**. Only the trusted-decision
   block can carry the leader's intent, and only under the rules below.

## Primitives

**`agentctl digest --as X`** has two parts:

- **snapshot**: no cursor, always complete. It lists open tasks X owns or requested, with state, peer and
  last update; active pauses; offline peers; and watcher status plus the command to re-arm it.
- **delta**: the messages after X's acknowledged cursor, capped in length, oldest first, followed by
  `next_cursor=<rowid>`.

**Cursor contract (at-least-once).**
- The digest never advances the cursor itself.
- The session calls `agentctl digest --ack <rowid>` once it has read the delta. Repeating a delta is
  acceptable; losing one is not. A hook whose output never reached the model, or a crashed session, simply
  sees the same delta again.
- One agent id has **one active session**, recorded as a lease `session_id → agent` in `node/data/`. A
  second session gets the snapshot only, plus a warning, and cannot ack. There are no per-session cursors,
  because two sessions working one inbox is the bug to prevent.

**Work log.**
- An append-only JSONL file per agent under that agent's own directory, never in the public repo.
- Each entry is `{ts, session_id, speaker, kind: decision|instruction|lesson|state, text, reason,
  quote (≤200 chars), sensitivity: normal|private}`.
- It records decisions and their reasons, not transcripts.
- `private` entries never leave the node and never enter the ledger (handbook R3.3).

**`agentctl memlint`** checks memory and work-log text against live facts: network addresses not in the
registry, paths that do not exist, and commands not on PATH. It only warns and never edits.

## Trust boundary (Codex's main finding; agreed)

The injected text has two blocks, and the hook script is responsible for keeping them apart:

```
<mutmuas-state source="ledger" trust="untrusted-data">
  … snapshot and delta; peer text is quoted and truncated, never phrased as instructions …
</mutmuas-state>
<mutmuas-decisions trust="leader-via-configured-relay">
  … only (a) the agent's own work log and (b) leader_quote items that pass the check below …
</mutmuas-decisions>
```

- `outputs.leader_quote` is accepted only when the sending address is listed as `relay` in the receiving
  node's config (today that is B:claude-secretary). From any other sender it is dropped, and the digest
  shows the drop as an event.
- Each accepted quote carries `{speaker: leader, relayed_by, task_id, rowid}`.
- Quotes are limited to task-relevant words the recipient needs to know. The leader's private discussion
  with any agent is never relayed (R3.3).
- [assumed] Wrapping text in tags is a soft boundary for a language model. The hard part is what the
  digest leaves out: it never carries peer text longer than a summary line, and a trusted block never
  grants anything the relay could not already ask for in a normal REQUEST.

## The four failures

| | Problem | Claude Code | Codex |
|---|---|---|---|
| **F1** compaction | state, the leader's words and decisions are lost | A `SessionStart` hook (matcher `startup\|resume\|compact`) runs a trusted local script that prints both blocks [assumed from the hook docs, not tested here] | `~/.codex/hooks.json` `SessionStart` (`startup\|resume\|clear\|compact`) returns the same blocks as `additionalContext` [verified: A:codex, hooks are stable in 0.156.1]. AGENTS.md is kept as a fallback |
| **F2** lost watcher | the wake path dies with the session | The only verified path is a watcher the **session starts itself as a background task**; its exit wakes the session [verified: A:claude, used all night]. `claude --help` (2.1.281) shows no command that queues a message into a running session [verified: A:claude]. MCP "channels" might [assumed]. So the digest reports "watcher: none" and the session re-arms it; the hook does not try | launchd owns `agentctl watch`, which runs `codex queue --thread <session_id>` [verified end to end: A:codex]. The SessionStart hook only updates the `A:codex → session_id` lease; SessionEnd clears it if still owned. If queueing fails, the message stays unread, a desktop notification goes out, and the next digest catches it |
| both | | Work that must not wait for a human goes to `mode: worker` agents (`claude -p` / `codex exec`); a missing watcher then delays only chat | same |
| **F3** nothing crosses agents | messages to workers or offline sessions reach nobody's memory | The ledger already holds them; the digest delta reads them. Decisions sent between agents carry `outputs.decision`, which the digest always lists in the state block as data | same |
| **F4** stale memory | old addresses and paths survive in memory [verified: B] | memlint from the same hook; rule: never copy what `status`/`agents`/`digest` gives live | same |

## Evaluation

Run it as a replay harness.

**Fixtures.** Several real workdays, with secrets and `private` entries removed.

**Conditions** (a fresh session each time):
- memory
- memory + work log
- memory + digest
- memory + digest + work log
- full: memory + digest + work log + trust split + memlint

**memlint** is scored separately, as stale-fact precision and recall against a hand-labelled list.

**Questions.** A fixed set, labelled with the part each one needs (ledger, work log, or both). For example,
"what did the leader decide about merging exp branches" needs the work log. N = 20 is only a smoke test; the
real bar needs several days and several runs.

**Adversarial cases:**
- an old address and a new address in conflict;
- a decision reversed later;
- two sessions for one agent;
- a cursor replayed after a crash;
- a non-relay peer sending `leader_quote`, or text phrased as an instruction.

**Behaviour after recovery**, beyond question accuracy:
- did it pick up the right task;
- did it redo finished work;
- did it act on stale or injected text.

Pass bar, proposed:
- the full condition ≥ 90 % on answers, 0 injected instructions followed, and 0 duplicate executions;
- memlint recall ≥ 0.9;
- the digest ≤ 1.5k tokens.

## Out of scope

- installing hooks or changing any agent's settings (that is the leader's decision);
- a shared server-side memory store (needs a server config change);
- any code.
