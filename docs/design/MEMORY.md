# Long memory for mutmuas agents: design (round 3, exp/memory)

Status: design only, no code. Written by A:claude and reviewed by B:claude-secretary on 2026-09-24.
Tags: [verified] means checked tonight; [assumed] means from docs or reasoning and not tested.

## Principle

The node ledger (SQLite) and the JetStream stream already hold everything that happened: every
message, task state, result and cursor. They are durable, shared and exact. Memory files are the
opposite: private, hand-written, and they go stale. So:

> **Rebuild working state from the ledger; keep in memory only what the ledger cannot know.**

The ledger cannot know the leader's own words, decisions and their reasons, preferences, or
lessons learned. Memory holds those, and holds *pointers* instead of copies for everything else,
such as "run `agentctl status`" in place of "B:main is the research agent".

## One new primitive: `agentctl digest`

`agentctl digest --as X [--since <cursor>]` returns a short, bounded text built only from the ledger:

- open tasks X owns, with their state and last update;
- open tasks X requested, and who is on each one;
- results and FYIs received since the cursor;
- the leader's messages since the cursor, verbatim;
- active pauses, and which peers are offline;
- whether this agent's watcher is running, plus the exact command to re-arm it.

The cursor is the ledger rowid, the same monotonic cursor `inbox --since` and `watch` already use
[verified]. It is stored per agent in `node/data/`, so the digest is read-only and costs nothing to repeat.

## The four failures

| | What went wrong tonight | Mechanism | Owner |
|---|---|---|---|
| **F1** context compaction | the leader's exact words and in-progress state were lost after compaction | Claude: a `SessionStart` hook (matcher `startup\|resume\|compact`) runs `agentctl digest` and its output enters context [assumed from the Claude Code hook docs]. `PreCompact` optionally appends the leader's quotes to a notes file [assumed]. Codex: AGENTS.md tells it to run `agentctl digest` first [assumed: Codex has no equivalent hook]. | A (src/: digest), leader (installs hooks; not changed tonight) |
| **F2** watcher lost | the in-session watcher is a child process of the session and dies with it; after resume or compaction it had to be re-armed by hand [verified: the pid lives under the session's shell] | (a) the digest reports "watcher: none" and gives the command, and the hook re-arms it; (b) structural fix: work that must not wait for a human goes to a `mode: worker` agent (`claude -p` or codex), so a missing watcher delays only chat, never tasks. `agentctl watch` under launchd already survives sessions but can only notify the desktop [verified] | A (digest), B (deploy of worker agents) |
| **F3** nothing crosses agents | what was sent to a worker or an offline session never reaches anyone's long-term memory | Nothing new to store: the ledger already has it. What's missing is *reading* it. Each agent's digest covers its own threads, and the node lead's digest also covers FYIs (the existing `notify`). For decisions, add a `decision` tag on RESULT/ANSWER (`outputs.decision: …`) that the digest always lists, so a decision is recorded once and read by everyone. | A |
| **F4** stale memory | the secretary's memory still names `~/mutmuas-claude`, `B:main` and `watch_a.sh` [verified by the secretary] | `agentctl memlint <memory dir>` flags network addresses that are not in the registry, paths that do not exist, and commands that are not on PATH. It runs from the same `SessionStart` hook and only warns, never edits. Rule: never copy a fact that `status`/`agents`/`digest` can give live. | A (memlint), each agent (fixes its own memory) |

## Measuring it (repeatable)

A replay test in the style of `tests/`, with no human in the loop:

1. **Fixture.** An export of a real workday's ledger with secrets removed. Tonight's exchange is a good one.
2. **Setup.** For each condition, start a fresh session given only that condition's context:
   - `memory-only` (today's baseline);
   - `memory + digest`;
   - `memory + digest + memlint`.
3. **Questions.** Ask N = 20 fixed questions whose answers the ledger fixes. For example:
   - which tasks A:claude has open;
   - what the secretary's review said about M1;
   - which branch holds the quota work;
   - what the leader said about merging to main (quoted);
   - whether a watcher is running and how to re-arm it.
4. **Scoring.** An exact-match or rubric check scored by a different model vendor, as the cross-vendor rule already recommends. Report accuracy per failure class (F1–F4) and the digest's token size.

Pass bar for merging, proposed: `memory + digest` ≥ 90 % overall, F4 questions 100 % flagged, and the digest ≤ 1.5k tokens.

## Out of scope tonight

Changing the leader's Claude settings or hooks, a shared memory store on the server (that needs a
server config change), and any code.
