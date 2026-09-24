# Implementation plan

Status as of 2026-09-24. ✅ done and tested · 🟡 partial · ⬜ not started.

## P0: the Phase 1 loop

"I sit at A, talk only to A's agent, it finds B's agent, delegates, gets the result, and continues."

| # | Item | Status | Evidence |
|---|---|---|---|
| 1 | Envelope + 10 message types + body validation + honest RESULT status | ✅ | `test_protocol.py` |
| 2 | Addresses `NODE:agent`, display aliases, provider/model as attributes | ✅ | `test_registry_and_discovery` |
| 3 | NATS JetStream topology, durable mailbox per agent | ✅ | `test_durable_inbox.py` |
| 4 | Local ledger: outbox-before-publish, commit-before-ack, dedup | ✅ | `test_duplicate_delivery_executes_once`, `test_network_interruption` |
| 5 | Registry (AgentCard/NodeCard) + presence + find by capability | ✅ | `test_registry_and_discovery` |
| 6 | Task ledger (local SQLite + owner-published KV record) + state machine | ✅ | `test_request_to_interactive_agent_roundtrip` |
| 7 | Node daemon: dispatch, runners, heartbeat, outbox retry, crash recovery | ✅ | `test_failure_recovery.py` |
| 8 | Runtimes: script, Claude Code (`claude -p`), Codex (`codex exec`) | ✅ | `scripts/smoke-llm.sh` passed for both real CLIs |
| 9 | Artifacts: object store + file refs, sha256, directories | ✅ | `test_artifact_transfer.py` |
| 10 | Agent-facing MCP server | ✅ | `test_mcp_tools.py` (real stdio client) |
| 11 | CLI `agentctl` (status/agents/find/ask/send/tasks/task/result/inbox/…) | ✅ | `scripts/e2e-local.sh` |
| 12 | Permissions: kind→permission, accept_from, REQUEST_TASK, act only as local agents | ✅ | `test_permission_denied` |
| 13 | Server config with per-node users; sender-node enforcement | ✅ | `test_generated_server_auth_enforces_node_identity` |
| 14 | TEST 1 (cross-machine data) + TEST 2 (delegated experiment), multi-process | ✅ | `scripts/e2e-local.sh`: 3 checks PASS |
| 15 | Deployment: install script, `agent-node init/join/start/service/doctor`, launchd + systemd, docs | ✅ | `docs/DEPLOYMENT.md` |

## P1: next, to make daily use comfortable

| # | Item | Why | Status |
|---|---|---|---|
| 1 | **Real two-machine deployment** (Mac A + Linux B over Tailscale) | Everything so far ran on one Mac: multi-process and multi-node, but one host | ⬜ |
| 2 | **Push new inbox items into interactive sessions** (Claude Code `UserPromptSubmit` hook that runs `agentctl inbox`) | Today an interactive agent sees requests only when it calls `inbox` | ⬜ |
| 3 | **Deliver ANSWER/QUESTION to running workers**: resume the worker session with the answer (`claude --resume`, `codex exec resume`) | Workers cannot yet hold a conversation mid-task; they finish partial and state the question in `follow_up` | ⬜ |
| 4 | Resume a crashed task *from its session* instead of restarting it | Long experiments restart from scratch today (attempt 2) | 🟡 attempts tracked, no session resume |
| 5 | Garbage collection: finished task records, old artifacts, task worktrees | KV and object store grow without bound | ⬜ |
| 6 | Scoped JetStream permissions (NATS accounts / JWT) | Closes the "$JS.API" gap described in ARCHITECTURE_V1 §5 | ⬜ |
| 7 | Resource-aware `find_agent` (free GPU memory from `nvidia-smi` in the heartbeat) | "Who has a GPU free?" | 🟡 static resources only |
| 8 | Deadlines enforced on the requester side too (auto CANCEL after `deadline`) | Only the owner-side `timeout_s` is enforced now | ⬜ |

## P2: Phase 2 foundations (only after the P0 loop has run in real use)

- S3/MinIO/DVC artifact scheme for multi-GB datasets and checkpoints.
- Research protocol message types: `EXPERIMENT_REQUEST`, `EVIDENCE`, `HYPOTHESIS`, `DECISION`, `REVIEW`.
- Experiment scheduling and GPU allocation across nodes.
- Shared project truth / hypothesis graph; multi-agent review; organisational roles.
- Web dashboard. The CLI covers the Phase 1 observability requirements.
- Merge queue for task branches (MERGE permission already exists in the model).
