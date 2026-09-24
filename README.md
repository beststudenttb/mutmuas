# mutmuas: multi-terminal multi-agent system

AI agents on different machines (a Mac, a GPU server, a cloud box) that find each other,
delegate work, report progress, return results with artifacts, and lose nothing while a machine
is offline. Different vendors (Claude Code, Codex, plain scripts) sit behind one protocol.

```
you ──► A:lead (your Claude Code session)
          │  find_agent("isaac_lab") → B:representation
          │  send_request(kind=experiment, objective, reason, acceptance criteria)
          ▼
        NATS JetStream  (durable mailboxes, registry, task ledger, artifact store)
          ▼
        B:representation (claude -p on the GPU server)  ACK → RUNNING → UPDATE… → RESULT + artifact://…
          │
A:lead ◄──┘ wait_for_result → fetch_artifact → continues its own work
```

This is **Phase 1**: communication, identity, tasks, artifacts, deployment and recovery. Research protocols,
scheduling and organisational structure come later (`docs/IMPLEMENTATION_PLAN.md`).

## Quick start (one machine, two simulated nodes)

```bash
scripts/install.sh            # .venv + nats-server binary, all inside the repo
scripts/test.sh               # 45 tests + multi-process end-to-end run (TEST 1 / TEST 2)
```

Two real machines: follow `docs/DEPLOYMENT.md`.

## Concepts

| Concept | Meaning |
|---|---|
| **node** | a machine running one `agent-node` daemon (`A`, `B`, …) |
| **agent** | `NODE:agent`, e.g. `B:representation`: a stable logical identity with a durable inbox. Provider and model are attributes and can change. |
| **worker** agent | the daemon runs incoming tasks itself via a runtime (`claude-code`, `codex`, `script`) |
| **interactive** agent | your own session; it gets the network as MCP tools (`agentctl mcp`) |
| **task** | created by a REQUEST; states PENDING → ACCEPTED → RUNNING → COMPLETED/FAILED/CANCELLED |
| **artifact** | data outside the bus; messages carry `artifact://`, `file://NODE/path`, `git://` references |

## Commands

```
agentctl status | agents | find <capability>
agentctl ask <to> "<objective>" --reason … --kind query|artifact|experiment|code [--input k=v] [--wait]
agentctl send <to> --file request.yaml
agentctl tasks [--all] | task <id> | result <id> [--wait] [--fetch DIR] | cancel <id>
agentctl inbox | accept | reject | update | submit-result | question | answer     (owner side)
agentctl artifact publish <path> | fetch <uri> | list
agentctl history [--task <id>]
agentctl mcp --as A:lead                      # MCP server for Claude Code / Codex

agent-node init | join | start | service [--write] | server-config | doctor
```

MCP tools available to agents: `whoami, list_agents, find_agent, send_request, check_task,
wait_for_result, cancel_task, inbox, accept_task, reject_task, report_progress, submit_result,
ask_question, answer_question, publish_artifact, fetch_artifact`.

## Layout

```
src/mutmuas/     protocol · bus (NATS) · ledger (SQLite) · hub · node daemon · runtimes ·
                 worktree · artifacts · tools · mcp_server · cli · server_config
config/          example-node-A.yaml (macOS), example-node-B.yaml (Linux GPU) — templates only;
                 real per-machine configs go in ~/.mutmuas/ or config/local/ (git-ignored)
deploy/          docker-compose.yml, nats-server.service
scripts/         install.sh, test.sh, e2e-local.sh, smoke-llm.sh, start-server.sh
examples/        workers/ping.py (deployment self-test)
tests/           cross_node_message, durable_inbox, delegated_task, artifact_transfer,
                 failure_recovery, worktree, mcp_tools, config, protocol
docs/            CURRENT_ARCHITECTURE · ARCHITECTURE_V1 · MESSAGE_PROTOCOL · DEPLOYMENT · IMPLEMENTATION_PLAN
```

## Status

- Verified on one macOS machine (Apple Silicon, Python 3.14, nats-server 2.15): the unit tests,
  in-process multi-node tests, multi-process E2E with per-node auth, and real Claude Code and Codex workers.
- **Not yet run on two physical machines or on Linux.** The Linux paths (systemd unit, install script
  branch) are written but untested. See the P1 list in `docs/IMPLEMENTATION_PLAN.md`.
