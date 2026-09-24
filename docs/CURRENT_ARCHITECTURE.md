# Current architecture (review before Phase 1)

Date: 2026-09-24. Scope: the `mutmuas/` directory, which is the project root.

## 1. What existed

The review found an **empty directory**: no source code, README, docs, configs,
deployment files, agent adapters or state stores. `mutmuas/` got its own git repository.

| Review item | Finding |
|---|---|
| Project structure | none |
| README / docs / configs | none |
| Communication architecture | none |
| Agent adapters | none |
| State persistence | none |
| Deployment | none |
| Parts worth keeping | none, since nothing existed |
| Parts that obviously need refactoring | none |

## 2. KEEP / REFACTOR / REPLACE / DELETE

With no existing code, every category is empty:

- **KEEP**: –
- **REFACTOR**: –
- **REPLACE**: –
- **DELETE**: –

So Phase 1 is a new build, and there is no migration strategy for old code.
The migration question moves to *future* phases. `ARCHITECTURE_V1.md` lists
every component and the interface it is replaceable behind.

## 3. Existing external components that were evaluated

The brief says to combine mature components rather than rebuild messaging.

| Need | Options considered | Chosen | Why |
|---|---|---|---|
| Durable messaging | NATS JetStream, Redis Streams, RabbitMQ, Kafka, ZeroMQ, plain HTTP | **NATS JetStream** | A single static binary (macOS and Linux) with durable per-consumer mailboxes, ack/redelivery, server-side dedup (`Nats-Msg-Id`), KV and object store built in. One server replaces a queue, a registry DB and a blob store for Phase 1. Kafka and RabbitMQ are heavier to run. Redis has no object store, and its persistence semantics are weaker. ZeroMQ and HTTP have no durability. |
| Registry / presence | etcd, Consul, a DB table | **NATS KV** | Already in the same server. The per-key publish permissions give "a node writes only its own card". |
| Artifacts | MinIO/S3, DVC, git-lfs, NATS object store, shared FS | **NATS object store + `file://` refs** (S3/DVC later) | No extra service. Chunked with sha256 check, fine up to a few GB. Huge data stays where it is as a `file://NODE/path` reference. |
| Task ledger | Postgres, SQLite, event store | **SQLite per node + NATS KV mirror** | Each node owns its truth locally (crash-safe outbox/inbox). The owner publishes a read-only record so that any node can observe it. No distributed DB. |
| Agent protocol | Google A2A, MCP, free chat | **A2A-inspired envelope** over NATS; **MCP** as the agent-facing tool interface | A2A's task/message/artifact split and AgentCard ideas are adopted without its HTTP transport, because the brief requires offline durability that point-to-point HTTP does not provide. MCP is what Claude Code and Codex already speak natively. |
| Overlay network | Tailscale, WireGuard, public TLS | **Tailscale (or WireGuard) recommended**, TLS supported | Nothing gets exposed publicly, and both work on macOS and Linux. |

**Cotal.** No Cotal component exists in this repository. I could not verify what
"Cotal" refers to, so I did not guess at it. If it is a specific
project, point me to its repository or docs and I will evaluate it against the
interfaces in `ARCHITECTURE_V1.md` §9, where messaging is isolated behind `bus.py`.

## 4. Proposed architecture

See `ARCHITECTURE_V1.md`. In short: one NATS JetStream server, and one
`agent-node` daemon per machine that manages its agents. The CLI (`agentctl`) is
for humans. The MCP server (`agentctl mcp`) is for LLM agents.
