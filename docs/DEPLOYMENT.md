# Deployment

This file is self-contained: a person or an agent should be able to set up the system
from it alone. Example throughout: project `visual_rl`, node **A** = macOS laptop, node
**B** = Linux GPU server that also hosts the NATS server. Replace names and addresses as needed.

```
   A (macOS)  ── agent-node ─┐                 ┌─ agent-node ── B (Linux, GPU)
                             └── NATS :4222 ───┘   (server runs on B, or anywhere)
                          private overlay network only
```

## 0. Prerequisites (every machine)

| Need | Check |
|---|---|
| Python ≥ 3.10 | `python3 --version` |
| git, curl | `git --version && curl --version` |
| Private network between machines (Tailscale recommended) | `tailscale ip -4` on each machine; the machines can ping each other |
| For Claude workers: Claude Code logged in *as the user the daemon runs as* | `claude -p "say ok"` |
| For Codex workers: Codex logged in | `codex exec --ignore-user-config "say ok"` |

Install on every machine (all state lives in the repo and in `~/.mutmuas`):

```bash
git clone <this repo> ~/mutmuas && cd ~/mutmuas
scripts/install.sh                      # .venv/ + .local/bin/nats-server
export PATH="$HOME/mutmuas/.venv/bin:$PATH"   # add to ~/.zshrc / ~/.bashrc
```

## 1. Network and security requirements

| Port | Service | Bind to | Who connects |
|---|---|---|---|
| 4222/tcp | NATS client port | the server's **Tailscale/WireGuard IP** (or 127.0.0.1 + a tunnel) | every node |
| 8222/tcp | NATS monitoring | 127.0.0.1 only | local admin |

Rules:
- Never expose 4222 without TLS, and never expose 8222 at all. With a public IP (no overlay network),
  generate the config with `--tls <public-ip-or-dns>[,127.0.0.1]`. This creates a private CA and a server
  certificate for those names. Copy `tls/ca.crt` (public, not secret) to every node and set `nats.tls_ca`.
  All traffic is then encrypted, and nodes verify the server's identity.
- Each node has its own NATS user. The server enforces that node X can only send as X and
  only write its own registry and task entries.
- `<NODE>.env` files are secrets (mode 600). Copy them over ssh/scp only.
- Current limitation: all nodes of one project can read the JetStream API, including other
  nodes' mailboxes, so only add machines you trust (see ARCHITECTURE_V1 §5).

## 2. Server (central message bus)

**Public IP instead of an overlay network** (e.g. a lab server at 150.89.170.193):

```bash
agent-node server-config --project visual_rl --nodes A,B --out ~/mutmuas-server \
    --listen 0.0.0.0 --tls 150.89.170.193,127.0.0.1 --store-dir ~/mutmuas-server/jetstream
# nodes: servers ["nats://150.89.170.193:4222"], credentials_file <NODE>.env, tls_ca ca.crt
# a node on the server itself may use nats://127.0.0.1:4222 (127.0.0.1 is in the certificate)
```

**Private overlay network** (Tailscale/WireGuard):

On the machine that hosts NATS (here B). Take its overlay IP, e.g. `100.64.0.10`.

```bash
cd ~/mutmuas
agent-node server-config --project visual_rl --nodes A,B \
    --out ~/mutmuas-server --listen 100.64.0.10 --store-dir /var/lib/nats/jetstream
# writes ~/mutmuas-server/nats-server.conf, A.env, B.env, admin.env  (mode 600)
```

Choose one way to run it.

**Linux, native + systemd (recommended):**
```bash
sudo install -m 755 .local/bin/nats-server /usr/local/bin/
sudo mkdir -p /etc/nats /var/lib/nats && sudo cp ~/mutmuas-server/nats-server.conf /etc/nats/
sudo cp deploy/nats-server.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now nats-server
systemctl status nats-server --no-pager
```

**Linux, native + systemd user service (no sudo):** the same server as a user service. Needs only
`loginctl enable-linger`, which Ubuntu lets a user run for themselves (check with
`pkaction --action-id org.freedesktop.login1.set-self-linger --verbose`: `implicit any: yes`).
Keep `--store-dir` under your home, e.g. `~/mutmuas-server/jetstream`.
```bash
mkdir -p ~/.config/systemd/user
cp deploy/nats-server.user.service ~/.config/systemd/user/mutmuas-nats-server.service   # edit paths if not ~/mutmuas, ~/mutmuas-server
systemctl --user daemon-reload && systemctl --user enable --now mutmuas-nats-server
loginctl enable-linger "$USER"          # run with nobody logged in, start at boot
systemctl --user status mutmuas-nats-server --no-pager
journalctl --user -u mutmuas-nats-server -f
```
If the node daemon runs on the same machine, order it after the server with the drop-in
`deploy/agent-node-after-nats.conf` (see section 3). Reload after adding a node: `systemctl --user reload mutmuas-nats-server`.
Processes started earlier with `nohup` must be stopped first (by PID), or the service cannot bind 4222.

**Docker Compose** (generate with `--store-dir /data/jetstream --listen 0.0.0.0`, and publish the port
only on the overlay IP):
```bash
cp -r ~/mutmuas-server ./server
NATS_BIND=100.64.0.10 docker compose -f deploy/docker-compose.yml up -d
```

**macOS host:** `scripts/start-server.sh ~/mutmuas-server/nats-server.conf` runs it in the foreground.
For a service, `brew install nats-server` and point `brew services` at the config, or wrap the same
command in a launchd plist, as `agent-node service` does for nodes.

Verify from any node: `nc -vz 100.64.0.10 4222` succeeds.

**Adding a machine later (node C):** re-run the same `server-config` command with `--nodes A,B,C` and
the same `--out`. Existing nodes keep their passwords. Copy the new `nats-server.conf` into place, then
run `sudo systemctl reload nats-server` (or `nats-server --signal reload`), and copy `C.env` to C.

## 3. Join a Linux node (B)

```bash
mkdir -p ~/.mutmuas && cp ~/mutmuas-server/B.env ~/.mutmuas/B.env && chmod 600 ~/.mutmuas/B.env
cp config/example-node-B.yaml ~/.mutmuas/node.yaml     # edit: workdir, repo, capabilities, model
agent-node join --server nats://100.64.0.10:4222 --credentials ~/.mutmuas/B.env
agent-node doctor                 # config ok, CLIs found, NATS reachable
agent-node start                  # foreground first time; Ctrl-C to stop
```

As a service (systemd user unit, which keeps running after logout):
```bash
agent-node service --write
systemctl --user daemon-reload && systemctl --user enable --now mutmuas-agent-node
loginctl enable-linger "$USER"
journalctl --user -u mutmuas-agent-node -f      # or: tail -f ~/.mutmuas/visual_rl/B/node.log
```

On the machine that also runs NATS as a user service, start the node after it:
```bash
mkdir -p ~/.config/systemd/user/mutmuas-agent-node.service.d
cp deploy/agent-node-after-nats.conf ~/.config/systemd/user/mutmuas-agent-node.service.d/after-nats.conf
systemctl --user daemon-reload
```
Check the generated unit before enabling it: `ExecStart` falls back to `python -m mutmuas.cli node` when
`agent-node` is not on `PATH`, and `Environment=PATH=` is copied from the shell you ran it in (for example an
activated conda env would leak into every worker). Run `agent-node service --write` from a clean shell with
`~/mutmuas/.venv/bin` on `PATH`.

**Linux: Codex workers need unprivileged user namespaces.** Codex sandboxes shell commands with bubblewrap.
Ubuntu 24.04 blocks this by default (`sysctl kernel.apparmor_restrict_unprivileged_userns` = 1), so every shell
command of a `runtime: codex` worker fails with `bwrap: setting up uid map: Permission denied`. Check with
`unshare -Ur true`. Without root, run Codex workers on macOS nodes and Claude Code workers on Linux. With root,
allow user namespaces for `/usr/bin/bwrap` through an AppArmor profile.

## 4. Join a macOS node (A)

```bash
mkdir -p ~/.mutmuas && scp B:~/mutmuas-server/A.env ~/.mutmuas/A.env && chmod 600 ~/.mutmuas/A.env
cp config/example-node-A.yaml ~/.mutmuas/node.yaml
agent-node join --server nats://100.64.0.10:4222 --credentials ~/.mutmuas/A.env
agent-node doctor
agent-node service --write        # ~/Library/LaunchAgents/dev.mutmuas.agent-node.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.mutmuas.agent-node.plist
launchctl kickstart -k gui/$(id -u)/dev.mutmuas.agent-node
```

The unit captures your current `PATH` so that the daemon finds `claude` and `codex`. Re-run
`agent-node service --write` if they move. A launchd agent runs while you are logged in. For
a headless Mac, use a LaunchDaemon with `UserName` set.

## 5. Register agents

Agents are declared in the node's `node.yaml` (`agents:` list). After editing, restart the
daemon (`launchctl kickstart -k …` / `systemctl --user restart mutmuas-agent-node`).

**Worker agents** run tasks headless, launched by the daemon:

```yaml
- id: representation            # address B:representation (stable; the inbox is keyed on it)
  display: "B:a1"               # optional alias
  mode: worker
  runtime: claude-code          # claude-code | codex | script
  model: opus                   # optional; empty = CLI default
  workdir: ~/work/visual_rl
  repo: ~/work/visual_rl        # optional: kind=code tasks get their own git worktree
  capabilities: [isaac_lab, gpu_training]
  permissions: [READ, RUN_EXPERIMENT, PUBLISH_ARTIFACT, REQUEST_TASK]
  accept_from: ["A:*", "B:*"]
  task_timeout_s: 14400
```

Runtime notes:
- `claude-code` runs `claude -p` with only the mutmuas MCP server (`--strict-mcp-config`) and the tools
  allowed by the agent's permissions (ARCHITECTURE_V1 §5). `extra_args` are appended to the command line.
- `codex` runs `codex exec` with `--ignore-user-config` (your `~/.codex/config.toml` model and MCP servers
  are not loaded; set `inherit_user_config: true` to change that), sandbox `read-only`/`workspace-write`,
  and the mutmuas tools pre-approved.
- `script` runs any `command:`. The task JSON arrives on stdin, and the last JSON line on stdout is the
  RESULT body. `{python}` expands to the venv's Python.

**Interactive agents** are your own Claude Code / Codex sessions:

```yaml
- id: lead
  display: "A:a1"
  mode: interactive
  permissions: [READ, REQUEST_TASK, PUBLISH_ARTIFACT]
```

Give the session the network as tools:

```bash
# Claude Code (user scope = every project on this machine)
claude mcp add mutmuas -s user -- ~/mutmuas/.venv/bin/agentctl mcp --config ~/.mutmuas/node.yaml --as A:lead
```
```toml
# Codex: ~/.codex/config.toml
[mcp_servers.mutmuas]
command = "/Users/<you>/mutmuas/.venv/bin/agentctl"
args = ["mcp", "--config", "/Users/<you>/.mutmuas/node.yaml", "--as", "A:lead"]
```

Then just say it in the session: *"Ask B for last week's representation experiment data."* The agent
calls `find_agent`, `send_request`, `wait_for_result` and `fetch_artifact` by itself. Use one
interactive agent id per concurrently open session, because sessions that share an id share an inbox.

## 6. Test A → B

1. Add a self-test worker to B's `node.yaml` and restart B's daemon:
   ```yaml
   - id: ping
     mode: worker
     runtime: script
     command: ["{python}", "/home/<you>/mutmuas/examples/workers/ping.py"]
     accept_from: ["*"]
   ```
2. On A:
   ```bash
   agentctl status                        # NODE A ONLINE, NODE B ONLINE, agents listed
   agentctl ask B:ping "ping" --wait 60   # status COMPLETED, outputs: hostname, platform, gpus
   ```
3. Durable delivery: stop B's daemon, then run `agentctl ask B:ping "ping while offline"` on A. The
   output shows `delivery: sent` with a note that B is offline. Start B's daemon again and run
   `agentctl result <task_id> --wait 60`. It completes.
4. Real LLM worker: `agentctl ask B:representation "List the files in your working directory" --wait 600`.
5. Artifacts: on A run `echo hi > /tmp/x.txt && agentctl artifact publish /tmp/x.txt`, then on B run
   `agentctl artifact fetch <uri> --dest /tmp/got`.

All of this can be rehearsed on one machine first: `scripts/e2e-local.sh` runs the same flow with a
real server, generated credentials and two daemon processes. `scripts/smoke-llm.sh claude-code haiku`
and `scripts/smoke-llm.sh codex` exercise the real LLM runtimes.

## 7. Operations and observability

```bash
agentctl status                  # nodes, agents, online/offline, current task, queue, inbox, heartbeat
agentctl agents --capability isaac_lab
agentctl tasks                   # tasks this node requested or owns
agentctl tasks --all             # every task, from the shared ledger
agentctl task <id>               # full message history: who asked what, why, and what came back
agentctl history --task <id>     # raw audit trail from the stream
agentctl inbox --as A:lead       # messages for an interactive agent
tail -f ~/.mutmuas/visual_rl/B/node.log
ls ~/.mutmuas/visual_rl/B/runs/  # per-task agent output (<task>.attemptN.log)
```

## 8. Recovery

| Situation | What happens / what to do |
|---|---|
| NATS server stops | Nodes keep running. New messages queue in each node's outbox (`agentctl status` shows `outbox=N` once the server is back). Restart the server: `systemctl restart nats-server`. Everything flushes automatically and nothing needs replaying. |
| Server machine lost | Messages and artifacts live in `store_dir`, so back up `/var/lib/nats/jetstream`. Without a backup: start a fresh server with the same `nats-server.conf`. Nodes re-register on their next heartbeat, and each node's `ledger.sqlite3` still holds its own tasks and messages. Messages in flight and object-store artifacts are lost. |
| Node daemon crashes / machine reboots | The service manager restarts it. On start it re-queues unfinished tasks (attempt+1, requester notified "restarted"). After `max_attempts` a task fails with an explanation. |
| A task is stuck | `agentctl cancel <id>` from the requester. On the owner, `agentctl task <id>` and the run log show why. |
| Wrong credentials | The daemon logs `Authorization Violation` and retries. Check `nats.credentials_file` and that the server config includes the node. |
| Reset one node completely | Stop its daemon and delete its `data_dir`. Its durable mailbox on the server still holds unacknowledged messages, which are delivered again. |
| Rotate a node's password | Delete `<out>/<NODE>.env`, re-run `server-config`, reload the server, copy the new file to the node, and restart the node. |

## 9. Uninstall

Stop and remove the service (`launchctl bootout gui/$(id -u)/dev.mutmuas.agent-node`, then delete the
plist; or `systemctl --user disable --now mutmuas-agent-node`). Then run `rm -rf ~/.mutmuas ~/mutmuas`.
