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
  workdir: ~/mutmuas/work/representation/visual-rl   # function x project directory (v4), not in a repo
  default_project: ""           # D-069: requests without `project` go to work/<post>/<this>/ ("" = workdir)
  repo: ~/work/visual_rl        # optional: kind=code tasks get their own git worktree
  code_mode: copy               # copy (default): kind=code works on a private worktree of repo;
                                # direct: edit code_dirs in place and commit (project-level work, D-031)
  code_dirs: []                 # direct mode: the project code, reached with --add-dir
  capabilities: [isaac_lab, gpu_training]
  permissions: [READ, RUN_EXPERIMENT, PUBLISH_ARTIFACT, REQUEST_TASK]
  accept_from: ["A:*", "B:*"]
  task_timeout_s: 14400
  max_turns: 150                # optional (D-066): a claude-code run stops after this many turns ...
  max_cost_usd: 15              # ... or this spend; defaults: the node's worker_max_turns / worker_max_cost_usd
```

Node-wide worker limits (D-066), top level of node.yaml: `worker_max_turns` and `worker_max_cost_usd` (default:
none). They map to `claude -p --max-turns` / `--max-budget-usd`; Codex has no equivalent flag, so a codex worker is
bounded by `task_timeout_s` only. A run stopped at a limit without a result counts as a failed run (recorded,
laid out once more, then failed).

Long commands (D-104): a worker's prompt tells it to run any command it expects to take longer than
`background_after_min` minutes (top level of node.yaml, default 10) as a background job (detached, registered with
add_job), never in the foreground of its run. Claude Code's own settings are not changed for this.

Runtime notes:
- `claude-code` runs `claude -p` with only the mutmuas MCP server (`--strict-mcp-config`) and the tools
  allowed by the agent's permissions (ARCHITECTURE_V1 §5). `extra_args` are appended to the command line.
  Tools come from node.yaml only (D-032): `--tools` limits the built-in tools that exist (`--allowedTools` only
  pre-approves); setting source `project` only, not `user` (its allow rules, plugin hooks) nor `local` (a
  session's "don't ask again" approvals).
  HANDOFF.md belongs to the interactive session: the worker's prompt says not to edit it (a convention only).
- Staff system v4 (D-029..D-031): workdir is the function x project directory, e.g.
  `~/mutmuas/work/paper/visualrl/` (not inside any git repo). LLM workers start there, so the function's
  `CLAUDE.md` one level up and the project's auto memory load; a code task's worktree (copy) or `code_dirs`
  (direct) are added with `--add-dir`. A workdir inside `repo` keeps the old behaviour (start in the worktree).
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

**One address, two ways of working** (`auto_worker`, staff system v4, D-030/D-032a): an interactive agent
with `auto_worker: true` and a worker `runtime` is run by the daemon as a worker while no session holds it.

```yaml
- id: paper-visualrl
  mode: interactive
  auto_worker: true
  runtime: claude-code
  workdir: ~/mutmuas/work/paper/visualrl
```

- No session (and none for `AUTO_WORKER_GRACE_S`, 120 s, so a restarted terminal does not count as gone):
  a REQUEST is accepted and run as a worker task.
- The leader's session is there: requests are listed in its inbox and not run; the leader decides.
  A task queued for the worker but not started yet goes back to PENDING for the session.
- A worker already running when the session comes is not interrupted. The session sees it in
  `whoami` (`worker_running`: task, since, latest end) and may neither accept it nor deliver its result;
  the worker's own processes keep acting as the agent (the lease admits the pid the daemon recorded).
- Who does a task is claimed in the ledger (`tasks.runner`: worker | session) in one transaction with the
  session check, so a task is never done twice.

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
   `agentctl task <task_id>` until it shows COMPLETED.
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
agentctl inbox --as A:lead       # messages for an interactive agent
tail -f ~/.mutmuas/visual_rl/B/node.log
ls ~/.mutmuas/visual_rl/B/runs/  # per-task agent output (<task>.<UTC start time>.attempt<N>.log, one per run)
```

While an agent's session is online, a shell beside it only reads (the commands above with `inbox --peek`, and
`watch --headers-only`) and switches the session's work (`session off|on`). `agentctl update`, `submit`, `artifact`
and the other commands that act as the agent are refused there: the session acts through its MCP tools, and a
running worker's own processes act on its task (MESSAGE_PROTOCOL.md, "One agent, one session").

## 8. Recovery

| Situation | What happens / what to do |
|---|---|
| NATS server stops | Nodes keep running. New messages queue in each node's outbox (`agentctl status` shows `outbox=N` once the server is back). Restart the server: `systemctl restart nats-server`. Everything flushes automatically and nothing needs replaying. |
| Server machine lost | Messages and artifacts live in `store_dir`, so back up `/var/lib/nats/jetstream`. Without a backup: start a fresh server with the same `nats-server.conf`. Nodes re-register on their next heartbeat, and each node's `ledger.sqlite3` still holds its own tasks and messages. Messages in flight and object-store artifacts are lost. |
| Node daemon crashes / machine reboots | The service manager restarts it. On start it re-queues unfinished tasks (attempt+1, requester notified "restarted"). After `max_attempts` a task fails with an explanation. **auto_worker exception** (v4, D-040): a task whose worker submitted its result is delivered, not run again; a task whose worker from before the restart still runs (pid and start time both match) is skipped, recorded once in the failure log (`agentctl failures`) and looked at again every heartbeat until that worker has ended; any other task is run again. The old worker is not stopped. A session's task is left to the session. |
| Rolling back from v4 | Back up `data/ledger.sqlite3` before deploying v4. The ledger keeps working with the old code (it ignores the new `tasks.runner*` columns and the `failures` table). Remove the v4 fields from `node.yaml` first (`auto_worker`, `code_mode`, `code_dirs`): the old code rejects unknown keys and would not start. |
| A task is stuck | The requester's session withdraws it (MCP `cancel_task`). On the owner, `agentctl task <id>` and the run log show why. |
| Wrong credentials | The daemon logs `Authorization Violation` and retries. Check `nats.credentials_file` and that the server config includes the node. |
| Reset one node completely | Stop its daemon and delete its `data_dir`. Its durable mailbox on the server still holds unacknowledged messages, which are delivered again. |
| Rotate a node's password | Delete `<out>/<NODE>.env`, re-run `server-config`, reload the server, copy the new file to the node, and restart the node. |

## 8b. Retire a post (D-085/D-089)

`agent-node retire-agent <id> [--hand-over <addr>]` shows what it would do; add `-y` to do it, on the node
that has the post, **with the node daemon stopped**. That is the guard (D-102): the command holds the daemon's
lock (`data/daemon.lock`) to the end, and refuses while the post's session is online or a worker of it runs, so
nothing else acts for the post meanwhile. A person starting its session or editing its files in the middle is
not guarded against.

1. refuses also while unread mail waits in its mailbox, unless `--keep-mailbox` (the mailbox is then kept and
   listed under `todo`; nothing unread is deleted);
2. writes the whole plan to `RETIRED-<id>-<time>.json` next to node.yaml before the first change;
3. node.yaml: a timestamped backup (`node.yaml.bak-<time>-retire-<id>`), then only that agent's list item is
   taken out, found from the parsed YAML's own positions (a comment inside the item goes with it; comments after
   it stay), written atomically;
4. its open work is refused back to each requester ("retired; ask <hand-over> instead"); what it asked others
   for is withdrawn (their nodes cascade further down);
5. its registry card and mailbox are removed now when the bus is reachable, else listed under `todo` (the node
   removes the card at its next start);
6. its post directory is moved whole to `work/_archive/<post>-<time>/`. Nothing in it is deleted, since it may
   hold the leader's files. A directory another agent uses (the same path, or one inside the other) is left in
   place.

Start the node again afterwards: it forgets the address. `agent-node retire-agent --undo <manifest>` (also with
`--dry-run`, and also with the node daemon stopped) puts the block back at its old place in the agents list
unless that id is configured, and the directory back from its archive unless something is in its place (then it
says so); then start the node. What is on disk decides, so a run that stopped midway is undone too. Refused and
withdrawn tasks stay so.

## 8c. Deploying without stopping work (D-104)

A deploy restarts the daemon on the new code; it does not stop the work in progress (as nginx reloads):
- Stopping the daemon leaves its runs running: each run and each job has its own session, and its output goes to
  its own log, not through the daemon. The service stops the daemon alone: the systemd unit has
  `KillMode=process`, the launchd plist `AbandonProcessGroup` (measured on macOS 2026-10-07: after `launchctl
  bootout` a child in its own session lives on even without it; with it, a child in the job's group does too).
  Units written before D-104 must be written again once (`agent-node service --write`) as part of a deploy.
- The new daemon takes new work, adopts each run it finds still going (recover), and when it ends delivers its
  result (the draft it submitted, or what it printed), or runs the task again if it left none.
- Old and new code work side by side meanwhile: runs finish on the code they started with (a process loads all of
  mutmuas when it starts), the ledger and the messages only ever gain fields and columns (ledger.ADDED_COLUMNS),
  never lose or change one.

## 8a. Supervision and known risks

Errors are skipped and recorded rather than defended against (D-039, D-040); patches follow once the records
show a pattern.

- **Failure log**: every node writes skipped errors to its ledger (`failures` table: time, stage, address,
  task, attempt, error). `agentctl failures [--limit N]` lists them, newest first.
- **Failed runs are laid out once more**: a run that crashed without a result, timed out, hit a runtime error or
  could not get its worktree is recorded and queued again once (`max_attempts`, default 2); the next failure
  finishes the task as failed. An agent's own "failed" result is final.
- **Background loops** (heartbeat, receive, handle, outbox, notify, follow-ups) record their errors and carry on.

Known risks (protections removed on purpose; one line each):

- The usage limit is recognised by the vendor CLI's own wording (runtime.py quota_patterns): if a vendor changes
  it, such a run counts as an ordinary failed run (laid out once more, then failed) until the pattern is added.

- An old worker's children that outlive their leader after a daemon crash are not looked for; a retry can run
  next to them.
- Observer copies of a REQUEST are sent by the owner's node once it has the request (D-102), so they arrive
  only when that node is up. During a rolling upgrade a new requester with an old owner sends none (the old code
  left them to the requester): they come once the owner's node runs the new code. A copy that reaches an
  observer without any record of the task (no bus at the observer) is dropped and logged.
- A worker's send_request takes its own task as parent_task by default (MUTMUAS_TASK_ID), unchecked; the
  daemon sets that variable, so it is the worker's task unless a person sets it by hand.
- A session started from the wrong directory (empty memory, other rules) is not told so.
- A `.claude/settings.json` in or above a worker's project directory that allows tools is trusted.
- The session holding an agent does not learn that a second session tried to take it over.
- A malformed optional REQUEST field (timeout_s, observers, deadline, next) is accepted and may fail later.
- In a rare interleaving the heartbeat accepts a task `_on_request` is still handling: a second ACK (never a
  second run).
- The recipient of a request refused for its kind does not see it; only the requester gets the REJECT.
- An unparsable incoming message gets no ERROR back; it is only in the receiving node's failure log.
- A result printed only inside a Markdown fence, mid-output or over several lines is not parsed (use
  submit_result).
- A typo in a node.yaml key gives Python's TypeError instead of a one-line message.
- Tasks created in the same millisecond may run in either order after a restart (the queue orders by created_at;
  Codex T-20260929122530-54280d88).
- A job registered with only a done-file that never writes it keeps its task waiting forever (cancel the task).
- A task waiting on child tasks whose owners never answer and that have no deadline waits forever (sends normally
  get the node's default reply deadline, so this needs `reply: none` or a removed deadline).
- A limit-stopped run that is laid out again usually stops at the same limit: the retry costs one more run.
- The node and the brain may write the same PLAN.md at the same moment: the node's writes take a lock
  (.PLAN.md.lock) and rewrite only the sub's line, but the brain does not take that lock, so a brain write in
  between can be lost (D-073).
- Who may interrupt is decided by the sender address (node.yaml `trusted_controllers`, a list of full addresses,
  checked at load; D-089). Each node has its own bus credential and may publish only as its own node, but the
  agents of one node share it: any process on node B can send as B:claude-secretary. The list is a soft
  boundary between honest senders, not a guard against a forger on that node; that needs per-daemon identity or
  credentials, or signed relays (not built). The list is a node.yaml change the leader makes. `leader: true`
  and priority high order work, they grant nothing.
- Pause and interrupt stop the worker's process group only. A process that left the group (its own setsid), a
  job on another machine, or a training started elsewhere keeps running: a task shown paused does not mean its
  GPU training stopped or its GPU locks were released. A group that cannot be shown stopped (still alive after
  SIGKILL, EPERM from a member that is not a zombie, or a ps answer that cannot be read) is recorded on the task
  (`stuck_pgid`) and watched as a group, also after its leader has ended; nothing runs for the task while it
  lives, and the heartbeat lays the task out once it has gone. The node knows a run's process only from the
  runtime's spawn report, and a group id is checked by its live members (a reused id is not told apart).
- Pause is not reliable for every open child task: a child its owner node has already recorded is paused there,
  but a pause that reaches the owner node before the
  child's REQUEST was handled is dropped (no persistent order or tombstone for unknown tasks).
- A run stopped while its worktree is made (code tasks) is not known to be safe: the git process may go on and
  the next run reuses a half-made worktree as it finds it. Not verified; do not count on interrupting a code
  task at that stage.
- An interrupted run is killed midway: work it had not saved, and side effects it had started (a training
  process it launched without add_job), are not undone; the next run is told it was interrupted (D-089).
- The 收件 section is rewritten by the node under the PLAN lock; an agent editing PLAN.md at the same moment
  without the lock can lose its edit or the node's line (D-073 batch 2).
- An eta lives on the requester's node only (not on the public card): another node's coordinator sees it only
  through check_task on a task it may see.
- Retiring a post cannot take back the refusals and withdrawals it sent; `--undo` restores only the config
  block and the directory (D-085).
- `retire-agent` trusts `data/daemon.lock`: a daemon started from another data directory for the same node is not
  seen.
- Two brain runs of one post never overlap, so different projects of a post wait for each other's brain runs.
- A session started in any real subdirectory of the post directory (other than memory/ and hidden ones) counts
  as working on a project of that name, and with auto_worker takes only that project's requests (D-072).
  Cancelling such a task does not stop the job: the node knows no process for it.
- A session's job wake-up moves the task to RUNNING and then writes the session's note; a crash between the two
  loses that wake-up (the task is no longer WAITING, so recovery does not make it up).
- An auto_worker task woken after its jobs: a crash between moving it to ACCEPTED and queueing it lets the next
  recovery deliver the worker's old draft as the result.
- recover() assumes one daemon per node, running serially; it does not claim tasks against a concurrent daemon.
- Delivery log (D-052): a crash after the RESULT is stored but before the log line is written or the PLAN.md
  section is removed leaves the line missing and the section on the board.
- A session and a still-running auto_worker delivering at the same moment can overwrite each other's PLAN.md edit
  (plain read-modify-write, no lock).

## 9. Uninstall

Stop and remove the service (`launchctl bootout gui/$(id -u)/dev.mutmuas.agent-node`, then delete the
plist; or `systemctl --user disable --now mutmuas-agent-node`). Then run `rm -rf ~/.mutmuas ~/mutmuas`.
