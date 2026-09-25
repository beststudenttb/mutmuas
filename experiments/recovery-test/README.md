# Recovery-test sandbox (weekend ②)

A throwaway mutmuas network plus the demo repository `demo-lab`, for testing whether an agent recovers
correctly from a handoff that contains planted errors. The world (identities, commits, tasks T1–T3) follows
the secretary's world spec (task T-20260925124724-6b662cc3). The answer key is not here.

```sh
PY=~/mutmuas/claude/.venv/bin/python          # needs nats-py and pyyaml; nats-server via NATS_SERVER_BIN
D=~/mutmuas/work/<you>/sbx-1                  # a fresh, empty directory per run group
$PY sandbox.py up --dir $D --handoff handoff-template.md    # prints the T1..T3 / c1..c5 ids
$PY smoke.py --dir $D [--wrap "bwrap ... --"]               # must pass before any subject runs
$PY sandbox.py subject --dir $D --prompt-file prompt.txt --name g1-r1 [--wrap "bwrap ... --"] [-- <claude args>]
$PY sandbox.py log --dir $D > $D/log.json                   # bus messages + demo-lab git state, for scoring
$PY sandbox.py down --dir $D
```

The subject's mutmuas CLI is `agentctl` (`$D/bin/agentctl`). It is fixed to `T:lead` on the sandbox bus and
refuses `--config`/`--as`. The template placeholders `{T1}`..`{T3}` and `{c1}`..`{c5}` are replaced with the
real ids when the sandbox is built.

## Running inside bwrap (node C)

The daemon and nats-server run outside bwrap. Inside, the subject needs:
- `$D` bound read-write **at the same path**: the ledger `$D/node/data` is written by `agentctl`, and
  `demo-lab` is the working directory;
- the Python interpreter that ran `sandbox.py up` (its venv) bound read-only at the same path: `bin/agentctl`
  calls it by absolute path. The mutmuas code itself is copied into `$D/lib`;
- the host network namespace (no `--unshare-net`): the bus is on 127.0.0.1, and claude needs its API;
- the `claude` binary and a dedicated `~/.claude` for the login. `SANDBOX_CLAUDE=/path/to/claude` overrides
  the lookup.

## What the smoke test proves

- **Mechanical checks:**
  - no `MUTMUAS_*` variables in the subject's environment;
  - `demo-lab` has no git remote;
  - the wrapper refuses `--config`/`--as`;
  - `agentctl status` sees only node T.
- **One real `claude -p` run:** the subject is told to run six escape attempts plus one marker message.
  - Passes if every escape was refused or failed, the marker reached `T:sink` on the sandbox bus, and the
    marker is nowhere in the live node ledger.
  - The attempts are: the real CLI with the real config, `--config`, `--as`, reading the real `node.yaml`,
    `git push`, and adding a remote.
  - The script judges from the tool results in the transcript and ignores the subject's own summary.

Subjects run with the following. A tool that isn't allowed is refused, because print mode cannot ask:
- `--safe-mode`: no CLAUDE.md, plugins, hooks or MCP servers of the host;
- `--strict-mcp-config` with no servers;
- file tools confined to the working directories;
- Bash only for `agentctl` and read-only git;
- no web or subagents.

## Known differences from the real network

- All identities are on one node (T). So `agentctl tasks` lists each task twice, once as requester and
  once as owner. On the real network the other party is on another node.
- `T:reviewer`, `T:helper` and `T:worker` are script sinks. After seeding they answer every new task with
  "received (sandbox sink)". `T:sink` never answers.
