#!/usr/bin/env bash
# One-command deploy of the Codex side of a mutmuas node (branch: codex).
#
#   deploy/codex/setup.sh --node A --server nats://150.89.170.193:4222 --credentials A.env --ca ca.crt \
#       [--project mutmuas] [--config PATH] [--agent codex] [--worker] [--workdir DIR] \
#       [--service] [--notifier] [--mcp] [--skip-install]
#
# One machine = one node; several assistants can live on it. This script never overwrites what another
# assistant set up: it creates the node config only if missing, adds its own agents idempotently, and
# restarts an existing node service without rewriting it. Changes outside this checkout require a flag:
#   --service   install/restart the node daemon as a launchd (macOS) / systemd --user (Linux) service
#   --notifier  install/restart desktop notifications for the interactive Codex agent
#   --mcp       replace the global Codex MCP entry named "mutmuas" with this node and identity
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT=mutmuas
NODE=""
SERVER=""
CREDS=""
CA=""
CONFIG=""
AGENT=codex
WORKER=0
WORKDIR="$REPO"
SERVICE=0
NOTIFIER=0
MCP=0
INSTALL=1

usage() {
  cat <<'USAGE'
Usage: deploy/codex/setup.sh --node NODE [options]

Required for a new node:
  --server URL --credentials NODE.env [--ca ca.crt]

Configuration:
  --project NAME --config PATH --agent ID --workdir DIR --worker --skip-install

Explicit host changes:
  --service     install/restart the shared node daemon
  --notifier    install/restart this Codex agent's desktop notifier
  --mcp         replace Codex's global "mutmuas" MCP entry
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --node) NODE="$2"; shift 2 ;;
    --server) SERVER="$2"; shift 2 ;;
    --credentials) CREDS="$2"; shift 2 ;;
    --ca) CA="$2"; shift 2 ;;
    --project) PROJECT="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --agent) AGENT="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    --worker) WORKER=1; shift ;;
    --service) SERVICE=1; shift ;;
    --notifier) NOTIFIER=1; shift ;;
    --mcp) MCP=1; shift ;;
    --skip-install) INSTALL=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

[ -n "$NODE" ] || { echo "--node is required" >&2; exit 2; }
CONFIG="${CONFIG:-$HOME/.mutmuas/$PROJECT/$NODE/node.yaml}"
case "$CONFIG" in /*) ;; *) CONFIG="$PWD/$CONFIG" ;; esac
case "$WORKDIR" in /*) ;; *) WORKDIR="$PWD/$WORKDIR" ;; esac
DIR="$(dirname "$CONFIG")"
BIN="$REPO/.venv/bin"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

if [ "$INSTALL" -eq 1 ]; then
  say "install ($REPO)"
  SKIP_NATS=1 "$REPO/scripts/install.sh" >/dev/null
elif [ ! -x "$BIN/agent-node" ] || [ ! -x "$BIN/agentctl" ]; then
  echo "--skip-install requires $BIN/agent-node and $BIN/agentctl" >&2
  exit 2
fi

say "node config $CONFIG"
if [ -f "$CONFIG" ]; then
  echo "exists — keeping it (other assistants may use this node); only adding Codex agents"
else
  [ -n "$SERVER" ] && [ -n "$CREDS" ] || {
    echo "new node needs --server and --credentials" >&2
    exit 2
  }
  mkdir -p "$DIR"
  chmod 700 "$DIR"
  install -m 600 "$CREDS" "$DIR/$NODE.env"
  init_args=(init --bare --config "$CONFIG" --project "$PROJECT" --node "$NODE" --server "$SERVER"
             --credentials "$DIR/$NODE.env" --data-dir "$DIR/data")
  if [ -n "$CA" ]; then
    install -m 644 "$CA" "$DIR/ca.crt"
    init_args+=(--ca "$DIR/ca.crt")
  fi
  "$BIN/agent-node" "${init_args[@]}" >/dev/null
  echo "created"
fi

mkdir -p "$WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)"
identity="$("$BIN/python" -c 'import sys, yaml; data=yaml.safe_load(open(sys.argv[1])); print(data["project"]); print(data["node"])' "$CONFIG")"
actual_project="$(printf '%s\n' "$identity" | sed -n '1p')"
actual_node="$(printf '%s\n' "$identity" | sed -n '2p')"
[ "$actual_node" = "$NODE" ] || {
  echo "--node $NODE does not match node $actual_node in $CONFIG" >&2
  exit 2
}
PROJECT="$actual_project"

say "agents"
"$BIN/agent-node" add-agent --config "$CONFIG" --id "$AGENT" --mode interactive --provider openai \
  --role codex-lead --workdir "$WORKDIR" \
  --capability coding --capability debugging --capability code_review --capability research \
  --permission READ --permission REQUEST_TASK --permission PUBLISH_ARTIFACT

if [ "$WORKER" -eq 1 ]; then
  "$BIN/agent-node" add-agent --config "$CONFIG" --id "$AGENT-worker" --mode worker --runtime codex \
    --provider openai --role implementation --workdir "$WORKDIR" --repo "$WORKDIR" --notify "$NODE:$AGENT" \
    --capability coding --capability python --capability refactoring --capability unit_tests \
    --permission READ --permission WRITE_WORKTREE --permission PUBLISH_ARTIFACT --permission REQUEST_TASK
fi

say "doctor"
"$BIN/agent-node" doctor --config "$CONFIG"

load_unit() { # $1 is empty for the node daemon, or the agent id for a notifier
  local watched="$1" label unit plist
  if [ "$(uname -s)" = Darwin ]; then
    label="dev.mutmuas.$PROJECT.$NODE${watched:+.watch-$watched}"
    plist="$HOME/Library/LaunchAgents/$label.plist"
    if [ -f "$plist" ]; then
      grep -Fq -- "$CONFIG" "$plist" || {
        echo "$plist belongs to a different config; refusing to replace it" >&2
        exit 2
      }
      echo "$label exists — restarting it (not rewriting: another checkout may own it)"
    else
      if [ -n "$watched" ]; then
        "$BIN/agent-node" service --write --config "$CONFIG" --watch "$watched" | head -1
      else
        "$BIN/agent-node" service --write --config "$CONFIG" | head -1
      fi
    fi
    launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1 || launchctl bootstrap "gui/$(id -u)" "$plist"
    launchctl kickstart -k "gui/$(id -u)/$label"
  else
    unit="mutmuas-agent-node${watched:+-$NODE.watch-$watched}"
    if [ -f "$HOME/.config/systemd/user/$unit.service" ]; then
      grep -Fq -- "$CONFIG" "$HOME/.config/systemd/user/$unit.service" || {
        echo "$unit belongs to a different config; refusing to replace it" >&2
        exit 2
      }
      echo "$unit exists — restarting it"
    else
      if [ -n "$watched" ]; then
        "$BIN/agent-node" service --write --config "$CONFIG" --watch "$watched" | head -1
      else
        "$BIN/agent-node" service --write --config "$CONFIG" | head -1
      fi
    fi
    systemctl --user daemon-reload
    systemctl --user enable "$unit"
    loginctl enable-linger "$USER" 2>/dev/null || true
    systemctl --user restart "$unit"
  fi
}

if [ "$SERVICE" -eq 1 ]; then
  say "node service"
  load_unit ""
fi
if [ "$NOTIFIER" -eq 1 ]; then
  say "notifier for $NODE:$AGENT"
  load_unit "$AGENT"
fi

if [ "$MCP" -eq 1 ]; then
  say "Codex MCP"
  command -v codex >/dev/null 2>&1 || { echo "codex CLI is required for --mcp" >&2; exit 2; }
  if codex mcp get mutmuas >/dev/null 2>&1; then
    echo "replacing existing Codex MCP server named mutmuas"
    codex mcp remove mutmuas >/dev/null
  fi
  codex mcp add mutmuas -- "$BIN/agentctl" mcp --config "$CONFIG" --as "$NODE:$AGENT" >/dev/null
  codex mcp get mutmuas
fi

say "done"
cat <<MSG
this assistant is $NODE:$AGENT (config $CONFIG)
check:  $BIN/agentctl status --config $CONFIG
talk:   $BIN/agentctl ask <NODE:agent> "..." --config $CONFIG --as $NODE:$AGENT --wait 600
MCP changes are loaded by new Codex sessions.
MSG
