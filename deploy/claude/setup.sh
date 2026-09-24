#!/usr/bin/env bash
# One-command deploy of the Claude side of a mutmuas node (branch: claude).
#
#   deploy/claude/setup.sh --node A --server nats://150.89.170.193:4222 --credentials A.env --ca ca.crt \
#       [--project mutmuas] [--config PATH] [--agent claude] [--worker] [--workdir DIR] \
#       [--service] [--notifier] [--mcp] [--skip-install]
#
# One machine = one node; several assistants can live on it. This script never overwrites what another
# assistant set up: it creates the node config only if missing, *adds* its own agents, and restarts (not
# rewrites) an existing node service. Steps that change the system or global settings need a flag:
#   --service   install/restart the node daemon as a launchd (macOS) / systemd --user (Linux) service
#   --notifier  desktop notifications for the interactive agent (agentctl watch as a service)
#   --mcp       register the mutmuas MCP server in Claude Code (user scope)
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT=mutmuas; NODE=""; SERVER=""; CREDS=""; CA=""; CONFIG=""; AGENT=claude; WORKER=0; WORKDIR="$HOME"
SERVICE=0; NOTIFIER=0; MCP=0; INSTALL=1
while [ $# -gt 0 ]; do
  case "$1" in
    --node) NODE="$2"; shift 2 ;;          --server) SERVER="$2"; shift 2 ;;
    --credentials) CREDS="$2"; shift 2 ;;  --ca) CA="$2"; shift 2 ;;
    --project) PROJECT="$2"; shift 2 ;;    --config) CONFIG="$2"; shift 2 ;;
    --agent) AGENT="$2"; shift 2 ;;        --workdir) WORKDIR="$2"; shift 2 ;;
    --worker) WORKER=1; shift ;;           --service) SERVICE=1; shift ;;
    --notifier) NOTIFIER=1; shift ;;       --mcp) MCP=1; shift ;;
    --skip-install) INSTALL=0; shift ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
[ -n "$NODE" ] || { echo "--node is required" >&2; exit 2; }
# STANDARD v1: <ROOT>/mutmuas/{claude,codex,node[,server]}; the node config sits next to the checkouts.
CONFIG="${CONFIG:-$(dirname "$REPO")/node/node.yaml}"
DIR="$(dirname "$CONFIG")"
BIN="$REPO/.venv/bin"
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

if [ $INSTALL -eq 1 ]; then
  say "install ($REPO)"
  SKIP_NATS=1 "$REPO/scripts/install.sh" >/dev/null
fi

say "node config $CONFIG"
if [ -f "$CONFIG" ]; then
  echo "exists — keeping it (other assistants may use this node); only adding agents"
else
  [ -n "$SERVER" ] && [ -n "$CREDS" ] || { echo "new node needs --server and --credentials" >&2; exit 2; }
  mkdir -p "$DIR"; chmod 700 "$DIR"
  install -m 600 "$CREDS" "$DIR/$NODE.env"
  [ -n "$CA" ] && install -m 644 "$CA" "$DIR/ca.crt"
  "$BIN/agent-node" init --bare --config "$CONFIG" --project "$PROJECT" --node "$NODE" --server "$SERVER" \
      --credentials "$DIR/$NODE.env" ${CA:+--ca "$DIR/ca.crt"} --data-dir "$DIR/data" >/dev/null
  echo "created"
fi

actual_node="$("$BIN/python" -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))["node"])' "$CONFIG")"
[ "$actual_node" = "$NODE" ] || { echo "--node $NODE does not match node $actual_node in $CONFIG" >&2; exit 2; }

say "agents"
"$BIN/agent-node" add-agent --config "$CONFIG" --id "$AGENT" --mode interactive --provider anthropic \
    --role lead --workdir "$WORKDIR" --permission READ --permission REQUEST_TASK --permission PUBLISH_ARTIFACT
if [ $WORKER -eq 1 ]; then
  mkdir -p "$WORKDIR"
  "$BIN/agent-node" add-agent --config "$CONFIG" --id "$AGENT-worker" --mode worker --runtime claude-code \
      --provider anthropic --role worker --workdir "$WORKDIR" --notify "$NODE:$AGENT" \
      --permission READ --permission PUBLISH_ARTIFACT --permission REQUEST_TASK
fi

say "doctor"
"$BIN/agent-node" doctor --config "$CONFIG"

load_unit() {   # $1 = extra args for `agent-node service` (empty or "--watch AGENT")
  local label unit plist
  if [ "$(uname -s)" = Darwin ]; then
    label="dev.mutmuas.$PROJECT.$NODE${1:+.watch-$AGENT}"; plist="$HOME/Library/LaunchAgents/$label.plist"
    if [ -f "$plist" ]; then
      grep -Fq -- "$CONFIG" "$plist" || { echo "$plist belongs to a different config; refusing to touch it" >&2; exit 2; }
      echo "$label exists — restarting it (not rewriting: another checkout may own it)"
    else
      "$BIN/agent-node" service --write --config "$CONFIG" $1 | head -1
    fi
    launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1 || launchctl bootstrap "gui/$(id -u)" "$plist"   # (from codex branch)
    launchctl kickstart -k "gui/$(id -u)/$label"
  else
    unit="mutmuas-agent-node${1:+-$NODE.watch-$AGENT}"
    if [ -f "$HOME/.config/systemd/user/$unit.service" ]; then
      grep -Fq -- "$CONFIG" "$HOME/.config/systemd/user/$unit.service" || { echo "$unit belongs to a different config" >&2; exit 2; }
      echo "$unit exists — restarting it"
    else
      "$BIN/agent-node" service --write --config "$CONFIG" $1 | head -1
      systemctl --user daemon-reload && systemctl --user enable "$unit"
      loginctl enable-linger "$USER" 2>/dev/null || true
    fi
    systemctl --user restart "$unit"
  fi
}
if [ $SERVICE -eq 1 ]; then say "node service"; load_unit ""; fi
if [ $NOTIFIER -eq 1 ]; then say "notifier for $NODE:$AGENT"; load_unit "--watch $AGENT"; fi

if [ $MCP -eq 1 ]; then
  say "Claude Code MCP"
  if claude mcp get mutmuas >/dev/null 2>&1; then
    echo "an MCP server named mutmuas is already registered — leaving it (claude mcp remove mutmuas -s user to redo)"
  else
    claude mcp add mutmuas -s user -- "$BIN/agentctl" mcp --config "$CONFIG" --as "$NODE:$AGENT"
  fi
fi

say "done"
cat <<MSG
this assistant is $NODE:$AGENT (config $CONFIG)
check:  $BIN/agentctl status --config $CONFIG
talk:   $BIN/agentctl ask <NODE:agent> "..." --config $CONFIG --as $NODE:$AGENT --wait 600
MSG
