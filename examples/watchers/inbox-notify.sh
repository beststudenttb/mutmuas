#!/usr/bin/env bash
# Long-running notifier for an *interactive* agent: when a message that needs a decision arrives,
# show a desktop notification (macOS: osascript; Linux: notify-send; otherwise log only).
# It never marks messages read (--peek) and never re-announces one (--since cursor), so the agent's
# session still reads and handles everything itself. Run it under launchd/systemd (see below).
#
#   examples/watchers/inbox-notify.sh <config.yaml> <NODE:agent> [--dry-run]
#
# launchd (macOS): ProgramArguments = [/bin/bash, <repo>/examples/watchers/inbox-notify.sh, <config>, <agent>],
# RunAtLoad + KeepAlive, StandardOutPath = a log file. The loop keeps its cursor in <config dir>/<agent>.notify-cursor.
set -uo pipefail
CONFIG="$1"; AGENT="$2"; DRY="${3:-}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
AGENTCTL="$REPO/.venv/bin/agentctl"
CURSOR_FILE="$(dirname "$CONFIG")/${AGENT//:/_}.notify-cursor"
[ -f "$CURSOR_FILE" ] || date -u +%Y-%m-%dT%H:%M:%S.000+00:00 > "$CURSOR_FILE"

notify() {
  local title="$1" text="$2"
  echo "$(date '+%F %T') notify: $title — $text"
  [ "$DRY" = "--dry-run" ] && return
  if command -v osascript >/dev/null; then
    # Message text comes from other agents: pass it as argv, never splice it into AppleScript source.
    osascript -e 'on run argv' -e 'display notification (item 2 of argv) with title (item 1 of argv) sound name "Glass"' \
              -e 'end run' "$title" "$text" >/dev/null
  elif command -v notify-send >/dev/null; then
    notify-send "$title" "$text"
  fi
}

while true; do
  since="$(cat "$CURSOR_FILE")"
  out="$("$AGENTCTL" inbox --wait 3600 --peek --only actionable --since "$since" --json \
         --config "$CONFIG" --as "$AGENT" 2>&1)"
  rc=$?
  if [ $rc -eq 3 ]; then continue; fi                     # timeout, nothing new
  if [ $rc -ne 0 ]; then echo "$(date '+%F %T') agentctl failed ($rc): $out"; sleep 30; continue; fi
  summary="$(printf '%s' "$out" | "$REPO/.venv/bin/python" -c '
import json, sys
rows = json.load(sys.stdin)
first = rows[0]
body = first["body"]
note = body.get("objective") or body.get("summary") or body.get("question") or body.get("reason") or ""
print(max(r["received_at"] for r in rows))
print("%d new: %s from %s: %s" % (len(rows), first["type"], first["from"], note[:120]))')"
  cursor="$(printf '%s\n' "$summary" | sed -n 1p)"
  text="$(printf '%s\n' "$summary" | sed -n 2p)"
  if [ -z "$cursor" ] || [ -z "$text" ]; then       # never spin: keep the old cursor and back off
    echo "$(date '+%F %T') could not parse agentctl output: $out"; sleep 30; continue
  fi
  notify "mutmuas → $AGENT" "$text"
  echo "$cursor" > "$CURSOR_FILE"
done
