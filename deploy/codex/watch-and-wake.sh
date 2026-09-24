#!/usr/bin/env bash
# Keep the mutmuas desktop notifier running and wake one Codex thread for each
# notification it emits.  Values are supplied by the LaunchAgent environment.

set -u

: "${MUTMUAS_AGENTCTL:?MUTMUAS_AGENTCTL is required}"
: "${MUTMUAS_CONFIG:?MUTMUAS_CONFIG is required}"
: "${MUTMUAS_AGENT:?MUTMUAS_AGENT is required}"
: "${CODEX_THREAD:?CODEX_THREAD is required}"

CODEX_BIN="${CODEX_BIN:-$(command -v codex)}"
RETRY_SECONDS="${RETRY_SECONDS:-10}"

while true; do
  "$MUTMUAS_AGENTCTL" watch \
    --config "$MUTMUAS_CONFIG" \
    --as "$MUTMUAS_AGENT" 2>&1 |
  while IFS= read -r line; do
    printf '%s\n' "$line"
    case "$line" in
      *" notify: "*)
        "$CODEX_BIN" queue \
          --thread "$CODEX_THREAD" \
          --message "mutmuas 自动收件：请在下一个安全点读取 ${MUTMUAS_AGENT} 的未读邮箱，接收并处理可执行消息；如果当前已有用户任务，先保存进度再处理。" \
          || printf '%s wake failed for Codex thread %s\n' "$(date '+%F %T')" "$CODEX_THREAD" >&2
        ;;
    esac
  done

  printf '%s watcher exited; retrying in %ss\n' "$(date '+%F %T')" "$RETRY_SECONDS" >&2
  sleep "$RETRY_SECONDS"
done
