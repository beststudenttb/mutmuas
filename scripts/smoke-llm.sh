#!/usr/bin/env bash
# Smoke test for a real LLM worker runtime (costs a few cents of tokens).
#   scripts/smoke-llm.sh claude-code [model]     e.g. scripts/smoke-llm.sh claude-code haiku
#   scripts/smoke-llm.sh codex [model]
# Node B runs one worker with the chosen runtime (read-only tools + the mutmuas MCP server).
# Node A asks it to read a file, publish it as an artifact and report the first line.
# Passes only if the agent itself called publish_artifact + submit_result through MCP.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
BIN="$ROOT/.venv/bin"
RUNTIME="${1:-claude-code}"
MODEL="${2:-}"
W="$ROOT/.local/smoke-$RUNTIME"
PORT="${SMOKE_PORT:-14322}"
PIDS=()
trap 'for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true' EXIT

rm -rf "$W" && mkdir -p "$W/A" "$W/B/work"
printf 'mutmuas smoke line 1: the answer is 42\nsecond line\n' >"$W/B/work/notes.txt"
"$ROOT/.local/bin/nats-server" -js -a 127.0.0.1 -p "$PORT" -sd "$W/js" >"$W/nats.log" 2>&1 &
PIDS+=($!)

cat >"$W/A/node.yaml" <<YAML
project: smoke
node: A
data_dir: ./data
heartbeat_s: 1
nats: {servers: ["nats://127.0.0.1:$PORT"]}
agents:
  - {id: main, mode: interactive, workdir: ./work, permissions: [READ, REQUEST_TASK, PUBLISH_ARTIFACT]}
YAML
cat >"$W/B/node.yaml" <<YAML
project: smoke
node: B
data_dir: ./data
heartbeat_s: 1
nats: {servers: ["nats://127.0.0.1:$PORT"]}
agents:
  - id: llm
    mode: worker
    runtime: $RUNTIME
    model: "$MODEL"
    workdir: ./work
    task_timeout_s: 300
    capabilities: [file_lookup]
    permissions: [READ, PUBLISH_ARTIFACT, REQUEST_TASK]
YAML

"$BIN/agent-node" start --config "$W/B/node.yaml" >"$W/B.log" 2>&1 &
PIDS+=($!)
"$BIN/agent-node" start --config "$W/A/node.yaml" >"$W/A.log" 2>&1 &
PIDS+=($!)
sleep 3

echo "asking B:llm ($RUNTIME${MODEL:+, $MODEL}) ..."
OUT=$("$BIN/agentctl" ask B:llm "Read notes.txt in your working directory. Publish it as an artifact and report its first line in outputs.first_line." \
  --reason "smoke test of the $RUNTIME runtime" --kind artifact \
  --accept "notes.txt published as artifact" --accept "outputs.first_line equals the file's first line" \
  --wait 300 --json --config "$W/A/node.yaml")
echo "$OUT" | "$BIN/python" -c "
import json, sys
d = json.load(sys.stdin)
r = d.get('result') or {}
print('status:', d.get('status'), '| result_status:', d.get('result_status'))
print('summary:', r.get('summary'))
print('outputs:', r.get('outputs'))
print('artifacts:', [a['uri'] for a in d.get('output_refs', [])])
ok = (d.get('result_status') == 'complete' and d.get('output_refs')
      and 'answer is 42' in json.dumps(r.get('outputs', {})))
print('PASS' if ok else 'FAIL (see $W/B/data/runs/*.log)')
sys.exit(0 if ok else 1)"
