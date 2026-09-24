#!/usr/bin/env bash
# Multi-process end-to-end check on one machine: a real nats-server with the generated per-node
# auth + TLS config, two separate `agent-node start` daemons (node A and node B, separate data dirs),
# and everything driven through `agentctl`, exactly as on two real machines.
#
#   TEST 1  A asks B for a file   -> B publishes artifact -> A downloads and verifies sha256
#   TEST 1b same, but B is offline when A sends               -> delivered after B restarts
#   TEST 2  A delegates an experiment to B -> ACK/RUNNING/UPDATE/RESULT + artifact
#
# Usage: scripts/e2e-local.sh [--keep]     (work dir: .local/e2e/, kept with --keep)
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
BIN="$ROOT/.venv/bin"
NATS="${NATS_SERVER_BIN:-$ROOT/.local/bin/nats-server}"
W="$ROOT/.local/e2e"
PORT="${E2E_PORT:-14222}"
KEEP="${1:-}"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  [ "$KEEP" = "--keep" ] || rm -rf "$W"
}
trap cleanup EXIT
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
json() { "$BIN/python" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
# check '<python condition on d>' '<label>': exits non-zero (and so fails the script) when false
check() { "$BIN/python" -c "import json,sys; d=json.load(sys.stdin); assert $1, d; print('PASS: $2')"; }

rm -rf "$W" && mkdir -p "$W/B-disk/representation_exp082" "$W/A-disk"

step "generate server config with per-node credentials and TLS (private CA)"
"$BIN/agent-node" server-config --project demo --nodes A,B --out "$W/server" --listen 127.0.0.1 \
  --port "$PORT" --store-dir "$W/jetstream" --tls 127.0.0.1 >/dev/null
sed -i.bak "s/^http: .*/http: 127.0.0.1:$((PORT + 1))/" "$W/server/nats-server.conf"
"$NATS" -c "$W/server/nats-server.conf" >"$W/nats.log" 2>&1 &
PIDS+=($!)

for node in A B; do
  mkdir -p "$W/$node"
done
cat >"$W/A/node.yaml" <<YAML
project: demo
node: A
description: "Mac / local main terminal"
data_dir: ./data
heartbeat_s: 1
nats: {servers: ["nats://127.0.0.1:$PORT"], credentials_file: ../server/A.env, tls_ca: ../server/tls/ca.crt}
agents:
  - id: main
    display: "A:a1"
    mode: interactive
    role: lead-researcher
    workdir: ./work
    permissions: [READ, REQUEST_TASK, PUBLISH_ARTIFACT]
YAML
cat >"$W/B/node.yaml" <<YAML
project: demo
node: B
description: "GPU server (simulated)"
data_dir: ./data
heartbeat_s: 1
resources: {gpu: {type: RTX4090, count: 2}}
nats: {servers: ["nats://127.0.0.1:$PORT"], credentials_file: ../server/B.env, tls_ca: ../server/tls/ca.crt}
agents:
  - id: data
    display: "B:a1"
    mode: worker
    runtime: script
    command: ["{python}", "$ROOT/tests/handlers/lab.py"]
    role: data-keeper
    capabilities: [representation_data]
    workdir: ./work/data
    accept_from: ["A:*", "B:*"]
    permissions: [READ, PUBLISH_ARTIFACT, REQUEST_TASK]
  - id: experimenter
    display: "B:b1"
    mode: worker
    runtime: script
    command: ["{python}", "$ROOT/tests/handlers/lab.py"]
    role: experimenter
    capabilities: [isaac_lab, gpu_training]
    workdir: ./work/exp
    permissions: [READ, RUN_EXPERIMENT, PUBLISH_ARTIFACT, REQUEST_TASK]
YAML

start_node() {
  "$BIN/agent-node" start --config "$W/$1/node.yaml" >>"$W/$1.log" 2>&1 &
  PIDS+=($!)
  eval "PID_$1=$!"
}

step "start node daemons A and B (separate processes)"
start_node A
start_node B
for _ in $(seq 50); do
  n=$("$BIN/agentctl" agents --json --config "$W/A/node.yaml" 2>/dev/null \
      | json "sum(1 for a in d if a['online'])" 2>/dev/null || echo 0)
  [ "$n" = "3" ] && break
  sleep 0.2
done
"$BIN/agentctl" status --config "$W/A/node.yaml"

step "TEST 1: A:a1 asks B for experiment 82 data (discovered by capability)"
"$BIN/python" -c "
import json, random; random.seed(82)
json.dump({'exp': 82, 'latents': [[random.random() for _ in range(16)] for _ in range(5000)]},
          open('$W/B-disk/representation_exp082/latents.json', 'w'))"
WHO=$("$BIN/agentctl" find representation_data --json --config "$W/A/node.yaml" | json "d['best']['address']")
echo "registry says: $WHO"
OUT=$("$BIN/agentctl" ask "$WHO" "Return the latent data of representation experiment 82" \
  --reason "A:a1 continues the probing analysis" --kind artifact \
  --input action=fetch_file --input "path=$W/B-disk/representation_exp082/latents.json" \
  --wait 60 --json --config "$W/A/node.yaml")
TASK1=$(echo "$OUT" | json "d['task_id']")
echo "$OUT" | json "d['status'], d['result_status'], d['result']['summary']"
"$BIN/agentctl" result "$TASK1" --fetch "$W/A-disk" --config "$W/A/node.yaml" | grep fetched
SRC=$(shasum -a 256 "$W/B-disk/representation_exp082/latents.json" | cut -d' ' -f1)
DST=$(shasum -a 256 "$W/A-disk/latents.json" | cut -d' ' -f1)
[ "$SRC" = "$DST" ] && echo "PASS: A has B's file, sha256 $DST" || { echo "FAIL: checksum differs"; exit 1; }

step "TEST 1b: B goes offline, A sends anyway, B comes back"
kill "$PID_B"; wait "$PID_B" 2>/dev/null || true
sleep 3.5
"$BIN/agentctl" status --config "$W/A/node.yaml" | sed -n '/NODE B/,$p'
OUT=$("$BIN/agentctl" ask B:data "echo while offline" --input action=echo --input text=durable --json \
  --config "$W/A/node.yaml")
echo "$OUT" | json "d['delivery'], d.get('note')"
TASK1B=$(echo "$OUT" | json "d['task_id']")
start_node B
"$BIN/agentctl" result "$TASK1B" --wait 60 --json --config "$W/A/node.yaml" \
  | check "d['status']=='COMPLETED' and d['result']['summary']=='echo: durable'" "message sent while B was offline was processed after restart"

step "TEST 2: A:a1 delegates an experiment to B:b1 (alias)"
OUT=$("$BIN/agentctl" ask B:b1 "Run the module-B ablation, seed 7" --reason "module A result depends on it" \
  --kind experiment --input action=experiment --input steps=5 --input step_s=0.3 --input seed=7 \
  --accept "5 loss values" --json --config "$W/A/node.yaml")
TASK2=$(echo "$OUT" | json "d['task_id']")
sleep 1
"$BIN/agentctl" status --config "$W/A/node.yaml" | sed -n '/NODE B/,$p'
"$BIN/agentctl" result "$TASK2" --wait 60 --json --config "$W/A/node.yaml" \
  | check "d['result_status']=='complete' and d['result']['outputs']['final_loss']==0.2" "delegated experiment completed with metrics"
"$BIN/agentctl" task "$TASK2" --config "$W/A/node.yaml"

step "audit: why did B run that experiment? (asked from node B's side)"
"$BIN/agentctl" task "$TASK2" --config "$W/B/node.yaml" | sed -n '1,5p'

step "all end-to-end checks passed"
