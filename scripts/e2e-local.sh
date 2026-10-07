#!/usr/bin/env bash
# Multi-process end-to-end check on one machine: a real nats-server with the generated per-node
# auth + TLS config, two separate `agent-node start` daemons (node A and node B, separate data dirs),
# and everything driven through `agentctl`, exactly as on two real machines.
#
#   TEST 1  A asks B for a file   -> B publishes artifact -> A downloads and verifies sha256
#   TEST 1b same, but B is offline when A sends               -> delivered after B restarts
#   TEST 2  A delegates an experiment to B -> ACK/RUNNING/UPDATE/RESULT + artifact
#   TEST 3  B's brain (auto_worker, no session) reports BLOCKED; A answers naming it next (by its alias)
#           -> it runs again and finishes
#   TEST 4  A pauses and resumes B's experiment; interrupts its own task without touching another one running
#           on the same post, and the next run gets the message (control_task)
#   TEST 6  B:secretary, trusted on B, pauses and resumes a task it did not request (run before TEST 5)
#   TEST 7  a hot deploy: B's daemon restarts while a run and a job go on; both finish, the run once (D-104)
#   TEST 8  a run stops at the usage limit: it waits; B:secretary lists and resumes it (D-104)
#   TEST 5  B retires its post B:data (daemon stopped) and undoes it
#
# Usage: scripts/e2e-local.sh [--keep]     (work dir: .local/e2e/, kept with --keep)
# E2E_PYTHON: the Python to run with (default .venv/bin/python); the code is taken from src/ either way.
set -euo pipefail
unset MUTMUAS_AGENT MUTMUAS_CONFIG MUTMUAS_TASK_ID     # run as the test's own agents, never as the caller's
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
PY="${E2E_PYTHON:-$ROOT/.venv/bin/python}"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
AGENTCTL=("$PY" -m mutmuas.cli)
AGENT_NODE=("$PY" -m mutmuas.cli node)
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
json() { "$PY" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
# check '<python condition on d>' '<label>': exits non-zero (and so fails the script) when false
check() { "$PY" -c "import json,sys; d=json.load(sys.stdin); assert $1, d; print('PASS:', sys.argv[1])" "$2"; }

rm -rf "$W" && mkdir -p "$W/B-disk/representation_exp082" "$W/A-disk"

step "generate server config with per-node credentials and TLS (private CA)"
"${AGENT_NODE[@]}" server-config --project demo --nodes A,B --out "$W/server" --listen 127.0.0.1 \
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
  - id: aux
    mode: interactive
    role: assistant
    workdir: ./work-aux
    permissions: [READ, REQUEST_TASK]
YAML
cat >"$W/B/node.yaml" <<YAML
project: demo
node: B
description: "GPU server (simulated)"
data_dir: ./data
heartbeat_s: 1
resources: {gpu: {type: RTX4090, count: 2}}
trusted_controllers: ["A:main", "B:secretary"]
coordinators: ["B:secretary"]
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
  - id: secretary
    mode: interactive
    role: secretary
    workdir: ./work/secretary
    permissions: [READ, REQUEST_TASK]
  - id: brain
    display: "B:c1"
    mode: interactive
    auto_worker: true
    runtime: script
    command: ["{python}", "$ROOT/tests/handlers/lab.py"]
    role: planner
    workdir: ./work/brain
    permissions: [READ, PUBLISH_ARTIFACT, REQUEST_TASK]
YAML

# what has no agentctl command (MCP tools only): wait for a result, control a task, answer; as A:main
cat >"$W/ctl.py" <<'PY'
import asyncio, json, os, sys
from mutmuas import tools
from mutmuas.config import load_config
from mutmuas.hub import Hub

ME = os.environ.get("CTL_AS", "A:main")

async def main(cmd, task_id, *rest):
    hub = await Hub.open(load_config(sys.argv[-1]), "cli")
    try:
        if cmd == "wait":
            out = await tools.wait_for_result(hub, task_id, 60, me=ME)
        elif cmd == "status":
            out = await hub.task_view(task_id, ME)
        elif cmd == "control":
            out = await tools.control_task(hub, ME, task_id, rest[0], rest[1])
        elif cmd == "answer":
            out = await tools.answer(hub, ME, task_id, rest[0], next=rest[1])
        elif cmd == "quota_waits":
            out = await tools.quota_waits(hub, ME)
        elif cmd == "resume_quota":
            out = await tools.resume_quota_waits(hub, ME)
        print(json.dumps(out, default=str))
    finally:
        await hub.close()

asyncio.run(main(*sys.argv[1:-1]))
PY
ctl() { "$PY" "$W/ctl.py" "$@" "$W/A/node.yaml"; }
ctl_b() { "$PY" "$W/ctl.py" "$@" "$W/B/node.yaml"; }     # as an agent of node B (CTL_AS)
# wait_status <task> '<python condition on d (A's view)>' '<label>'
wait_status() {
  for _ in $(seq 150); do
    ctl status "$1" | "$PY" -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if $2 else 1)" && { echo "PASS: $3"; return 0; }
    sleep 0.2
  done
  echo "FAIL: $3"; ctl status "$1"; exit 1
}

start_node() {
  "${AGENT_NODE[@]}" start --config "$W/$1/node.yaml" >>"$W/$1.log" 2>&1 &
  PIDS+=($!)
  eval "PID_$1=$!"
}

step "start node daemons A and B (separate processes)"
start_node A
start_node B
for _ in $(seq 50); do
  n=$("${AGENTCTL[@]}" agents --json --config "$W/A/node.yaml" 2>/dev/null \
      | json "sum(1 for a in d if a['online'])" 2>/dev/null || echo 0)
  [ "$n" = "6" ] && break
  sleep 0.2
done
"${AGENTCTL[@]}" status --config "$W/A/node.yaml"

step "TEST 1: A:a1 asks B for experiment 82 data (discovered by capability)"
"$PY" -c "
import json, random; random.seed(82)
json.dump({'exp': 82, 'latents': [[random.random() for _ in range(16)] for _ in range(5000)]},
          open('$W/B-disk/representation_exp082/latents.json', 'w'))"
WHO=$("${AGENTCTL[@]}" find representation_data --json --config "$W/A/node.yaml" | json "d['best']['address']")
echo "registry says: $WHO"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" "$WHO" "Return the latent data of representation experiment 82" \
  --reason "A:a1 continues the probing analysis" --kind artifact \
  --input action=fetch_file --input "path=$W/B-disk/representation_exp082/latents.json" \
  --wait 60 --json --config "$W/A/node.yaml")
TASK1=$(echo "$OUT" | json "d['task_id']")
echo "$OUT" | json "d['status'], d['result_status'], d['result']['summary']"
URI=$(echo "$OUT" | json "d['output_refs'][0]['uri']")
GOT=$("${AGENTCTL[@]}" artifact fetch "$URI" --dest "$W/A-disk" --json --config "$W/A/node.yaml" | json "d['path']")
SRC=$(shasum -a 256 "$W/B-disk/representation_exp082/latents.json" | cut -d' ' -f1)
DST=$(shasum -a 256 "$GOT" | cut -d' ' -f1)
[ "$SRC" = "$DST" ] && echo "PASS: A has B's file, sha256 $DST" || { echo "FAIL: checksum differs"; exit 1; }

step "TEST 1b: B goes offline, A sends anyway, B comes back"
kill "$PID_B"; wait "$PID_B" 2>/dev/null || true
sleep 3.5
"${AGENTCTL[@]}" status --config "$W/A/node.yaml" | sed -n '/NODE B/,$p'
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:data "echo while offline" --input action=echo --input text=durable --json \
  --config "$W/A/node.yaml")
echo "$OUT" | json "d['delivery'], d.get('note')"
TASK1B=$(echo "$OUT" | json "d['task_id']")
start_node B
ctl wait "$TASK1B" \
  | check "d['status']=='COMPLETED' and d['result']['summary']=='echo: durable'" "message sent while B was offline was processed after restart"

step "TEST 2: A:a1 delegates an experiment to B:b1 (alias)"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Run the module-B ablation, seed 7" --reason "module A result depends on it" \
  --kind experiment --input action=experiment --input steps=5 --input step_s=0.3 --input seed=7 \
  --accept "5 loss values" --json --config "$W/A/node.yaml")
TASK2=$(echo "$OUT" | json "d['task_id']")
sleep 1
"${AGENTCTL[@]}" status --config "$W/A/node.yaml" | sed -n '/NODE B/,$p'
ctl wait "$TASK2" \
  | check "d['result_status']=='complete' and d['result']['outputs']['final_loss']==0.2" "delegated experiment completed with metrics"
"${AGENTCTL[@]}" task "$TASK2" --config "$W/A/node.yaml"

step "audit: why did B run that experiment? (asked from node B's side)"
"${AGENTCTL[@]}" task "$TASK2" --as B:experimenter --config "$W/B/node.yaml" | sed -n '1,5p'

step "TEST 3: B:c1 (a brain without a session) reports BLOCKED; A:aux (not trusted) answers and names it next"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:c1 "Plan the next dataset" --reason "wake test" --as A:aux \
  --input action=block_once --input "flag=$W/answered" --json --config "$W/A/node.yaml")
TASK3=$(echo "$OUT" | json "d['task_id']")
CTL_AS=A:aux wait_status "$TASK3" "d['status']=='BLOCKED'" "the worker is blocked and its run has ended"
touch "$W/answered"
CTL_AS=A:aux ctl answer "$TASK3" "the dataset is in /data/v2" B:c1 >/dev/null
CTL_AS=A:aux ctl wait "$TASK3" \
  | check "d['status']=='COMPLETED' and d['result']['summary']=='unblocked'" "named next, the blocked task ran again and finished"

step "TEST 4a: A pauses and resumes B:b1's experiment"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Long ablation" --reason "control test" --kind experiment \
  --input action=experiment --input steps=15 --input step_s=0.2 --json --config "$W/A/node.yaml")
TASK4=$(echo "$OUT" | json "d['task_id']")
wait_status "$TASK4" "d['status']=='RUNNING'" "the experiment runs"
ctl control "$TASK4" pause "hold on: the GPU is needed" >/dev/null
wait_status "$TASK4" "d['status']=='WAITING'" "paused: the run is stopped, the task waits"
ctl control "$TASK4" resume "go on" >/dev/null
wait_status "$TASK4" "d['status']=='RUNNING'" "resumed: it runs again"
ctl wait "$TASK4" | check "d['result_status']=='complete'" "the resumed experiment finished"
RUNS=$(ls "$W/B/data/runs/" | grep -c "^$TASK4\..*\.log$")
[ "$RUNS" = "2" ] && echo "PASS: two runs: before the pause and after resume" || { echo "FAIL: $RUNS runs"; exit 1; }

step "TEST 4b: A interrupts its own queued task; A:aux's task running on the same post is left alone"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Someone else's run" --reason "bystander" --kind experiment --as A:aux \
  --input action=experiment --input steps=15 --input step_s=0.2 --json --config "$W/A/node.yaml")
OTHER=$(echo "$OUT" | json "d['task_id']")
CTL_AS=A:aux wait_status "$OTHER" "d['status']=='RUNNING'" "A:aux's experiment runs"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Short check" --reason "interrupt test" --kind experiment \
  --input action=experiment --input steps=2 --input step_s=0.1 --json --config "$W/A/node.yaml")
MINE=$(echo "$OUT" | json "d['task_id']")
ctl control "$MINE" interrupt "use seed 9 from now on" >/dev/null
CTL_AS=A:aux ctl wait "$OTHER" \
  | check "d['result_status']=='complete' and d['result']['outputs']['told']==[]" "the running bystander was neither stopped nor told"
RUNS=$(ls "$W/B/data/runs/" | grep -c "^$OTHER\..*\.log$")
[ "$RUNS" = "1" ] && echo "PASS: the bystander ran once" || { echo "FAIL: bystander ran $RUNS times"; exit 1; }
ctl wait "$MINE" \
  | check "d['result_status']=='complete' and any('use seed 9' in t for t in d['result']['outputs']['told'])" "the next run of the interrupted task got the message"

step "TEST 6: B:secretary (trusted on B) pauses and resumes a task A:aux asked B for"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Another long run" --reason "secretary control" --kind experiment --as A:aux \
  --input action=experiment --input steps=15 --input step_s=0.2 --json --config "$W/A/node.yaml")
TASK6=$(echo "$OUT" | json "d['task_id']")
CTL_AS=A:aux wait_status "$TASK6" "d['status']=='RUNNING'" "A:aux's experiment runs"
CTL_AS=B:secretary ctl_b control "$TASK6" pause "the leader needs the GPU" >/dev/null
CTL_AS=A:aux wait_status "$TASK6" "d['status']=='WAITING'" "paused by the secretary, who did not request it"
CTL_AS=B:secretary ctl_b control "$TASK6" resume "go on" >/dev/null
CTL_AS=A:aux ctl wait "$TASK6" \
  | check "d['result_status']=='complete' and any('the leader needs the GPU' in t for t in d['result']['outputs']['told'])" \
          "resumed and finished; the run after resume was told why it was paused"

step "TEST 7: a hot deploy: B's daemon restarts while a run and a job are in progress"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Job across the deploy" --reason "hot deploy" --kind experiment \
  --input action=job_then_result --input "flag=$W/job7.done" --input job_s=12 --json --config "$W/A/node.yaml")
JOB7=$(echo "$OUT" | json "d['task_id']")
wait_status "$JOB7" "d['status']=='WAITING'" "a job is started and its task waits on it"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Run across the deploy" --reason "hot deploy" --kind experiment \
  --input action=experiment --input steps=40 --input step_s=0.25 --json --config "$W/A/node.yaml")
RUN7=$(echo "$OUT" | json "d['task_id']")
wait_status "$RUN7" "d['status']=='RUNNING'" "a run is under way"
[ -f "$W/job7.done" ] && { echo "FAIL: the job ended before the deploy"; exit 1; }
kill "$PID_B"; wait "$PID_B" 2>/dev/null || true             # the old daemon stops (SIGTERM, as a deploy does)
start_node B                                                  # the new one starts
ctl wait "$RUN7" | check "d['result_status']=='complete' and d['result']['outputs']['final_loss']==0.025" \
  "the run went on across the restart and its result was delivered"
grep -q "task $RUN7: adopted its run" "$W/B.log" && echo "PASS: the new daemon adopted the run still going" \
  || { echo "FAIL: the run was not adopted while it ran"; exit 1; }
RUNS=$(ls "$W/B/data/runs/" | grep -c "^$RUN7\..*\.log$")
[ "$RUNS" = "1" ] && echo "PASS: it ran once (not again after the restart)" || { echo "FAIL: $RUNS runs"; exit 1; }
ctl wait "$JOB7" | check "d['result_status']=='complete' and d['result']['summary']=='job finished'" \
  "the job started before the deploy ran to its end and woke its task"

step "TEST 8: a run stops at the usage limit; B:secretary lists and resumes what waits for quota"
OUT=$("${AGENTCTL[@]}" ask --reason "end-to-end check" --expect "the result" --accept "as asked" B:b1 "Out of quota" --reason "quota" --kind experiment \
  --input action=quota_once --input "flag=$W/quota8" --json --config "$W/A/node.yaml")
TASK8=$(echo "$OUT" | json "d['task_id']")
wait_status "$TASK8" "d['status']=='WAITING'" "it waits instead of failing"
CTL_AS=B:secretary ctl_b quota_waits - | check "[t['task_id'] for t in d]==['$TASK8']" "quota_waits lists it"
CTL_AS=B:secretary ctl_b resume_quota - | check "d['resumed']==['$TASK8']" "resume_quota_waits resumes it"
ctl wait "$TASK8" | check "d['result_status']=='complete' and d['result']['summary']=='done after the limit came back'" \
  "it ran again and finished"

step "TEST 5: retire B:data (B's daemon stopped), then undo"
kill "$PID_B"; wait "$PID_B" 2>/dev/null || true
"${AGENT_NODE[@]}" retire-agent data --config "$W/B/node.yaml" --hand-over B:b1 -y >"$W/retire.out"
grep -q "id: data" "$W/B/node.yaml" && { echo "FAIL: B:data still configured"; exit 1; } || echo "PASS: B:data out of node.yaml"
"${AGENTCTL[@]}" agents --json --config "$W/A/node.yaml" \
  | check "not any(a['address']=='B:data' for a in d)" "the card of B:data is gone from the registry"
MANIFEST=$(ls "$W"/B/RETIRED-data-*.json)
"${AGENT_NODE[@]}" retire-agent --undo "$MANIFEST" --config "$W/B/node.yaml" -y >/dev/null
grep -q "id: data" "$W/B/node.yaml" && echo "PASS: undo put B:data back into node.yaml" || { echo "FAIL: not restored"; exit 1; }
start_node B
for _ in $(seq 50); do
  "${AGENTCTL[@]}" agents --json --config "$W/A/node.yaml" \
    | "$PY" -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if any(a['address']=='B:data' and a['online'] for a in d) else 1)" && break
  sleep 0.2
done
"${AGENTCTL[@]}" agents --json --config "$W/A/node.yaml" \
  | check "any(a['address']=='B:data' and a['online'] for a in d)" "B's daemon started again: B:data is back online"

step "all end-to-end checks passed"
