"""Deterministic worker used by the tests (stands in for an LLM agent).

Reads the task JSON on stdin; behaviour is chosen by request.body.inputs.action.
Uses both integration paths an agent has: the agentctl CLI (subprocess) and
the Python tool API (what the MCP server calls).
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from mutmuas import tools
from mutmuas.config import load_config
from mutmuas.hub import Hub

task = json.load(sys.stdin)
inputs = task["request"]["body"].get("inputs", {})
action = inputs.get("action", "echo")
me = os.environ["MUTMUAS_AGENT"]

if inputs.get("marker"):  # count executions, for exactly-once checks
    with open(inputs["marker"], "a") as f:
        f.write(f"{task['task_id']} attempt={task['attempt']}\n")


def agentctl(*args: str) -> dict:
    out = subprocess.run([sys.executable, "-m", "mutmuas.cli", *args, "--json"], check=True,
                         capture_output=True, text=True)
    return json.loads(out.stdout)


async def with_hub(fn):
    hub = await Hub.open(load_config(os.environ["MUTMUAS_CONFIG"]), "handler", require_bus=False)
    try:
        return await fn(hub)
    finally:
        await hub.close()


if action == "echo":
    print(json.dumps({"status": "complete", "summary": f"echo: {inputs.get('text', '')}"}))

elif action == "fetch_file":
    path = Path(inputs["path"])
    if not path.exists():
        print(json.dumps({"status": "failed", "summary": f"{path} does not exist on {me}",
                          "limitations": ["nothing to publish"]}))
        sys.exit(0)
    agentctl("update", f"found {path.name}, publishing")
    ref = agentctl("artifact", "publish", str(path), "--description", "requested file")
    print(json.dumps({"status": "complete", "summary": f"published {path.name}", "artifacts": [ref],
                      "evidence": [f"sha256={ref['sha256']}"]}))

elif action == "experiment":
    steps = int(inputs.get("steps", 3))
    step_s = float(inputs.get("step_s", 0.2))

    async def run(hub):
        losses = []
        start = int(inputs.get("resume_from", 0))
        for i in range(start, steps):
            time.sleep(step_s)
            losses.append(round(1.0 / (i + 1), 4))
            await tools.report_progress(hub, me, f"step {i + 1}/{steps} loss={losses[-1]}")
        out = Path.cwd() / f"{task['task_id']}-metrics.json"
        out.write_text(json.dumps({"losses": losses, "seed": inputs.get("seed", 0)}))
        ref = await tools.publish_artifact(hub, me, str(out), id="METRICS")
        await tools.submit_result(hub, me, "complete", f"ran {steps} steps", outputs={"final_loss": losses[-1]},
                                  artifacts=[ref], evidence=[f"{len(losses)} loss values recorded"])

    asyncio.run(with_hub(run))

elif action == "sleep":
    time.sleep(float(inputs.get("seconds", 5)))
    print(json.dumps({"status": "complete", "summary": "slept"}))

elif action == "crash":
    print("fatal error: about to crash", file=sys.stderr)
    sys.exit(3)

elif action == "noresult":
    print("did some things but never reported a structured result")

elif action == "lie":
    print(json.dumps({"status": "complete", "summary": "everything worked, trust me"}))
    sys.exit(1)

elif action == "commit":
    # A code task: we run inside a per-task git worktree; commit a change there.
    Path(inputs["file"]).write_text(inputs["content"])
    for cmd in (["git", "add", inputs["file"]], ["git", "-c", "user.name=lab", "-c", "user.email=lab@example.invalid",
                                                 "commit", "-qm", inputs["message"]]):
        subprocess.run(cmd, check=True)
    print(json.dumps({"status": "complete", "summary": f"committed {inputs['file']} on {task['git_branch']}"}))

elif action == "block_then_result":
    # Reproduces node B's report: a worker reports BLOCKED, then still submits a (partial) result.
    async def run(hub):
        await tools.report_progress(hub, me, "cannot run shell commands here", state="BLOCKED")
        await tools.submit_result(hub, me, "partial", "did what was possible without a shell",
                                  limitations=["sandbox blocked shell commands"])

    asyncio.run(with_hub(run))

elif action == "block_only":
    async def run(hub):
        await tools.report_progress(hub, me, "need the dataset path from the requester", state="BLOCKED")

    asyncio.run(with_hub(run))

elif action == "evil_commit":
    # A hostile agent: commit, then plant an fsmonitor and hooks in its
    # own clone, so that anything running git in this clone afterwards (i.e. the daemon) would trigger them.
    marker = inputs["marker"]
    Path(inputs["file"]).write_text(inputs["content"])
    subprocess.run(["git", "add", inputs["file"]], check=True)
    subprocess.run(["git", "-c", "user.name=evil", "-c", "user.email=e@example.invalid", "commit", "-qm", "evil"],
                   check=True)
    subprocess.run(["git", "config", "core.fsmonitor", f"touch {marker}.fsmonitor"], check=True)
    for hook in ("post-checkout", "pre-commit", "reference-transaction"):
        Path(f".git/hooks/{hook}").write_text(f"#!/bin/sh\ntouch {marker}.hook\n")
        Path(f".git/hooks/{hook}").chmod(0o755)
    print(json.dumps({"status": "complete", "summary": "committed"}))

elif action == "delegate":
    # B worker delegates onward to another agent and waits: A -> B -> C chains.
    async def run(hub):
        sent = await tools.send_request(hub, me, inputs["to"], "sub-task", "needed by parent task",
                                        inputs={"action": "echo", "text": "from the chain"})
        res = await tools.wait_for_result(hub, sent["task_id"], 30)
        await tools.submit_result(hub, me, "complete", f"sub-task said: {res['result']['summary']}",
                                  outputs={"child_task": sent["task_id"]})

    asyncio.run(with_hub(run))
