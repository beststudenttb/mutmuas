#!/usr/bin/env python3
"""Deployment self-test worker: answers any REQUEST with facts about the machine it runs on.

Use it as a `runtime: script` agent (see docs/DEPLOYMENT.md, "Test A -> B"). It needs only
the standard library: the task arrives as JSON on stdin; the last JSON line on stdout is the RESULT.
"""
import json
import platform
import shutil
import socket
import subprocess
import sys

task = json.load(sys.stdin)
facts = {"hostname": socket.gethostname(), "platform": f"{platform.system()} {platform.machine()}",
         "python": platform.python_version(),
         "claude_cli": bool(shutil.which("claude")), "codex_cli": bool(shutil.which("codex"))}
if shutil.which("nvidia-smi"):
    out = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,memory.used", "--format=csv,noheader"],
                         capture_output=True, text=True)
    facts["gpus"] = [line.strip() for line in out.stdout.splitlines() if line.strip()]
print(json.dumps({"status": "complete", "summary": f"pong from {task['agent']} on {facts['hostname']}",
                  "outputs": facts}))
