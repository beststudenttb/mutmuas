"""Sink worker for the recovery-test sandbox: records every task it gets and answers "received".

Stands in for every other agent of the sandbox network, so that a subject's messages reach
something that answers, and nothing that thinks (R3.4: test traffic goes to sinks only).
"""

import json
import os
import sys
import time
from pathlib import Path

task = json.load(sys.stdin)
log = Path(os.environ["SANDBOX_SINK_LOG"])
with log.open("a") as f:
    f.write(json.dumps({"at": time.time(), "to": os.environ.get("MUTMUAS_AGENT"), "task": task},
                       ensure_ascii=False) + "\n")
objective = task["request"]["body"].get("objective", "")
replies = json.loads(Path(os.environ["SANDBOX_REPLIES"]).read_text() or "{}")   # seeding only, then {}
print(json.dumps({"status": "complete", "summary": replies.get(objective, "received (sandbox sink)")},
                 ensure_ascii=False))
