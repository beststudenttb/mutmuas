#!/usr/bin/env python3
"""Probe: can an MCP server push messages into a *running* Claude Code session (channels)?

Standalone, standard library only. A minimal stdio MCP server that declares the experimental
`claude/channel` capability and, once initialized, sends a `notifications/claude/channel` every
INTERVAL seconds. If the session shows these as incoming messages, mutmuas can wake a Claude
session on new mail without a watcher and without tmux.

Try it (a throwaway session; it asks for confirmation once at startup):
  claude mcp add -s local mmprobe -- python3 /path/to/channel_probe.py
  claude --dangerously-load-development-channels server:mmprobe
Result: the session receives "mutmuas channel probe #1 ..." within INTERVAL seconds, or it does not
(e.g. "Channels are not enabled for your org"). Remove afterwards: claude mcp remove -s local mmprobe
"""
import json
import os
import sys
import threading
import time

INTERVAL = float(os.environ.get("PROBE_INTERVAL", "30"))
_lock = threading.Lock()


def send(msg: dict) -> None:
    with _lock:
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()


def pusher() -> None:
    n = 0
    while True:
        time.sleep(INTERVAL)
        n += 1
        send({"jsonrpc": "2.0", "method": "notifications/claude/channel",
              "params": {"content": f"mutmuas channel probe #{n}: if you can read this, channels work. "
                                    "No action needed.",
                         "meta": {"source": "mutmuas_probe", "seq": str(n)}}})


def main() -> None:
    started = False
    for line in sys.stdin:
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        method, mid = msg.get("method"), msg.get("id")
        if method == "initialize":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": msg.get("params", {}).get("protocolVersion", "2025-06-18"),
                "capabilities": {"experimental": {"claude/channel": {}}, "tools": {}},
                "serverInfo": {"name": "mmprobe", "version": "0.1"},
                "instructions": "Test server: it only pushes probe messages over the claude/channel "
                                "capability. Report whether they arrive; no action needed."}})
        elif method == "notifications/initialized" and not started:
            started = True
            threading.Thread(target=pusher, daemon=True).start()
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": mid, "result": {"tools": []}})
        elif mid is not None:        # ping and anything else: answer, never hang the client
            send({"jsonrpc": "2.0", "id": mid, "result": {}})


if __name__ == "__main__":
    main()
