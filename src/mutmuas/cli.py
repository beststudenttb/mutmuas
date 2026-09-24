"""Command line: ``agentctl`` (use/inspect the network) and ``agent-node`` (run/manage a node).

The CLI is for humans and scripts (debugging, administration). LLM agents use
the same operations through the MCP server (``agentctl mcp``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from . import __version__, tools
from .bus import Bus, BusUnavailable
from .config import ConfigError, NodeConfig, dump_config, find_config, load_config
from .hub import Hub, is_online
from .ids import Address, parse_iso
from .protocol import ArtifactRef, Envelope, ProtocolError

# --------------------------------------------------------------------------- helpers


def _cfg(args) -> NodeConfig:
    return load_config(find_config(getattr(args, "config", None)))


def _me(args) -> str | None:
    return getattr(args, "as_agent", None) or os.environ.get("MUTMUAS_AGENT") or None


def _print(value: Any, as_json: bool) -> None:
    if as_json or not isinstance(value, (list, dict)):
        print(json.dumps(value, indent=2, ensure_ascii=False, default=str) if not isinstance(value, str) else value)
    else:
        print(yaml.safe_dump(value, sort_keys=False, allow_unicode=True, width=110).rstrip())


def _ago(ts: str | None) -> str:
    if not ts:
        return "never"
    secs = (datetime.now(timezone.utc) - parse_iso(ts)).total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs >= size:
            return f"{secs / size:.0f}{unit} ago"
    return f"{secs:.0f}s ago"


async def _with_hub(args, fn, *, require_bus: bool = True):
    hub = await Hub.open(_cfg(args), "cli", require_bus=require_bus, reconnect=False)
    try:
        return await fn(hub)
    finally:
        await hub.close()


def _parse_kv(pairs: list[str] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep:
            raise SystemExit(f"expected key=value, got {pair!r}")
        try:
            out[key] = json.loads(value)
        except ValueError:
            out[key] = value
    return out


def _load_file(path: str) -> dict[str, Any]:
    try:
        data = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as e:
        raise SystemExit(f"error: {path} is not valid YAML: {e}")
    if not isinstance(data, dict):
        raise SystemExit(f"{path} must contain a mapping")
    return data


# --------------------------------------------------------------------------- agentctl commands


async def cmd_status(args, hub: Hub):
    nodes = await hub.nodes()
    cards = await hub.agents()
    if args.json:
        return _print({"nodes": nodes, "agents": cards}, True)
    by_node: dict[str, list] = {}
    for c in cards:
        by_node.setdefault(c["node"], []).append(c)
    known = {n["node"]: n for n in nodes}
    for node in sorted(set(known) | set(by_node)):
        n = known.get(node, {})
        online = "ONLINE" if n.get("online") else "OFFLINE"
        extra = f"  outbox={n['outbox_queued']}" if n.get("outbox_queued") else ""
        code = (f"  code {n['code']}" if n.get("code") else "") + (f"  py {n['python']}" if n.get("python") else "")
        print(f"NODE {node}  {online}  {n.get('platform', '')}{code}  heartbeat {_ago(n.get('last_heartbeat'))}{extra}")
        for c in by_node.get(node, []):
            state = "OFFLINE" if not c["online"] else c.get("state", "?").upper()
            alias = f" ({c['display']})" if c.get("display") else ""
            print(f"  {c['address']}{alias}  {state}  [{c.get('mode')}/{c.get('runtime') or '-'}]")
            details = []
            if c.get("current_task"):
                details.append(f"task: {c['current_task']}")
            if c.get("queue"):
                details.append(f"queued: {c['queue']}")
            if c.get("open_tasks"):
                details.append(f"open: {c['open_tasks']}")
            if c.get("inbox_unread"):
                details.append(f"inbox: {c['inbox_unread']}")
            if details:
                print("    " + "  ".join(details))
    if not known and not by_node:
        print("no nodes registered yet (is any agent-node running?)")


async def cmd_agents(args, hub: Hub):
    rows = await tools.list_agents(hub, args.capability, args.online)
    if args.json:
        return _print(rows, True)
    for c in rows:
        flag = "online " if c.get("online") else "offline"
        print(f"{c['address']:<28} {flag} {c.get('state', ''):<8} {c.get('role', ''):<28} "
              f"{','.join(c.get('capabilities', []))}")


async def cmd_find(args, hub: Hub):
    _print(await tools.find_agent(hub, args.capability), args.json)


async def cmd_ask(args, hub: Hub):
    out = await tools.send_request(
        hub, _me(args), args.to, args.objective, args.reason or "requested via agentctl", kind=args.kind,
        inputs=_parse_kv(args.input) or None, expected_outputs=args.expect, acceptance_criteria=args.accept,
        constraints=args.constraint, timeout_s=args.timeout, priority=args.priority)
    if args.wait is not None:
        out = await tools.wait_for_result(hub, out["task_id"], args.wait)
    _print(out, args.json)


async def cmd_send(args, hub: Hub):
    data = _load_file(args.file) if args.file else {}
    body = dict(data.get("body", data))
    artifacts = data.get("artifacts", []) if "body" in data else body.pop("artifacts", [])
    msg_type = args.type.upper()
    if msg_type == "REQUEST":
        fields = ("kind", "inputs", "expected_outputs", "constraints", "acceptance_criteria", "timeout_s", "deadline")
        unknown = sorted(set(body) - set(fields) - {"objective", "reason", "priority"})
        if unknown:   # never drop content silently; free-form data belongs in inputs
            raise SystemExit(f"error: unknown REQUEST field(s) {unknown}; put free-form data under 'inputs'. "
                             f"Allowed: objective, reason, priority, {', '.join(fields)}")
        out = await tools.send_request(
            hub, _me(args), args.to, body.get("objective") or args.objective or "", body.get("reason") or "",
            artifacts=artifacts, priority=body.get("priority", "normal"),
            **{k: body[k] for k in fields if k in body})
    else:
        if not args.task:
            raise SystemExit(f"{msg_type} needs --task")
        addr, _ = hub.local_agent(_me(args))
        env = Envelope(type=msg_type, sender=str(addr), to=await hub.resolve(args.to), body=body,
                       task_id=args.task, artifacts=[ArtifactRef.from_dict(a) for a in artifacts])
        out = {"message_id": env.message_id, "delivery": await hub.send(env)}
    _print(out, args.json)


async def cmd_tasks(args, hub: Hub):
    if args.all:
        rows = await hub.all_tasks(args.limit)
    else:
        rows = hub.ledger.tasks(limit=args.limit)
    if args.json:
        return _print(rows, True)
    if not rows:
        print("no tasks")
    for t in rows:
        objective = t.get("objective") or (t.get("request") or {}).get("objective") or ""
        role = t.get("role", "")
        res = f"({t['result_status']})" if t.get("result_status") else ""
        print(f"{t['task_id']:<30} {t['status'] + res:<20} {t['requester']} -> {t['owner']}  "
              f"{role:<9} {objective[:60]}")


async def cmd_task(args, hub: Hub):
    view = await hub.task_view(args.task_id)
    if view is None:
        raise SystemExit(f"unknown task {args.task_id}")
    if args.json:
        return _print(view, True)
    req = view.get("request") or {}
    print(f"task      {view['task_id']}")
    print(f"status    {view.get('status')}" + (f" ({view['result_status']})" if view.get("result_status") else ""))
    print(f"requester {view.get('requester')}   owner {view.get('owner')}   parent {view.get('parent_task') or '-'}")
    print(f"objective {req.get('objective') or view.get('objective')}")
    print(f"reason    {req.get('reason') or view.get('reason')}")
    if view.get("result"):
        print("result:")
        print("  " + yaml.safe_dump(view["result"], allow_unicode=True, sort_keys=False).rstrip().replace("\n", "\n  "))
    for ref in view.get("output_refs") or []:
        print(f"artifact  {ref.get('id', '')}  {ref['uri']}  {ref.get('size', '')} bytes")
    print("history:")
    for m in view.get("thread") or []:
        print(f"  {m.get('timestamp')}  {m.get('type'):<8} {m.get('from')} -> {m.get('to')}  "
              f"{tools._note(m)[:90]}" + (f"  [{m['delivery']}]" if m.get("delivery") == "queued" else ""))


async def cmd_result(args, hub: Hub):
    if args.wait is not None:
        out = await tools.wait_for_result(hub, args.task_id, args.wait)
    else:
        out = await tools.check_task(hub, args.task_id)
    _print(out, args.json)
    if args.fetch and out.get("output_refs"):
        for ref in out["output_refs"]:
            path = await hub.artifacts.fetch(ArtifactRef.from_dict(ref), args.fetch)
            print(f"fetched {ref['uri']} -> {path}")


async def cmd_inbox(args, hub: Hub):
    types = None
    if args.only:
        named = {"actionable": tools.ACTIONABLE, "wake": tools.WAKE}
        types = named.get(args.only) or tuple(t.strip().upper() for t in args.only.split(","))
    rows = await tools.inbox(hub, _me(args), include_seen=args.all, peek=args.peek, wait_s=args.wait, types=types,
                             since=args.since)
    if args.json:
        _print(rows, True)
    elif not rows:
        print("inbox empty")
    for m in rows:
        if not args.json:
            print(f"{m['timestamp']}  {m['type']:<8} from {m['from']:<24} task {m['task_id']}  "
                  f"{tools._note({'body': m['body']})[:80]}")
    if args.wait is not None and not rows:
        raise SystemExit(3)      # timed out: lets a watcher loop tell "nothing yet" from "new mail"


async def cmd_watch(args, hub: Hub):
    """Long-running notifier for an interactive agent: one desktop notification per new actionable message.

    Never marks mail read (the agent's session does that), keeps a --since cursor on disk so nothing is
    announced twice, and backs off instead of spinning on errors. Meant to run under launchd/systemd.
    """
    me = _me(args)
    addr, _ = hub.local_agent(me)
    cursor_file = hub.cfg.data_path / f"{addr.node}_{addr.agent}.notify-cursor"
    if not cursor_file.exists():
        cursor_file.write_text(datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
    while True:
        try:
            rows = await tools.inbox(hub, me, peek=True, wait_s=args.interval, types=tools.ACTIONABLE,
                                     since=cursor_file.read_text().strip())
        except Exception as e:
            print(f"{datetime.now():%F %T} inbox failed: {e!r}", flush=True)
            await asyncio.sleep(30)
            continue
        if not rows:
            continue
        first = rows[0]
        body = first["body"]
        note = body.get("objective") or body.get("summary") or body.get("question") or body.get("reason") or ""
        text = f"{len(rows)} new: {first['type']} from {first['from']}: {str(note)[:120]}"
        _desktop_notify(f"mutmuas → {addr}", text, dry_run=args.dry_run)
        cursor_file.write_text(max(r["received_at"] for r in rows))


def _desktop_notify(title: str, text: str, dry_run: bool = False) -> None:
    print(f"{datetime.now():%F %T} notify: {title} — {text}", flush=True)
    if dry_run:
        return
    # Message text comes from other agents: pass it as argv, never splice it into script source.
    if shutil.which("osascript"):
        subprocess.run(["osascript", "-e", "on run argv", "-e",
                        'display notification (item 2 of argv) with title (item 1 of argv) sound name "Glass"',
                        "-e", "end run", title, text], capture_output=True)
    elif shutil.which("notify-send"):
        subprocess.run(["notify-send", title, text], capture_output=True)


async def cmd_cancel(args, hub: Hub):
    _print(await tools.cancel_task(hub, _me(args), args.task_id, args.reason or ""), args.json)


async def cmd_accept(args, hub: Hub):
    _print(await tools.accept_task(hub, _me(args), args.task_id), args.json)


async def cmd_reject(args, hub: Hub):
    _print(await tools.reject_task(hub, _me(args), args.task_id, args.reason), args.json)


async def cmd_update(args, hub: Hub):
    _print(await tools.report_progress(hub, _me(args), args.message, args.task, args.state), args.json)


async def cmd_submit(args, hub: Hub):
    data = _load_file(args.file) if args.file else {}
    artifacts = data.pop("artifacts", []) + [{"uri": u} for u in args.artifact or []]
    status = args.status or data.pop("status", None)
    summary = args.summary or data.pop("summary", None)
    if not status or not summary:
        raise SystemExit("--status and --summary (or a --file with them) are required")
    _print(await tools.submit_result(hub, _me(args), status, summary, task_id=args.task, artifacts=artifacts,
                                     **{k: data[k] for k in ("outputs", "evidence", "limitations", "follow_up")
                                        if k in data}), args.json)


async def cmd_question(args, hub: Hub):
    _print(await tools.ask_question(hub, _me(args), args.task_id, args.text), args.json)


async def cmd_answer(args, hub: Hub):
    _print(await tools.answer(hub, _me(args), args.task_id, args.text), args.json)


async def cmd_artifact(args, hub: Hub):
    if args.action == "publish":
        out = await tools.publish_artifact(hub, _me(args), args.target, key=args.key, description=args.description,
                                           backend=args.backend, task_id=args.task)
    elif args.action == "fetch":
        out = await tools.fetch_artifact(hub, args.target, args.dest)
    else:
        out = await hub.artifacts.list()
    _print(out, args.json)


async def cmd_history(args, hub: Hub):
    """Raw audit trail straight from the JetStream stream (all nodes)."""
    rows = []
    for subject, data in await hub.bus.history(limit=args.limit):
        try:
            env = Envelope.from_json(data)
        except ProtocolError:
            rows.append({"subject": subject, "invalid": True})
            continue
        if args.task and env.task_id != args.task:
            continue
        rows.append(env.to_dict())
    if args.json:
        return _print(rows, True)
    for r in rows:
        if r.get("invalid"):
            print(f"(invalid message on {r['subject']})")
            continue
        print(f"{r['timestamp']}  {r['type']:<8} {r['from']:<22} -> {r['to']:<22} task {r['task_id']}  "
              f"{tools._note(r)[:70]}")


def cmd_mcp(args):
    from .mcp_server import run
    run(_cfg(args), _me(args))


# --------------------------------------------------------------------------- agent-node commands


def node_init(args):
    path = find_config(args.config).resolve()
    if path.exists() and not args.force:
        raise SystemExit(f"{path} exists (use --force to overwrite)")
    data = {
        "project": args.project,
        "node": args.node,
        "description": args.description or "",
        "data_dir": args.data_dir or str(Path(path).parent / "data"),
        "nats": {"servers": [args.server]},
        "resources": {},
        "agents": [] if args.bare else [
            {"id": "main", "display": f"{args.node}:a1", "mode": "interactive", "role": "lead",
             "provider": "anthropic", "workdir": str(Path.cwd()),
             "permissions": ["READ", "REQUEST_TASK", "PUBLISH_ARTIFACT"]},
            {"id": "worker", "display": f"{args.node}:b1", "mode": "worker", "runtime": "claude-code",
             "provider": "anthropic", "role": "general-worker", "workdir": str(Path.cwd()),
             "capabilities": [], "accept_from": ["*"],
             "permissions": ["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"]},
        ],
    }
    if args.credentials:
        data["nats"]["credentials_file"] = str(Path(args.credentials).expanduser().resolve())
    if args.ca:
        data["nats"]["tls_ca"] = str(Path(args.ca).expanduser().resolve())
    dump_config(data, path)
    nxt = "agent-node add-agent ..." if args.bare else "edit the agents section"
    print(f"wrote {path}\nnext: {nxt}, then agent-node doctor && agent-node start")


def node_add_agent(args):
    """Add one agent to an existing node config, idempotently (several assistants can share one node)."""
    path = find_config(args.config).resolve()
    data = yaml.safe_load(path.read_text()) or {}
    agents = data.setdefault("agents", []) or []
    data["agents"] = agents
    existing = next((a for a in agents if a.get("id") == args.id), None)
    if existing and not args.replace:
        print(f"{data.get('node')}:{args.id} already configured in {path}; unchanged (use --replace to overwrite)")
        return
    agent: dict[str, Any] = {"id": args.id, "mode": args.mode}
    for key in ("runtime", "role", "provider", "model", "display", "workdir", "repo"):
        if getattr(args, key) is not None:
            agent[key] = getattr(args, key)
    for key, values in (("capabilities", args.capability), ("permissions", args.permission),
                        ("accept_from", args.accept_from), ("notify", args.notify)):
        if values:
            agent[key] = values
    if existing:
        agents[agents.index(existing)] = agent
    else:
        agents.append(agent)
    backup = path.read_text()
    dump_config(data, path)
    try:
        load_config(path)                      # never leave an invalid config behind
    except (ConfigError, ValueError) as e:
        path.write_text(backup)
        raise SystemExit(f"error: {e}; {path} left unchanged")
    print(f"{'replaced' if existing else 'added'} {data.get('node')}:{args.id} in {path}; restart the node to apply")


def node_join(args):
    path = find_config(args.config)
    data = yaml.safe_load(path.read_text())
    nats = data.setdefault("nats", {})
    if args.server:
        nats["servers"] = [args.server]
    if args.credentials:
        nats["credentials_file"] = str(Path(args.credentials).expanduser().resolve())
    if args.project:
        data["project"] = args.project
    if args.node:
        data["node"] = args.node
    dump_config(data, path)
    cfg = load_config(path)

    async def check():
        bus = await Bus.open(cfg.nats, cfg.project, f"mutmuas:{cfg.node}:join", reconnect=False)
        nodes = await bus.kv_keys(bus.names.nodes_kv)
        await bus.close()
        return nodes

    try:
        nodes = asyncio.run(check())
    except BusUnavailable as e:
        raise SystemExit(f"config saved, but the server is not reachable: {e}")
    print(f"joined project {cfg.project} as node {cfg.node}; nodes already registered: {nodes or 'none'}")
    print("start the daemon with: agent-node start   (or install it as a service: agent-node service)")


def node_start(args):
    cfg = _cfg(args)
    cfg.data_path.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    handlers.append(logging.FileHandler(cfg.data_path / "node.log"))
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), handlers=handlers,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("nats").setLevel(logging.WARNING)
    from .node import NodeDaemon

    async def main():
        daemon = NodeDaemon(cfg)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await daemon.start()
        await stop.wait()
        await daemon.stop()

    asyncio.run(main())


def node_server_config(args):
    from .server_config import generate
    nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
    tls_hosts = [h.strip() for h in args.tls.split(",") if h.strip()] if args.tls else None
    written = generate(args.project, nodes, Path(args.out), tls_hosts=tls_hosts, port=args.port,
                       store_dir=args.store_dir, listen_host=args.listen)
    for name, path in written.items():
        print(f"{name:>8}: {path}")
    print("copy <NODE>.env to each node and reference it as nats.credentials_file")
    if tls_hosts:
        print("copy tls/ca.crt (public, not secret) to each node and set nats.tls_ca; "
              f"connect with nats://{tls_hosts[0]}:{args.port}")


def node_service(args):
    """Print (or write) a launchd plist / systemd unit that runs `agent-node start`."""
    cfg_path = find_config(args.config).resolve()
    cfg = load_config(cfg_path)
    venv_bin = Path(sys.executable).parent
    exe = venv_bin / "agent-node"
    argv = ([str(exe)] if exe.exists() else [sys.executable, "-m", "mutmuas.cli", "node"]) + [
        "start", "--config", str(cfg_path)]
    suffix, what = "", "agent-node"
    if args.watch:          # the desktop notifier of one interactive agent, instead of the node daemon
        watched = Address.parse(args.watch if ":" in args.watch else f"{cfg.node}:{args.watch}")
        if watched.node != cfg.node:
            raise SystemExit(f"error: {watched} is not on node {cfg.node}")
        cfg.agent(watched.agent)                                    # must be configured on this node
        ctl = venv_bin / "agentctl"
        argv = ([str(ctl)] if ctl.exists() else [sys.executable, "-m", "mutmuas.cli"]) + [
            "watch", "--config", str(cfg_path), "--as", str(watched)]
        suffix, what = f".watch-{watched.agent}", f"notifier for {watched}"
    # A minimal PATH: the venv, wherever the agent CLIs live, and system dirs. Copying the caller's PATH
    # would leak e.g. an activated conda env into every worker.
    dirs = [str(venv_bin)] + [str(Path(p).parent) for p in (shutil.which("claude"), shutil.which("codex")) if p]
    dirs += ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"]
    path_env = ":".join(dict.fromkeys(d for d in dirs if Path(d).is_dir()))
    if sys.platform == "darwin":
        label = f"dev.mutmuas.{cfg.project}.{cfg.node}{suffix}"   # unique per node: several nodes can share a Mac
        items = "\n".join(f"    <string>{a}</string>" for a in argv)
        log = cfg_path.parent / f"{cfg.node}{suffix or '-agent-node'}.launchd.log"
        text = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{items}
  </array>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>{path_env}</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"""
        target = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
        hint = f"launchctl bootstrap gui/$(id -u) {target}\nlaunchctl kickstart -k gui/$(id -u)/{label}"
    else:
        nats_unit = Path.home() / ".config/systemd/user/mutmuas-nats-server.service"
        local_server = any(h in srv for srv in cfg.nats.servers for h in ("127.0.0.1", "localhost", "[::1]"))
        order = ("After=mutmuas-nats-server.service\nWants=mutmuas-nats-server.service\n"
                 if local_server and (nats_unit.exists() or args.after_nats) else "")
        text = f"""[Unit]
Description=mutmuas {what} ({cfg_path})
{order}
[Service]
ExecStart={' '.join(argv)}
Restart=always
RestartSec=5
Environment=PATH={path_env}
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=default.target
"""
        # Linux keeps one unit name for now (node B's rollout depends on it); the guard below still
        # refuses to overwrite a unit that was written for a different config.
        unit = "mutmuas-agent-node" + (f"-{cfg.node}{suffix}" if suffix else "")
        target = Path.home() / f".config/systemd/user/{unit}.service"
        hint = (f"systemctl --user daemon-reload\nsystemctl --user enable --now {unit}\n"
                "loginctl enable-linger $USER   # keep running after logout")
    if args.write:
        if target.exists() and cfg_path.as_posix() not in target.read_text() and not args.force:
            raise SystemExit(f"error: {target} exists and belongs to another config; use --force to replace it")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        print(f"wrote {target}\nthen run:\n{hint}")
    else:
        print(text)
        print(f"# save to {target} (or re-run with --write), then:\n# " + hint.replace("\n", "\n# "))


def node_retire(args):
    """Take a whole node out of the network: remove its agents' cards, its node card and empty mailboxes."""
    cfg = _cfg(args)

    async def run():
        bus = await Bus.open(cfg.nats, cfg.project, f"mutmuas:{cfg.node}:retire", reconnect=False)
        try:
            node = await bus.kv_get(bus.names.nodes_kv, cfg.node)
            if node and is_online(node) and not args.force:
                raise SystemExit(f"error: node {cfg.node} is still online; stop its daemon first (or --force)")
            keys = await bus.kv_keys(bus.names.agents_kv, [f"{cfg.node}.*"])
            for key in keys:
                agent = Address(cfg.node, key.split(".", 1)[1])
                print(f"{agent}: {await bus.remove_agent(agent, force=args.force)}")
            await bus.kv_delete(bus.names.nodes_kv, cfg.node)
            print(f"node {cfg.node}: card removed. Ask the server admin to remove user node_{cfg.node} "
                  "(server-config without this node, then reload) so its credentials stop working.")
        finally:
            await bus.close()

    asyncio.run(run())


def node_doctor(args):
    cfg = _cfg(args)
    print(f"config    {cfg.path}\nproject   {cfg.project}\nnode      {cfg.node}\ndata      {cfg.data_path}")
    for agent in cfg.agents:
        tool = {"claude-code": "claude", "codex": "codex"}.get(agent.runtime or "")
        found = shutil.which(tool) if tool else "-"
        print(f"agent     {cfg.node}:{agent.id}  mode={agent.mode} runtime={agent.runtime} "
              f"cli={'ok' if found else 'MISSING ' + str(tool)}  workdir={agent.workdir_path}")
        if agent.runtime == "codex" and sys.platform.startswith("linux"):
            ok = subprocess.run(["unshare", "-Ur", "true"], capture_output=True).returncode == 0
            if not ok:
                print("          WARNING: unprivileged user namespaces are blocked (unshare -Ur fails), so Codex's "
                      "bubblewrap sandbox cannot run shell commands. Run Codex workers on macOS, or ask root "
                      "for an AppArmor profile for bwrap (see docs/DEPLOYMENT.md)")

    async def check():
        hub = await Hub.open(cfg, "doctor", reconnect=False)
        try:
            nodes = await hub.nodes()
            print(f"nats      ok ({', '.join(cfg.nats.servers)})")
            for n in nodes:
                print(f"  node {n['node']:<10} {'online' if is_online(n) else 'offline'}  {_ago(n.get('last_heartbeat'))}")
            print(f"outbox    {hub.ledger.count('out', 'queued')} message(s) waiting")
        finally:
            await hub.close()

    try:
        asyncio.run(check())
    except BusUnavailable as e:
        raise SystemExit(f"nats      UNREACHABLE: {e}")


# --------------------------------------------------------------------------- parsers


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", help="node config (default: $MUTMUAS_CONFIG or ~/.mutmuas/node.yaml)")
    p.add_argument("--as", dest="as_agent", help="act as this local agent (default: $MUTMUAS_AGENT)")
    p.add_argument("--json", action="store_true", help="machine-readable output")


def agentctl_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentctl", description="mutmuas: talk to agents on other machines")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add(name, fn, help, bus=True):
        p = sub.add_parser(name, help=help)
        _common(p)
        p.set_defaults(fn=fn, bus=bus)
        return p

    add("status", cmd_status, "nodes, agents, presence, current tasks")
    p = add("agents", cmd_agents, "list agents")
    p.add_argument("--capability")
    p.add_argument("--online", action="store_true")
    p = add("find", cmd_find, "best agent for a capability/role")
    p.add_argument("capability")
    p = add("ask", cmd_ask, "send a REQUEST quickly")
    p.add_argument("to")
    p.add_argument("objective")
    p.add_argument("--reason")
    p.add_argument("--kind", default="query", choices=["query", "artifact", "experiment", "code"])
    p.add_argument("--input", action="append", help="key=value (value may be JSON)")
    p.add_argument("--expect", action="append", help="expected output (repeatable)")
    p.add_argument("--accept", action="append", help="acceptance criterion (repeatable)")
    p.add_argument("--constraint", action="append", help="constraint on how to do it (repeatable)")
    p.add_argument("--priority", default="normal", choices=["low", "normal", "high"])
    p.add_argument("--timeout", type=float, help="task timeout on the owner side (s)")
    p.add_argument("--wait", type=float, nargs="?", const=600, help="wait for the result (s)")
    p = add("send", cmd_send, "send a message from a YAML file", bus=False)
    p.add_argument("to")
    p.add_argument("--type", default="REQUEST")
    p.add_argument("--file")
    p.add_argument("--task")
    p.add_argument("--objective")
    p = add("tasks", cmd_tasks, "tasks on this node (--all: every node)", bus=False)
    p.add_argument("--all", action="store_true")
    p.add_argument("--limit", type=int, default=50)
    p = add("task", cmd_task, "one task with its full message history", bus=False)
    p.add_argument("task_id")
    p = add("result", cmd_result, "result of a task", bus=False)
    p.add_argument("task_id")
    p.add_argument("--wait", type=float, nargs="?", const=600)
    p.add_argument("--fetch", metavar="DIR", help="download result artifacts into DIR")
    p = add("inbox", cmd_inbox, "messages addressed to me", bus=False)
    p.add_argument("--all", action="store_true", help="include already seen messages")
    p.add_argument("--peek", action="store_true", help="do not mark messages as read")
    p.add_argument("--since", metavar="ISO_TIME", help="only messages received after this time (notifier cursor; "
                                                         "each row's 'received_at' is the next cursor)")
    p.add_argument("--only", metavar="TYPES",
                   help="'wake' (someone needs me to act: no ACKs, progress or RESULTs; best for a session "
                        "watcher), 'actionable' (also RESULTs), or comma separated types, e.g. REQUEST,QUESTION")
    p.add_argument("--wait", type=float, nargs="?", const=3600, metavar="SECONDS",
                   help="block until a message arrives (default up to 3600 s); exit code 3 on timeout")
    p = add("watch", cmd_watch, "run forever: desktop notification per new actionable message (launchd/systemd)")
    p.add_argument("--interval", type=float, default=3600, help="max seconds per wait cycle")
    p.add_argument("--dry-run", action="store_true", help="log notifications instead of showing them")
    p = add("cancel", cmd_cancel, "cancel a task I requested", bus=False)
    p.add_argument("task_id")
    p.add_argument("--reason")
    p = add("accept", cmd_accept, "accept a task sent to me", bus=False)
    p.add_argument("task_id")
    p = add("reject", cmd_reject, "reject a task sent to me", bus=False)
    p.add_argument("task_id")
    p.add_argument("reason")
    p = add("update", cmd_update, "progress update on a task I own", bus=False)
    p.add_argument("message")
    p.add_argument("--task")
    p.add_argument("--state", choices=["RUNNING", "WAITING", "BLOCKED"])
    p = add("submit-result", cmd_submit, "finish a task I own", bus=False)
    p.add_argument("--task")
    p.add_argument("--status", choices=["complete", "partial", "failed"])
    p.add_argument("--summary")
    p.add_argument("--artifact", action="append", help="artifact URI (repeatable)")
    p.add_argument("--file", help="YAML with status/summary/outputs/artifacts/evidence/limitations/follow_up")
    p = add("question", cmd_question, "ask the other side of a task", bus=False)
    p.add_argument("task_id")
    p.add_argument("text")
    p = add("answer", cmd_answer, "answer a question on a task", bus=False)
    p.add_argument("task_id")
    p.add_argument("text")
    p = add("artifact", cmd_artifact, "publish | fetch | list artifacts")
    p.add_argument("action", choices=["publish", "fetch", "list"])
    p.add_argument("target", nargs="?", help="path (publish) or URI (fetch)")
    p.add_argument("--key")
    p.add_argument("--description", default="")
    p.add_argument("--backend", default="object", choices=["object", "file"])
    p.add_argument("--task", help="file the artifact under this task id")
    p.add_argument("--dest")
    p = add("history", cmd_history, "raw audit trail from the message stream")
    p.add_argument("--task")
    p.add_argument("--limit", type=int, default=500)
    p = sub.add_parser("mcp", help="run the MCP server (stdio) for Claude Code / Codex")
    _common(p)
    p.set_defaults(fn=None, sync=cmd_mcp)
    return parser


def agent_node_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-node", description="mutmuas node daemon and setup")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init", help="create a node config")
    p.add_argument("--config")
    p.add_argument("--project", required=True)
    p.add_argument("--node", required=True)
    p.add_argument("--server", default="nats://127.0.0.1:4222")
    p.add_argument("--credentials", help="<NODE>.env file from server-config")
    p.add_argument("--data-dir")
    p.add_argument("--description")
    p.add_argument("--force", action="store_true")
    p.add_argument("--ca", help="server CA certificate (TLS)")
    p.add_argument("--bare", action="store_true", help="no agents yet (add them with add-agent)")
    p.set_defaults(sync=node_init)
    p = sub.add_parser("add-agent", help="add an agent to this node's config (idempotent)")
    p.add_argument("--config")
    p.add_argument("--id", required=True)
    p.add_argument("--mode", default="interactive", choices=["interactive", "worker"])
    p.add_argument("--runtime", choices=["claude-code", "codex", "script"])
    p.add_argument("--role")
    p.add_argument("--provider")
    p.add_argument("--model")
    p.add_argument("--display")
    p.add_argument("--workdir")
    p.add_argument("--repo")
    p.add_argument("--capability", action="append")
    p.add_argument("--permission", action="append")
    p.add_argument("--accept-from", dest="accept_from", action="append")
    p.add_argument("--notify", action="append")
    p.add_argument("--replace", action="store_true")
    p.set_defaults(sync=node_add_agent)
    p = sub.add_parser("join", help="point this node at a server and verify connectivity")
    p.add_argument("--config")
    p.add_argument("--server")
    p.add_argument("--credentials")
    p.add_argument("--project")
    p.add_argument("--node")
    p.set_defaults(sync=node_join)
    p = sub.add_parser("start", help="run the daemon in the foreground")
    p.add_argument("--config")
    p.add_argument("--log-level", default="info")
    p.set_defaults(sync=node_start)
    p = sub.add_parser("server-config", help="generate nats-server.conf + per-node credentials")
    p.add_argument("--project", required=True)
    p.add_argument("--nodes", required=True, help="comma separated node ids, e.g. A,B")
    p.add_argument("--out", default="./server")
    p.add_argument("--port", type=int, default=4222)
    p.add_argument("--listen", default="0.0.0.0")
    p.add_argument("--store-dir", default="/data/jetstream")
    p.add_argument("--tls", metavar="HOSTS",
                   help="enable TLS; comma separated IPs/DNS names clients use to reach the server "
                        "(e.g. 150.89.170.193). Generates a private CA + server cert")
    p.set_defaults(sync=node_server_config)
    p = sub.add_parser("service", help="launchd (macOS) / systemd (Linux) unit for the daemon")
    p.add_argument("--config")
    p.add_argument("--write", action="store_true", help="write the unit file instead of printing it")
    p.add_argument("--force", action="store_true", help="replace a unit file written for a different config")
    p.add_argument("--watch", metavar="AGENT", help="unit for the desktop notifier of this interactive agent "
                                                    "(agentctl watch) instead of the node daemon")
    p.add_argument("--after-nats", action="store_true",
                   help="Linux: order after mutmuas-nats-server.service (automatic when that unit exists "
                        "and the node connects to 127.0.0.1)")
    p.set_defaults(sync=node_service)
    p = sub.add_parser("retire", help="take this node out of the network (cards + empty mailboxes)")
    p.add_argument("--config")
    p.add_argument("--force", action="store_true", help="also while online / drop waiting messages")
    p.set_defaults(sync=node_retire)
    p = sub.add_parser("doctor", help="check config, CLIs and connectivity")
    p.add_argument("--config")
    p.set_defaults(sync=node_doctor)
    return parser


def _run(parser: argparse.ArgumentParser, argv: list[str] | None) -> None:
    args = parser.parse_args(argv)
    try:
        if getattr(args, "sync", None):
            return args.sync(args)
        asyncio.run(_with_hub(args, lambda hub: args.fn(args, hub), require_bus=args.bus))
    except (ConfigError, BusUnavailable, PermissionError, ProtocolError, KeyError, ValueError) as e:
        raise SystemExit(f"error: {e}")


def agentctl(argv: list[str] | None = None) -> None:
    _run(agentctl_parser(), argv)


def agent_node(argv: list[str] | None = None) -> None:
    _run(agent_node_parser(), argv)


if __name__ == "__main__":   # python -m mutmuas.cli [node] ...
    if len(sys.argv) > 1 and sys.argv[1] == "node":
        agent_node(sys.argv[2:])
    else:
        agentctl()

