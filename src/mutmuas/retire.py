"""Retire a post (D-085/D-089): `agent-node retire-agent <id>`.

Takes one address out of this node: its node.yaml block (the rest of the file is kept as written), its registry
card, its open work (given back to each requester, naming who takes over), what it asked others for (withdrawn;
their nodes cascade further down), and its post directory (moved whole to work/_archive/, nothing in it deleted:
it may hold the leader's files too). Refused while its session is online or its worker runs. Everything done is
written to a manifest in the archive; `--undo <manifest>` puts the block and the directory back. Rejected and
withdrawn tasks stay so: those messages have gone out."""

from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .config import ConfigError, load_config
from .hub import Hub
from .ids import Address
from .protocol import OPEN_STATES

MANIFEST = "RETIRED.json"


def config_block(text: str, agent_id: str) -> tuple[int, int]:
    """The lines [start, end) of an agent's list item under `agents:` in node.yaml, whatever order its keys are
    in: the n-th item (n from parsing the file) up to the next line indented no deeper than the item's dash
    (the next item, a comment at that level, or the next top-level key). Blank lines before that stay outside."""
    agents = (yaml.safe_load(text) or {}).get("agents") or []
    index = next((i for i, a in enumerate(agents) if isinstance(a, dict) and str(a.get("id")) == agent_id), None)
    if index is None:
        raise KeyError(f"agent {agent_id!r} is not in this node.yaml")
    lines = text.split("\n")
    top = next(i for i, line in enumerate(lines) if re.match(r"^agents:\s*(#.*)?$", line))
    items = [i for i in range(top + 1, len(lines)) if re.match(r"^\s*-(\s|$)", lines[i])
             and not lines[i].startswith((" " * 8, "\t"))]
    indent = len(lines[items[0]]) - len(lines[items[0]].lstrip())
    items = [i for i in items if len(lines[i]) - len(lines[i].lstrip()) == indent]
    start = items[index]
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip()) > indent):
        end += 1
    while end > start + 1 and not lines[end - 1].strip():
        end -= 1
    return start, end


async def retire(config: Path | str, agent_id: str, hand_over: str | None = None,
                 dry_run: bool = False) -> dict[str, Any]:
    from .node import live_worker_runs, session_present
    from .tools import cancel_task
    path = Path(config).resolve()
    cfg = load_config(path)
    agent = next((a for a in cfg.agents if a.id == agent_id), None)
    if agent is None:
        raise KeyError(f"{agent_id}: no such agent on node {cfg.node}")
    addr = str(Address(cfg.node, agent_id))
    # one try at the bus: with it the card goes offline now, without it at the node's next start
    hub = await Hub.open(cfg, "cli", require_bus=False, reconnect=False, initial_connect_attempts=1)
    try:
        ledger = hub.ledger
        if (why := session_present(ledger, addr)):
            raise PermissionError(f"{addr}: {why}; close its session first")
        if live_worker_runs(ledger, addr):
            raise PermissionError(f"{addr}: its worker is running a task; wait for it or cancel it first")
        owned = [t["task_id"] for t in ledger.tasks(role="owner", local_agent=addr, statuses=OPEN_STATES, limit=None)]
        asked = [t["task_id"] for t in ledger.tasks(role="requester", local_agent=addr, statuses=OPEN_STATES,
                                                   limit=None)]
        text = path.read_text()
        start, end = config_block(text, agent_id)
        block = "\n".join(text.split("\n")[start:end])
        post = agent.workdir_path
        shared = [a.id for a in cfg.agents if a.id != agent_id
                  and (a.workdir_path == post or post in a.workdir_path.parents or a.workdir_path in post.parents)]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        archive_to = None if shared or not post.exists() else post.parent / "_archive" / f"{post.name}-{stamp}"
        plan: dict[str, Any] = {"agent": addr, "config": str(path), "block": block, "rejected": owned,
                                "cancelled": asked, "workdir": str(post),
                                "archive_to": str(archive_to) if archive_to else None}
        if shared:
            plan["note"] = f"post directory shared with {', '.join(shared)}: not moved"
        if dry_run:
            return plan
        # 1 node.yaml: a timestamped backup, then only this block out; the result must still load
        backup = path.with_name(f"{path.name}.bak-{stamp}-retire-{agent_id}")
        shutil.copy2(path, backup)
        lines = text.split("\n")
        path.write_text("\n".join(lines[:start] + lines[end:]))
        try:
            load_config(path)
        except (ConfigError, ValueError, TypeError) as e:
            shutil.copy2(backup, path)
            raise SystemExit(f"error: node.yaml would not load without {agent_id} ({e}); left unchanged")
        # 2 its open work goes back to each requester; what it asked for is withdrawn (cascades downstream)
        who = f"; ask {hand_over} instead" if hand_over else "; ask the secretary who takes over"
        for task_id in owned:
            reason = f"{addr} has been retired (its post is closed){who}"
            await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT",
                                       body={"reason": reason, **({"hand_over": hand_over} if hand_over else {})})
        for task_id in asked:
            await cancel_task(hub, addr, task_id, f"{addr} has been retired")
        # 3 the card goes offline (the daemon also forgets it at its next start)
        plan["card"] = await hub.bus.remove_agent(Address.parse(addr)) if hub.bus else "no bus: removed at restart"
        # 4 the post directory is moved whole
        if archive_to:
            archive_to.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(post), str(archive_to))
        manifest_dir = archive_to or path.parent
        manifest = manifest_dir / (MANIFEST if archive_to else f"{MANIFEST[:-5]}-{agent_id}-{stamp}.json")
        plan.update(config_backup=str(backup), archived_to=str(archive_to) if archive_to else None,
                    manifest=str(manifest), at=stamp)
        plan.setdefault("note", "restart the node to stop its loops")
        manifest.write_text(json.dumps(plan, indent=2, ensure_ascii=False))
        return plan
    finally:
        await hub.close()


async def undo(manifest: Path | str) -> dict[str, Any]:
    """Put a retired post back: its node.yaml block (appended if absent) and its post directory. Messages that went
    out (rejections, withdrawals) cannot be taken back."""
    done = json.loads(Path(manifest).read_text())
    path = Path(done["config"])
    agent_id = done["agent"].split(":", 1)[1]
    text = path.read_text()
    try:
        config_block(text, agent_id)
    except KeyError:
        path.write_text(text.rstrip("\n") + "\n" + done["block"] + "\n")
        load_config(path)
    if done.get("archived_to") and not Path(done["workdir"]).exists():
        shutil.move(done["archived_to"], done["workdir"])
    return {"agent": done["agent"], "restored": True,
            "note": "restart the node to bring it back online; rejected tasks stay rejected and withdrawn ones "
                    "withdrawn"}
