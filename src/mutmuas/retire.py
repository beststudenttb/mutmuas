"""Retire a post (D-085/D-089): `agent-node retire-agent <id>`.

Takes one address out of this node: its node.yaml block (the rest of the file is kept as written), its registry
card and mailbox, its open work (given back to each requester, naming who takes over), what it asked others for
(withdrawn; their nodes cascade further down), and its post directory (moved whole to work/_archive/, nothing in it
deleted: it may hold the leader's files too).

Only with the node daemon stopped (its lock is held throughout, so it cannot start meanwhile), and refused while the
post's session is online or a worker of it runs; the session check and a fence that keeps any session from taking
the post afterwards are one transaction. A manifest (RETIRED-<id>-<time>.json next to node.yaml) is written before
the first change and after each step, so a run that fails midway can be undone too: `--undo <manifest>` puts the
block back into the agents list and the directory back where it was, and lifts the fence. Rejected and withdrawn
tasks stay so: those messages have gone out."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .config import load_config
from .hub import Hub
from .ids import Address
from .ledger import Ledger
from .protocol import OPEN_STATES


def _agents_node(text: str) -> yaml.SequenceNode:
    root = yaml.compose(text)
    for key, value in (root.value if isinstance(root, yaml.MappingNode) else []):
        if key.value == "agents" and isinstance(value, yaml.SequenceNode):
            return value
    raise ValueError("node.yaml has no agents list")


def _content_end(node: yaml.Node) -> int:
    """The line after an item's last content: the end of its last scalar or flow value (a block's own end mark
    reaches past the comments that follow it, which belong to what comes next)."""
    while isinstance(node, (yaml.MappingNode, yaml.SequenceNode)) and not node.flow_style and node.value:
        node = node.value[-1][1] if isinstance(node, yaml.MappingNode) else node.value[-1]
    mark = node.end_mark
    return mark.line if mark.column == 0 else mark.line + 1


def _item_lines(text: str, item: yaml.Node) -> tuple[int, int]:
    lines = text.split("\n")
    start = item.start_mark.line
    while start > 0 and not lines[start].lstrip().startswith("-"):
        start -= 1                                    # "-" alone on its line, the item below it
    return start, _content_end(item)


def config_block(text: str, agent_id: str) -> tuple[int, int, int]:
    """(start, end, index): the lines [start, end) of an agent's item under `agents:` and its place in the list,
    found from the parsed YAML's own positions, whatever its keys, style or comments. A comment inside the item
    is part of it; comments after its last value are left for what follows."""
    seq = _agents_node(text)
    for index, item in enumerate(seq.value):
        if isinstance(item, yaml.MappingNode) and any(k.value == "id" and str(v.value) == agent_id
                                                      for k, v in item.value if isinstance(v, yaml.ScalarNode)):
            start, end = _item_lines(text, item)
            return start, end, index
    raise KeyError(f"agent {agent_id!r} is not in this node.yaml")


def _check_only(before: str, after: str, agents_after: list[Any]) -> None:
    """The candidate node.yaml changes the agents list to exactly agents_after and nothing else."""
    old, new = yaml.safe_load(before) or {}, yaml.safe_load(after) or {}
    if {k: v for k, v in old.items() if k != "agents"} != {k: v for k, v in new.items() if k != "agents"} \
            or new.get("agents") != agents_after:
        raise ValueError("the edited node.yaml would change more than this one agent; nothing changed")


def _write_atomic(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        shutil.copymode(path, tmp)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _within(inner: Path, outer: Path) -> bool:
    """inner is outer or lies under it, by file identity where both exist (a case variant or a link of a directory is
    that directory), by case-folded path where not (cautious: a case-sensitive disk may count two as one)."""
    for p in (inner, *inner.parents):
        if p.exists() and outer.exists():
            if os.path.samefile(p, outer):
                return True
        elif str(p).casefold() == str(outer).casefold():
            return True
    return False


class _Manifest:
    """RETIRED-<id>-<time>.json, rewritten (atomically) at every step."""

    def __init__(self, path: Path, record: dict[str, Any]):
        self.path, self.record = path, record
        self.save()

    def step(self, name: str, **info: Any) -> None:
        self.record["steps"].append(name)
        self.record.update(info)
        self.save()

    def save(self) -> None:
        tmp = self.path.with_name(f".{self.path.name}.tmp")
        tmp.write_text(json.dumps(self.record, indent=2, ensure_ascii=False))
        os.replace(tmp, self.path)


async def retire(config: Path | str, agent_id: str, hand_over: str | None = None, dry_run: bool = False,
                 keep_mailbox: bool = False) -> dict[str, Any]:
    from .node import daemon_lock
    path = Path(config).resolve()
    cfg = load_config(path)
    agent = next((a for a in cfg.agents if a.id == agent_id), None)
    if agent is None:
        raise KeyError(f"{agent_id}: no such agent on node {cfg.node}")
    with contextlib.ExitStack() as lock:
        try:
            lock.enter_context(daemon_lock(cfg))       # held to the end: the daemon cannot start meanwhile
            running = False
        except BlockingIOError:
            running = True
        if running and not dry_run:
            raise PermissionError(f"node {cfg.node}'s daemon is running (it would take new work for the post "
                                  "meanwhile): stop it, retire, then start it again")
        plan = await _retire(path, cfg, agent, hand_over, dry_run, keep_mailbox)
        if running:
            plan["daemon"] = f"node {cfg.node}'s daemon is running: stop it before -y, start it again afterwards"
        return plan


async def _retire(path, cfg, agent, hand_over, dry_run, keep_mailbox) -> dict[str, Any]:
    from .node import live_worker_runs, session_present
    from .tools import cancel_task
    agent_id = agent.id
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
        start, end, index = config_block(text, agent_id)
        lines = text.split("\n")
        block = "\n".join(lines[start:end])
        candidate = "\n".join(lines[:start] + lines[end:])
        agents_before = (yaml.safe_load(text) or {})["agents"]
        _check_only(text, candidate, agents_before[:index] + agents_before[index + 1:])
        post = agent.workdir_path
        shared = [a.id for a in cfg.agents if a.id != agent_id
                  and (_within(a.workdir_path, post) or _within(post, a.workdir_path))]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        archive_to = None if shared or not post.exists() else post.parent / "_archive" / f"{post.name}-{stamp}"
        pending = await hub.bus.inbox_pending(Address.parse(addr)) if hub.bus else None
        plan: dict[str, Any] = {"agent": addr, "config": str(path), "block": block, "index": index,
                                "entry": agents_before[index], "rejected": owned, "cancelled": asked,
                                "workdir": str(post), "archive_to": str(archive_to) if archive_to else None,
                                "mailbox_unread": pending, "todo": []}
        if shared:
            plan["note"] = f"post directory shared with {', '.join(shared)}: not moved"
        if pending and not keep_mailbox:
            raise PermissionError(f"{addr}: {pending} unread message(s) wait in its mailbox; have them read (start "
                                  "the node and let its session or worker take them), or pass --keep-mailbox to "
                                  "keep the mailbox for whoever takes over")
        if dry_run:
            return plan
        # 0 the fence, in one transaction with the session check: from here no session takes the post
        if (why := ledger.begin_retire(addr, lambda: session_present(ledger, addr))):
            raise PermissionError(f"{addr}: {why}; close its session first")
        manifest = _Manifest(path.with_name(f"RETIRED-{agent_id}-{stamp}.json"),
                             {**plan, "at": stamp, "state": "started", "steps": ["fence"], "archived_to": None})
        try:
            # 1 node.yaml: a timestamped backup, then only this block out (checked above), written atomically
            backup = path.with_name(f"{path.name}.bak-{stamp}-retire-{agent_id}")
            shutil.copy2(path, backup)
            _write_atomic(path, candidate)
            load_config(path)
            manifest.step("config", config_backup=str(backup))
            # 2 its open work goes back to each requester; what it asked for is withdrawn (cascades downstream)
            who = f"; ask {hand_over} instead" if hand_over else "; ask the secretary who takes over"
            for task_id in owned:
                reason = f"{addr} has been retired (its post is closed){who}"
                await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT",
                                           body={"reason": reason, **({"hand_over": hand_over} if hand_over else {})})
                manifest.step(f"rejected:{task_id}")
            for task_id in asked:
                await cancel_task(hub, addr, task_id, f"{addr} has been retired")
                manifest.step(f"cancelled:{task_id}")
            # 3 the card and the mailbox (one with unread mail only with --keep-mailbox: then it is kept)
            todo = manifest.record["todo"]
            if hub.bus:
                card = await hub.bus.remove_agent(Address.parse(addr))
                if "mailbox kept" in card:
                    todo.append(f"{addr}'s mailbox was kept with {pending} unread message(s): nobody reads it now; "
                                "whoever takes over reads them, and the leader decides when it is deleted")
            else:
                card = "no bus"
                todo.append(f"{addr}'s card and mailbox: not reached (no bus); the node removes the card at its next "
                            "start, and the mailbox once nothing waits in it")
            manifest.step("card", card=card)
            # 4 the post directory is moved whole
            if archive_to:
                archive_to.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(post), str(archive_to))
                manifest.step("archived", archived_to=str(archive_to))
            manifest.record.setdefault("note", "start the node again: it forgets the address")
            manifest.record["state"] = "done"
            manifest.step("done")
        except BaseException as e:
            manifest.record.update(state="failed", error=f"{type(e).__name__}: {e}")
            manifest.save()
            raise
        return {**manifest.record, "manifest": str(manifest.path)}
    finally:
        await hub.close()


async def undo(manifest: Path | str, dry_run: bool = False) -> dict[str, Any]:
    """Put a retired post back from its manifest, also one of a run that failed midway: its block into the agents
    list where it was, its directory where it was, and the fence lifted. Everything is checked before anything
    changes: a directory that is there again (both are named) or a block that would not fit changes nothing.
    Messages that went out (rejections, withdrawals) cannot be taken back."""
    done = json.loads(Path(manifest).read_text())
    path = Path(done["config"])
    agent_id = done["agent"].split(":", 1)[1]
    steps = done.get("steps") or []
    out: dict[str, Any] = {"agent": done["agent"], "dry_run": dry_run, "config": None, "directory": None}
    text = path.read_text()
    candidate = None
    try:
        config_block(text, agent_id)
        out["config"] = "already configured"
    except KeyError:
        if "config" in steps:
            candidate = _insert(text, done)
            out["config"] = f"block goes back as item {done['index']} of agents"
    archived = done.get("archived_to")
    if archived and "archived" in steps:
        if Path(done["workdir"]).exists():
            raise FileExistsError(f"{done['workdir']} exists again: the archive {archived} is not moved back over "
                                  "it (nothing changed); merge the two by hand")
        if not Path(archived).exists():
            raise FileNotFoundError(f"the archive {archived} is gone (nothing changed)")
        out["directory"] = f"{archived} -> {done['workdir']}"
    if dry_run:
        return out
    if candidate is not None:
        _write_atomic(path, candidate)
        load_config(path)
    if out["directory"]:
        shutil.move(archived, done["workdir"])
    ledger = Ledger(load_config(path).db_path)
    try:
        ledger.end_retire(done["agent"])
    finally:
        ledger.close()
    out.update(restored=True, note="restart the node to bring it back online; rejected tasks stay rejected and "
                                   "withdrawn ones withdrawn")
    return out


def _insert(text: str, done: dict[str, Any]) -> str:
    """node.yaml with the retired block back at its old place in the agents list (after the item before it), checked
    to change nothing but that."""
    seq = _agents_node(text)
    if not seq.value:
        raise ValueError("the agents list is empty: put the block back by hand (it is in the manifest)")
    lines = text.split("\n")
    index = min(done["index"], len(seq.value))
    at = _item_lines(text, seq.value[index - 1])[1] if index else _item_lines(text, seq.value[0])[0]
    candidate = "\n".join(lines[:at] + done["block"].split("\n") + lines[at:])
    agents = (yaml.safe_load(text) or {})["agents"]
    _check_only(text, candidate, agents[:index] + [done["entry"]] + agents[index:])
    return candidate
