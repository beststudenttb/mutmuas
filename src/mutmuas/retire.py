"""Retire a post (D-085/D-089): `agent-node retire-agent <id>`.

Takes one address out of this node: its node.yaml block (the rest of the file is kept as written), its registry
card and mailbox, its open work (given back to each requester, naming who takes over), what it asked others for
(withdrawn; their nodes cascade further down), and its post directory (moved whole to work/_archive/, nothing in it
deleted: it may hold the leader's files too).

It runs only with the node daemon stopped, holding the daemon's lock to the end, and only while the post's session
is offline and no worker of it runs: nothing else acts for the post meanwhile (D-102: that is the guard; a person
starting a session or editing files in the middle is not guarded against). A manifest (RETIRED-<id>-<time>.json
next to node.yaml) holds the whole plan before the first change; `--undo <manifest>` puts the block back into the
agents list and the directory back where it was. Rejected and withdrawn tasks stay so: those messages have gone
out."""

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
from .node import daemon_lock, live_worker_runs, session_present
from .protocol import OPEN_STATES
from .runtime import group_alive
from .tools import cancel_task


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


@contextlib.contextmanager
def _node_stopped(cfg, dry_run: bool):
    """Hold the daemon's lock (the daemon cannot start meanwhile); a dry run only says when it is running."""
    with contextlib.ExitStack() as lock:
        try:
            lock.enter_context(daemon_lock(cfg))
            running = None
        except BlockingIOError:
            if not dry_run:
                raise PermissionError(f"node {cfg.node}'s daemon is running: stop it, run this, then start it "
                                      "again") from None
            running = f"node {cfg.node}'s daemon is running: stop it before the real run"
        yield running


async def retire(config: Path | str, agent_id: str, hand_over: str | None = None, dry_run: bool = False,
                 keep_mailbox: bool = False) -> dict[str, Any]:
    path = Path(config).resolve()
    cfg = load_config(path)
    agent = next((a for a in cfg.agents if a.id == agent_id), None)
    if agent is None:
        raise KeyError(f"{agent_id}: no such agent on node {cfg.node}")
    with _node_stopped(cfg, dry_run) as running:
        plan = await _retire(path, cfg, agent, hand_over, dry_run, keep_mailbox)
        if running:
            plan["daemon"] = running
        return plan


async def _retire(path, cfg, agent, hand_over, dry_run, keep_mailbox) -> dict[str, Any]:
    addr = str(Address(cfg.node, agent.id))
    # one try at the bus: with it the card goes offline now, without it at the node's next start
    hub = await Hub.open(cfg, "cli", require_bus=False, reconnect=False, initial_connect_attempts=1)
    try:
        ledger = hub.ledger
        if (why := session_present(ledger, addr)):
            raise PermissionError(f"{addr}: {why}; close its session first")
        if live_worker_runs(ledger, addr):
            raise PermissionError(f"{addr}: its worker is running a task; wait for it or cancel it first")
        # a process group a stop could not end runs on whatever its task's state, also after the daemon stopped
        if stuck := [t["task_id"] for t in ledger.tasks(role="owner", local_agent=addr, limit=None)
                     if t.get("stuck_pgid") and group_alive(t["stuck_pgid"])]:
            raise PermissionError(f"{addr}: a process group a stop could not end still runs for {', '.join(stuck)}; "
                                  "it must end first")
        pending = await hub.bus.inbox_pending(Address.parse(addr)) if hub.bus else None
        if pending and not keep_mailbox:         # unread mail is not thrown away
            raise PermissionError(f"{addr}: {pending} unread message(s) wait in its mailbox; have them read (start "
                                  "the node and let its session or worker take them), or pass --keep-mailbox to "
                                  "keep the mailbox for whoever takes over")
        text = path.read_text()
        start, end, index = config_block(text, agent.id)
        lines = text.split("\n")
        post = agent.workdir_path
        shared = [a.id for a in cfg.agents if a.id != agent.id and (
            a.workdir_path == post or post in a.workdir_path.parents or a.workdir_path in post.parents)]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        archive_to = None if shared or not post.exists() else post.parent / "_archive" / f"{post.name}-{stamp}"
        plan: dict[str, Any] = {
            "agent": addr, "config": str(path), "block": "\n".join(lines[start:end]), "index": index,
            "rejected": [t["task_id"] for t in ledger.tasks(role="owner", local_agent=addr, statuses=OPEN_STATES,
                                                           limit=None)],
            "cancelled": [t["task_id"] for t in ledger.tasks(role="requester", local_agent=addr,
                                                            statuses=OPEN_STATES, limit=None)],
            "workdir": str(post), "archived_to": str(archive_to) if archive_to else None,
            "mailbox_unread": pending, "todo": []}
        if shared:
            plan["note"] = f"post directory shared with {', '.join(shared)}: not moved"
        if dry_run:
            return plan
        manifest = path.with_name(f"RETIRED-{agent.id}-{stamp}.json")
        manifest.write_text(json.dumps(plan, indent=2, ensure_ascii=False))     # the whole plan, before any change
        backup = path.with_name(f"{path.name}.bak-{stamp}-retire-{agent.id}")
        shutil.copy2(path, backup)
        _write_atomic(path, "\n".join(lines[:start] + lines[end:]))
        load_config(path)
        who = f"; ask {hand_over} instead" if hand_over else "; ask the secretary who takes over"
        reason = f"{addr} has been retired (its post is closed){who}"
        for task_id in plan["rejected"]:
            await hub.owner_transition(task_id, "FAILED", reason, msg_type="REJECT",
                                       body={"reason": reason, **({"hand_over": hand_over} if hand_over else {})})
        for task_id in plan["cancelled"]:
            await cancel_task(hub, addr, task_id, f"{addr} has been retired")
        if hub.bus:
            plan["card"] = await hub.bus.remove_agent(Address.parse(addr))
            if "mailbox kept" in plan["card"]:
                plan["todo"].append(f"{addr}'s mailbox was kept with {pending} unread message(s): nobody reads it "
                                    "now; whoever takes over reads them, and the leader decides when it is deleted")
        else:
            plan["card"] = "no bus"
            plan["todo"].append(f"{addr}'s card and mailbox: not reached (no bus); the node removes the card at its "
                                "next start, and the mailbox once nothing waits in it")
        if archive_to:
            archive_to.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(post), str(archive_to))
        plan.update(config_backup=str(backup),
                    note=plan.get("note") or "start the node again: it forgets the address")
        manifest.write_text(json.dumps(plan, indent=2, ensure_ascii=False))
        return {**plan, "manifest": str(manifest)}
    finally:
        await hub.close()


async def undo(manifest: Path | str, dry_run: bool = False) -> dict[str, Any]:
    """Put a retired post back from its manifest, with the node daemon stopped (as retiring): its block into the
    agents list where it was, unless an agent of that id is configured; its directory back from the archive,
    unless something is in its place. What is on disk decides, so a run that stopped midway is undone too.
    Messages that went out (rejections, withdrawals) cannot be taken back."""
    done = json.loads(Path(manifest).read_text())
    path = Path(done["config"])
    with _node_stopped(load_config(path), dry_run) as running:
        agent_id = done["agent"].split(":", 1)[1]
        text = path.read_text()
        out: dict[str, Any] = {"agent": done["agent"], "dry_run": dry_run}
        try:
            config_block(text, agent_id)
            out["config"] = "already configured"
            candidate = None
        except KeyError:
            candidate = _insert(text, done)
            out["config"] = f"block goes back as item {done['index']} of agents"
        archive, workdir = done.get("archived_to"), Path(done["workdir"])
        move = bool(archive) and Path(archive).exists() and not workdir.exists()
        if move:
            out["directory"] = f"{archive} -> {workdir}"
        elif archive:
            out["directory"] = (f"not moved: the archive is {'there' if Path(archive).exists() else 'gone'}, "
                                f"{workdir} {'exists' if workdir.exists() else 'is missing'}")
        if running:
            out["daemon"] = running
        if dry_run:
            return out
        if candidate is not None:
            _write_atomic(path, candidate)
            load_config(path)
        if move:
            shutil.move(archive, str(workdir))
        out["note"] = "start the node to bring it back online; rejected tasks stay rejected and withdrawn ones withdrawn"
        return out


def _insert(text: str, done: dict[str, Any]) -> str:
    """node.yaml with the retired block back at its old place in the agents list (after the item before it)."""
    seq = _agents_node(text)
    if not seq.value:
        raise ValueError("the agents list is empty: put the block back by hand (it is in the manifest)")
    lines = text.split("\n")
    index = min(done["index"], len(seq.value))
    at = _item_lines(text, seq.value[index - 1])[1] if index else _item_lines(text, seq.value[0])[0]
    return "\n".join(lines[:at] + done["block"].split("\n") + lines[at:])
