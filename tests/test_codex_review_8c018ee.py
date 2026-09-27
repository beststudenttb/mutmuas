"""Remaining visibility/lease regressions found while reviewing ``8c018ee``."""

from __future__ import annotations

import argparse
import asyncio

import pytest

from conftest import eventually, interactive
from mutmuas import cli, tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon
from mutmuas.protocol import ArtifactRef, Envelope
from mutmuas.visibility import artifact_visible, is_participant


def _local_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[AgentConfig(id=agent_id, mode="interactive") for agent_id in agent_ids],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def _request(task_id: str = "T-review") -> Envelope:
    return Envelope(
        type="REQUEST",
        sender="A:main",
        to="B:desk",
        task_id=task_id,
        body={"objective": "review", "reason": "regression test"},
    )


async def test_unknown_observer_copy_is_retried_when_task_record_arrives(tmp_path, monkeypatch):
    """A normal REQUEST/copy/KV race must not strand the observer copy forever."""
    _, ledger, hub, daemon = _local_stack(tmp_path, "peer")
    hub.bus = object()  # the verifier only needs to know that a shared bus exists
    copy = Envelope(
        type="UPDATE",
        sender="A:main",
        to="A:peer",
        task_id="T-late-record",
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:main", "A:peer", "B:desk"],
            "copy_of": _request("T-late-record").to_dict(),
        },
    )
    ledger.ingest(copy)
    calls = 0

    async def task_record_after_first_lookup(task_id, owner):
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        return {"task_id": task_id, "requester": "A:main", "owner": "B:desk"}

    monkeypatch.setattr(hub, "_remote_task", task_record_after_first_lookup)
    try:
        assert daemon._on_observer_copy(copy) == "unverified"
        await asyncio.sleep(0.2)

        assert calls >= 2
        assert is_participant(ledger, "A:peer", copy.task_id)
    finally:
        ledger.close()


async def test_observer_can_add_observer_on_a_new_node(make_config, cluster):
    """The protocol allows any participant, including an observer, to add another observer."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("desk")])
    c = make_config("C", [interactive("other")])
    d = make_config("D", [interactive("peer")])
    for cfg in (a, b, c, d):
        await cluster.start(cfg)
    hub_a, hub_b, hub_c, hub_d = [await cluster.client(cfg) for cfg in (a, b, c, d)]

    sent = await tools.send_request(hub_a, "A:main", "B:desk", "review", "observer chain")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner task")
    await tools.add_observer(hub_b, "B:desk", sent["task_id"], "C:other")
    await eventually(
        lambda: hub_c.ledger.task(sent["task_id"], "observer:C:other"),
        what="first observer copy",
    )

    await tools.add_observer(hub_c, "C:other", sent["task_id"], "D:peer")
    await asyncio.sleep(1)

    assert hub_d.ledger.task(sent["task_id"], "observer:D:peer") is not None


def test_observer_copy_still_requires_recipient_in_participant_list(tmp_path):
    """The documented copy check covers both its sender and its recipient."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main", "peer")
    request = _request("T-copy-recipient")
    ledger.queue_outgoing(request)
    malformed = Envelope(
        type="UPDATE",
        sender="B:desk",
        to="A:peer",
        task_id=request.task_id,
        body={
            "message": "observer copy",
            "fyi": True,
            "participants": ["A:main", "B:desk"],
            "copy_of": request.to_dict(),
        },
    )
    try:
        assert daemon._on_observer_copy(malformed) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


def test_sending_an_artifact_reference_does_not_grant_fetch_access(tmp_path):
    """Artifact access comes from publishing or receiving the exact URI, not sending it."""
    _, ledger, _, _ = _local_stack(tmp_path, "main")
    uri = "artifact://testproj/C/other/T-private/report.txt"
    try:
        ledger.queue_outgoing(
            Envelope(
                type="UPDATE",
                sender="A:main",
                to="B:desk",
                task_id="T-private",
                body={"message": "a URI I do not own"},
                artifacts=[ArtifactRef(uri=uri)],
            )
        )

        assert not artifact_visible(ledger, "A:main", uri)
    finally:
        ledger.close()


async def test_headers_only_watch_does_not_make_body_eligible_for_clear(tmp_path, monkeypatch):
    """Seeing only a notification header is not seeing the message content."""
    cfg, ledger, hub, _ = _local_stack(tmp_path, "main")
    request = Envelope(
        type="REQUEST",
        sender="B:desk",
        to="A:main",
        task_id="T-header-only",
        body={"objective": "body never shown", "reason": "private body"},
    )
    ledger.ingest(request)
    ledger.mark_handled(request.message_id)
    (cfg.data_path / "A_main.notify-cursor").write_text("0")

    class WatchStopped(Exception):
        pass

    def stop_after_notification(*args, **kwargs):
        raise WatchStopped

    monkeypatch.setattr(cli, "_desktop_notify", stop_after_notification)
    args = argparse.Namespace(
        as_agent="A:main", interval=0, headers_only=True, dry_run=True
    )
    try:
        with pytest.raises(WatchStopped):
            await cli.cmd_watch(args, hub)

        cleared = await tools.clear_inbox(hub, "A:main", ledger.last_rowid())
        assert cleared["marked_read"] == 0
        assert ledger.unseen_count("A:main") == 1
    finally:
        ledger.close()


def test_agentctl_whoami_is_not_lease_free():
    """The protocol exempts MCP whoami, but not an arbitrary agentctl process."""
    assert "cmd_whoami" not in cli.LEASE_FREE
