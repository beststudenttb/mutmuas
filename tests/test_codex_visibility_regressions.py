"""Security regressions found during the independent review of ``dfdd719``.

These tests describe the intended visibility and single-session boundaries.  They
are deliberately small and do not require a running NATS server.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys

from mutmuas import cli, tools
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger
from mutmuas.node import NodeDaemon, lease_refusal
from mutmuas.protocol import ArtifactRef, Envelope
from mutmuas.visibility import artifact_visible, is_participant


def _local_stack(tmp_path, *agent_ids: str):
    cfg = NodeConfig(
        project="testproj",
        node="A",
        data_dir=str(tmp_path / "data"),
        agents=[
            AgentConfig(
                id=agent_id,
                mode="interactive",
                permissions=["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"],
            )
            for agent_id in agent_ids
        ],
    ).validate()
    ledger = Ledger(cfg.db_path)
    hub = Hub(cfg, None, ledger)
    daemon = NodeDaemon(cfg)
    daemon.hub = hub
    return cfg, ledger, hub, daemon


def _requested_task(ledger: Ledger, task_id: str) -> Envelope:
    request = Envelope(
        type="REQUEST",
        sender="A:main",
        to="B:desk",
        task_id=task_id,
        body={"objective": "review", "reason": "regression test"},
    )
    ledger.queue_outgoing(request)
    return request


def test_observer_copy_requires_sender_in_persisted_acl(tmp_path):
    """A sender cannot make itself authoritative with its own participant list."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main", "peer")
    try:
        request = _requested_task(ledger, "T-observer-acl")
        forged_copy = Envelope(
            type="UPDATE",
            sender="C:other",
            to="A:peer",
            task_id=request.task_id,
            body={
                "message": "observer copy",
                "fyi": True,
                "copy_of": request.to_dict(),
                "participants": ["A:main", "A:peer", "B:desk", "C:other"],
            },
        )

        assert asyncio.run(daemon._on_observer_copy(forged_copy)) == "rejected"
        assert not is_participant(ledger, "A:peer", request.task_id)
    finally:
        ledger.close()


async def test_result_requires_actual_task_owner(tmp_path):
    """Mail about a task is not a RESULT unless it came from the persisted owner."""
    _, ledger, _, daemon = _local_stack(tmp_path, "main")
    try:
        request = _requested_task(ledger, "T-result-owner")
        forged_result = Envelope(
            type="RESULT",
            sender="C:other",
            to="A:main",
            task_id=request.task_id,
            body={"status": "complete", "summary": "not produced by the owner"},
        )

        await daemon._on_reply(forged_result)

        task = ledger.task(request.task_id, "requester")
        assert task["status"] == "PENDING"
        assert task["result"] is None
    finally:
        ledger.close()


def test_artifact_visibility_treats_uri_literally(tmp_path):
    """SQL wildcard characters in an artifact URI must have no special meaning."""
    _, ledger, _, _ = _local_stack(tmp_path, "main")
    try:
        delivered = "artifact://testproj/B/desk/T-artifact/fooXbar"
        requested = "artifact://testproj/B/desk/T-artifact/foo_bar"
        ledger.ingest(
            Envelope(
                type="UPDATE",
                sender="B:desk",
                to="A:main",
                task_id="T-artifact",
                body={"message": "artifact ready"},
                artifacts=[ArtifactRef(uri=delivered)],
            )
        )

        assert not artifact_visible(ledger, "A:main", requested)
    finally:
        ledger.close()


def test_lease_ancestry_does_not_trust_path_ps(tmp_path, monkeypatch):
    """The lease check must not derive process ancestry from a PATH-controlled tool."""
    _, ledger, _, _ = _local_stack(tmp_path, "main")
    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        ledger.session_beat("A:main", holder.pid, "/holder", session_pid=holder.pid)
        fake_ps = tmp_path / "ps"
        fake_ps.write_text(f"#!/bin/sh\nprintf '%s\\n' {holder.pid}\n")
        fake_ps.chmod(0o755)
        monkeypatch.setenv("PATH", str(tmp_path))
        monkeypatch.delenv("MUTMUAS_TASK_ID", raising=False)

        assert lease_refusal(ledger, "A:main") is not None
    finally:
        holder.terminate()
        holder.wait(timeout=5)
        ledger.close()


def test_watch_is_not_lease_free_because_it_reads_mail_content():
    """A second session must not use watch to inspect the live holder's mail."""
    assert "cmd_watch" not in cli.LEASE_FREE


async def test_default_object_key_does_not_disclose_source_filename(tmp_path, monkeypatch):
    """Shared object metadata must not expose a local source filename by default."""
    _, ledger, hub, _ = _local_stack(tmp_path, "main")
    source = tmp_path / "layoff-plan-secret.txt"
    source.write_text("content")
    captured = {}

    async def capture_publish(src, key, **kwargs):
        captured["key"] = key
        return ArtifactRef(uri=f"artifact://testproj/{key}")

    monkeypatch.setattr(hub.artifacts, "publish", capture_publish)
    try:
        await tools.publish_artifact(hub, "A:main", str(source), task_id="T-object-key")

        assert source.name not in captured["key"]
    finally:
        ledger.close()
