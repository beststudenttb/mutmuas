"""Attaching artifacts to a request (regression found when batch 1 went live on 2026-09-28): since visibility
step 1 only an ArtifactRef attached to a message grants its recipient access, not a URI in the text, so
`agentctl ask` must be able to attach one. A sender may attach only artifacts it can see itself."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import eventually, interactive
from mutmuas import tools
from mutmuas.visibility import artifact_visible


def _agents():
    perms = ["READ", "PUBLISH_ARTIFACT", "REQUEST_TASK"]
    return interactive("main", permissions=perms), interactive("other", permissions=perms)


async def test_ask_with_artifact_lets_the_recipient_fetch_it(make_config, cluster, tmp_path):
    a = make_config("A", list(_agents()))
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    src = tmp_path / "review.md"
    src.write_text("findings")
    ref = await tools.publish_artifact(hub_a, "A:main", str(src), key="A/main/adhoc/review.md")
    out = subprocess.run([str(Path(sys.executable).parent / "agentctl"), "ask", "B:desk", "please read",
                          "--artifact", ref["uri"], "--json", "--config", str(a.path), "--as", "A:main"],
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    task_id = json.loads(out.stdout)["task_id"]
    await eventually(lambda: hub_b.ledger.task(task_id, "owner"), what="request arrived")
    assert artifact_visible(hub_b.ledger, "B:desk", ref["uri"])
    got = await tools.fetch_artifact(hub_b, ref["uri"], dest_dir=str(tmp_path / "in"), me="B:desk")
    assert Path(got["path"]).read_text() == "findings"


async def test_a_sender_cannot_attach_an_artifact_it_cannot_see(make_config, cluster, tmp_path):
    a = make_config("A", list(_agents()))
    b = make_config("B", [interactive("desk")])
    await cluster.start(a)
    await cluster.start(b)
    hub_a = await cluster.client(a)
    src = tmp_path / "private.md"
    src.write_text("secret")
    ref = await tools.publish_artifact(hub_a, "A:other", str(src), key="A/other/adhoc/private.md")
    with pytest.raises(PermissionError):
        await tools.send_request(hub_a, "A:main", "B:desk", "look", "x", artifacts=[{"uri": ref["uri"]}])
