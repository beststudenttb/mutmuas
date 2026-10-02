"""Retire a post (D-085/D-089): `agent-node retire-agent <id>` takes an address out of node.yaml, takes its card
offline, gives its open work back (or names who takes over), withdraws what it asked others for, moves its post
directory to work/_archive/ untouched, and can be undone."""

from __future__ import annotations

import json

import pytest
import yaml
from conftest import Orphan

from mutmuas.config import load_config
from mutmuas.ledger import Ledger
from mutmuas.protocol import Envelope, request_body
from mutmuas.retire import retire, undo

CONFIG = """# node C: issued by the secretary
project: p
node: C
data_dir: ./data
agents:
- id: lead
  mode: interactive
  workdir: ../work/lead
  permissions: [READ, REQUEST_TASK]
# the trackbot vision post (leader's comment, kept)
- id: vision
  mode: interactive
  auto_worker: true
  runtime: script
  command: ["true"]
  workdir: ../work/vision
  permissions: [READ, REQUEST_TASK, WRITE_WORKTREE]
- {id: plain, mode: worker, runtime: script, command: ["true"], workdir: ../work/plain}
"""


@pytest.fixture
def node(tmp_path):
    (tmp_path / "node").mkdir()
    path = tmp_path / "node" / "C.yaml"
    path.write_text(CONFIG)
    post = tmp_path / "work" / "vision"
    (post / "trackbot").mkdir(parents=True)
    (post / "HANDOFF.md").write_text("vision handoff\n")
    (post / "leader-notes.md").write_text("the leader's own file\n")
    (post / "trackbot" / "PLAN.md").write_text("# PLAN\n")
    cfg = load_config(path)
    ledger = Ledger(cfg.db_path)
    owned = Envelope(type="REQUEST", sender="A:lead", to="C:vision", task_id="T-open",
                     body=request_body("label the images", "trackbot"))
    ledger.ingest(owned)
    ledger.create_owned_task(owned)
    asked = Envelope(type="REQUEST", sender="C:vision", to="B:rl", task_id="T-asked",
                     body=request_body("train it", "trackbot"))
    ledger.queue_outgoing(asked)
    yield path, cfg, ledger, post
    ledger.close()


async def test_a_dry_run_shows_the_plan_and_changes_nothing(node):
    path, cfg, ledger, post = node
    plan = await retire(path, "vision", dry_run=True)
    assert plan["rejected"] == ["T-open"] and plan["cancelled"] == ["T-asked"]
    assert "/work/_archive/vision-" in plan["archive_to"]
    assert path.read_text() == CONFIG and post.is_dir()
    assert ledger.task("T-open", "owner")["status"] == "PENDING"


async def test_retiring_takes_the_post_out_and_keeps_everything_else(node):
    path, cfg, ledger, post = node
    done = await retire(path, "vision", hand_over="C:lead")
    text = path.read_text()
    assert "id: vision" not in text and "# node C: issued by the secretary" in text
    assert "id: lead" in text and "{id: plain" in text and "leader's comment, kept" in text
    assert [a.id for a in load_config(path).agents] == ["lead", "plain"]
    assert open(done["config_backup"]).read() == CONFIG
    # its open work goes back, naming who takes over; what it asked for is withdrawn
    assert ledger.task("T-open", "owner")["status"] == "FAILED"
    [reject] = [e for e in ledger.outbox() if e.type == "REJECT" and e.task_id == "T-open"]
    assert "retired" in reject.body["reason"] and "C:lead" in reject.body["reason"]
    assert ledger.task("T-asked", "requester")["status"] == "CANCELLED"
    assert [e.to for e in ledger.outbox() if e.type == "CANCEL"] == ["B:rl"]
    # the post directory is moved whole, nothing in it deleted
    archived = done["archived_to"]
    assert not post.exists()
    assert open(f"{archived}/leader-notes.md").read() == "the leader's own file\n"
    assert open(f"{archived}/trackbot/PLAN.md").read() == "# PLAN\n"
    manifest = json.load(open(done["manifest"]))
    assert manifest["agent"] == "C:vision" and manifest["rejected"] == ["T-open"]


async def test_a_post_whose_session_is_online_is_not_retired(node):
    path, cfg, ledger, post = node
    session = Orphan("import time; time.sleep(60)")
    try:
        ledger.session_beat("C:vision", session.pid, str(post), session_pid=session.pid)
        with pytest.raises(PermissionError, match="session"):
            await retire(path, "vision")
        assert path.read_text() == CONFIG and post.is_dir()
    finally:
        session.kill()


async def test_a_post_whose_worker_runs_is_not_retired(node):
    path, cfg, ledger, post = node
    worker = Orphan("import time; time.sleep(60)")
    try:
        from mutmuas.node import proc_start
        ledger.update_task("T-open", "owner", status="ACCEPTED")
        assert ledger.claim_task("T-open", "worker", ("ACCEPTED",)) is None
        ledger.set_runner_pid("T-open", worker.pid, proc_start(worker.pid))
        with pytest.raises(PermissionError, match="worker"):
            await retire(path, "vision")
    finally:
        worker.kill()


async def test_retiring_is_undone_from_its_manifest(node):
    path, cfg, ledger, post = node
    done = await retire(path, "vision")
    back = await undo(done["manifest"])
    assert [a.id for a in load_config(path).agents] == ["lead", "plain", "vision"]
    assert post.is_dir() and (post / "leader-notes.md").read_text() == "the leader's own file\n"
    assert "rejected tasks stay rejected" in back["note"]


async def test_a_directory_another_post_uses_is_not_moved(tmp_path, node):
    path, cfg, ledger, post = node
    data = yaml.safe_load(path.read_text())
    data["agents"][0]["workdir"] = "../work/vision"            # lead shares it
    path.write_text(yaml.safe_dump(data))
    done = await retire(path, "vision")
    assert post.is_dir() and done["archived_to"] is None and "shared" in done["note"]


async def test_an_unknown_post_is_refused(node):
    path, *_ = node
    with pytest.raises(KeyError, match="nobody"):
        await retire(path, "nobody")


def test_agent_node_retire_agent_shows_the_plan_until_told_yes(node, capsys):
    from mutmuas import cli
    path, cfg, ledger, post = node
    cli.agent_node(["retire-agent", "vision", "--config", str(path)])
    assert "run again with -y" in capsys.readouterr().err and path.read_text() == CONFIG
    cli.agent_node(["retire-agent", "vision", "--config", str(path), "--hand-over", "C:lead", "-y"])
    out = json.loads(capsys.readouterr().out)
    assert out["rejected"] == ["T-open"] and "id: vision" not in path.read_text()
    cli.agent_node(["retire-agent", "--undo", out["manifest"]])
    assert "id: vision" in path.read_text() and post.is_dir()
