"""Retire a post (D-085/D-089): `agent-node retire-agent <id>` takes an address out of node.yaml, takes its card
offline, gives its open work back (or names who takes over), withdraws what it asked others for, moves its post
directory to work/_archive/ untouched, and can be undone."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml
from conftest import Orphan

from mutmuas import runtime as runtime_module
from mutmuas.config import load_config
from mutmuas.ledger import Ledger
from mutmuas.node import proc_start
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
    assert "/work/_archive/vision-" in plan["archived_to"]
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
    assert [a.id for a in load_config(path).agents] == ["lead", "vision", "plain"]      # back where it was
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


SMALL = """project: p
node: C
data_dir: ./data
agents:
- id: lead
  mode: interactive
  workdir: {lead}
- id: vision
  mode: interactive
{comment}  workdir: ./work/vision
{trailing}"""


def _small(tmp_path, lead="./work/lead", comment="", trailing=""):
    path = tmp_path / "node.yaml"
    path.write_text(SMALL.format(lead=lead, comment=comment, trailing=trailing))
    (tmp_path / "work" / "vision").mkdir(parents=True)
    (tmp_path / "work" / "vision" / "leader-note").write_text("keep me")
    return path, tmp_path / "work" / "vision"


class FakeBus:
    """remove_agent as the real Bus does it, over a fake card store and a mailbox with `pending` unread."""
    def __init__(self, pending=0, fail=False):
        from types import SimpleNamespace
        self.pending, self.fail, self.events = pending, fail, []
        self.names = SimpleNamespace(agents_kv="cards", stream="mail", consumer=str)
        self.js = self

    async def kv_delete(self, *args):
        self.events.append("card deleted")

    async def inbox_pending(self, _addr):
        return self.pending

    async def delete_consumer(self, *args):
        if self.fail:
            raise OSError("synthetic mailbox deletion failure")      # after node.yaml changed
        self.events.append("mailbox deleted")

    async def close(self):
        pass

    from mutmuas.bus import Bus
    remove_agent = Bus.remove_agent


@pytest.fixture
def bus(monkeypatch):
    from mutmuas.hub import Hub
    made = {"bus": None}

    async def fake_open(cfg, *_args, **_kwargs):
        return Hub(cfg, made["bus"], Ledger(cfg.db_path))
    monkeypatch.setattr("mutmuas.retire.Hub.open", fake_open)
    return made


async def test_a_comment_inside_the_block_stays_out_of_the_neighbour(tmp_path, bus):
    path, post = _small(tmp_path, comment="# ordinary comment within the vision item\n")
    (tmp_path / "work" / "lead").mkdir()
    await retire(path, "vision")
    [lead] = load_config(path).agents
    assert lead.id == "lead" and lead.workdir_path == (tmp_path / "work" / "lead").resolve()
    assert (tmp_path / "work" / "lead").is_dir() and "workdir: ./work/vision" not in path.read_text()


async def test_undo_puts_the_block_back_inside_agents_with_keys_after_it(tmp_path, bus):
    path, post = _small(tmp_path, trailing="heartbeat_s: 30\n")
    done = await retire(path, "vision")
    await undo(done["manifest"])
    cfg = load_config(path)
    assert [a.id for a in cfg.agents] == ["lead", "vision"] and cfg.heartbeat_s == 30


async def test_retiring_is_refused_while_the_node_daemon_runs(tmp_path, bus):
    from mutmuas.node import daemon_lock
    path, post = _small(tmp_path)
    with daemon_lock(load_config(path)):
        plan = await retire(path, "vision", dry_run=True)
        assert "stop it" in plan["daemon"]
        with pytest.raises(PermissionError, match="daemon"):
            await retire(path, "vision")
    assert "id: vision" in path.read_text() and post.is_dir()


async def test_undo_does_not_move_the_archive_over_a_directory_made_again(tmp_path, bus):
    path, post = _small(tmp_path)
    done = await retire(path, "vision")
    post.mkdir()
    (post / "new-note").write_text("new files stay")
    back = await undo(done["manifest"])
    assert "not moved" in back["directory"] and "id: vision" in path.read_text()
    assert (post / "new-note").exists() and not (post / "leader-note").exists()
    assert (Path(done["archived_to"]) / "leader-note").exists()


async def test_a_mailbox_with_unread_mail_is_not_dropped_silently(tmp_path, bus):
    path, post = _small(tmp_path)
    bus["bus"] = FakeBus(pending=3)
    with pytest.raises(PermissionError, match="3 unread"):
        await retire(path, "vision")
    assert "id: vision" in path.read_text() and bus["bus"].events == []
    done = await retire(path, "vision", keep_mailbox=True)
    assert bus["bus"].events == ["card deleted"]
    assert any("3 unread" in t for t in done["todo"])
    bus["bus"] = FakeBus()
    (tmp_path / "again").mkdir()
    path2, _ = _small(tmp_path / "again")
    done = await retire(path2, "vision")
    assert bus["bus"].events == ["card deleted", "mailbox deleted"] and not done["todo"]


def test_undo_with_dry_run_changes_nothing(tmp_path, bus, capsys):
    import asyncio
    from mutmuas import cli
    path, post = _small(tmp_path)
    done = asyncio.run(retire(path, "vision"))
    cli.agent_node(["retire-agent", "--undo", done["manifest"], "--dry-run"])
    assert "id: vision" not in path.read_text() and not post.exists()
    assert json.loads(capsys.readouterr().out)["dry_run"]


async def test_undo_is_refused_while_the_node_daemon_runs(tmp_path, bus):
    from mutmuas.node import daemon_lock
    path, post = _small(tmp_path)
    done = await retire(path, "vision")
    with daemon_lock(load_config(path)):
        with pytest.raises(PermissionError, match="daemon"):
            await undo(done["manifest"])
    assert "id: vision" not in path.read_text() and not post.exists()


async def test_a_daemon_that_fails_to_start_lets_go_of_its_lock(tmp_path):
    from mutmuas.node import NodeDaemon, daemon_lock
    path, post = _small(tmp_path)
    cfg = load_config(path)
    daemon = NodeDaemon(cfg)

    async def broken_connect():
        raise RuntimeError("synthetic startup failure")
    daemon._connect = broken_connect
    with pytest.raises(RuntimeError):
        await daemon.run_forever()
    with daemon_lock(cfg):
        pass


async def test_a_stop_cancelled_while_going_offline_still_lets_go_of_the_lock(tmp_path):
    import asyncio
    from mutmuas.hub import Hub
    from mutmuas.node import NodeDaemon, daemon_lock
    path, post = _small(tmp_path)
    cfg = load_config(path)
    daemon = NodeDaemon(cfg)
    lock = daemon_lock(cfg)
    lock.__enter__()
    daemon._daemon_lock = lock
    daemon.hub = Hub(cfg, None, Ledger(cfg.db_path))
    publishing = asyncio.Event()

    async def slow_offline(**_kwargs):
        publishing.set()
        await asyncio.Event().wait()                    # the network is slow
    daemon._publish_cards = slow_offline
    stopping = asyncio.create_task(daemon.stop())
    await asyncio.wait_for(publishing.wait(), 3)
    stopping.cancel()                                   # stop itself is cancelled (a second ctrl-c, a timeout)
    await asyncio.gather(stopping, return_exceptions=True)
    with daemon_lock(cfg):
        pass


async def test_retire_waits_for_a_stuck_process_group_of_a_finished_task(node, monkeypatch):  # noqa: F811
    path, cfg, ledger, post = node
    ledger.update_task("T-open", "owner", status="CANCELLED", stuck_pgid=12345)
    monkeypatch.setattr(runtime_module, "group_alive", lambda pgid: pgid == 12345)
    with pytest.raises(PermissionError, match="T-open"):
        await retire(path, "vision")
    assert "id: vision" in path.read_text() and post.is_dir()


async def test_retire_waits_for_a_running_plain_worker(node):  # noqa: F811
    path, cfg, ledger, post = node
    run = Envelope(type="REQUEST", sender="C:lead", to="C:plain", task_id="T-run", body=request_body("run", "test"))
    ledger.ingest(run)
    ledger.create_owned_task(run)
    ledger.update_task("T-run", "owner", status="RUNNING")
    ledger.set_runner_pid("T-run", os.getpid(), proc_start(os.getpid()))   # mode: worker: no runner claim
    with pytest.raises(PermissionError, match="worker is running"):
        await retire(path, "plain")
