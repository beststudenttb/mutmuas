"""D-052: the node, not the model, does the mechanical part of delivery. When an owned task gets its RESULT it
appends the R7.11 line to the post's worker-log.md (how and notes from submit_result, '未填' when missing) and
attaches this task's PLAN.md section (the heading with the task id) to the result, then takes it off the board.

Arriving work is written into the post's plan and the sender is told its place in the queue; every
run keeps its own log."""

from __future__ import annotations

import os
import sys

import pytest
from conftest import Orphan, auto_worker_node, owned_task

from mutmuas import cli, tools
from mutmuas.node import proc_start
from mutmuas.protocol import Envelope, request_body, result_body
from mutmuas.runtime import ScriptRuntime, TaskContext


PLAN = """# T-other: another task
- [x] something -> out.txt

## T-d: count the lines
- [x] read CLAUDE.md
- [x] count -> 13

# T-later
- [ ] not started
"""


def _post(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    agent.workdir_path.mkdir(parents=True, exist_ok=True)
    return agent.workdir_path, ledger, hub, daemon


def _log_lines(workdir):
    path = workdir / "worker-log.md"
    return path.read_text().splitlines() if path.exists() else []


async def test_delivery_writes_the_log_line_and_hands_over_the_tasks_plan(tmp_path):
    workdir, ledger, hub, _ = _post(tmp_path)
    owned_task(ledger, "T-d", "RUNNING", ingest=True)
    (workdir / "PLAN.md").write_text(PLAN)
    try:
        await tools.submit_result(hub, "B:desk", "complete", "CLAUDE.md has 13 lines", task_id="T-d",
                                  how="read it and counted | twice", notes="the file ends without a newline")
        result = ledger.task("T-d", "owner")["result"]
        assert result["how"] == "read it and counted | twice"
        assert result["outputs"]["plan"] == "## T-d: count the lines\n- [x] read CLAUDE.md\n- [x] count -> 13\n"
        board = (workdir / "PLAN.md").read_text()
        assert "T-d" not in board and "# T-other" in board and "# T-later" in board
        [line] = _log_lines(workdir)
        fields = line.split(" | ")
        assert len(fields) == 6 and fields[1] == "A:sender" and fields[2].startswith("T-d")
        assert fields[3].startswith("complete: CLAUDE.md has 13 lines") and "A:sender" in fields[3]
        assert fields[4] == "read it and counted / twice" and fields[5] == "the file ends without a newline"
    finally:
        ledger.close()


async def test_nothing_filled_and_no_board_still_gives_one_line(tmp_path):
    workdir, ledger, hub, _ = _post(tmp_path)
    owned_task(ledger, "T-d", "RUNNING", ingest=True)
    try:
        await tools.submit_result(hub, "B:desk", "failed", "could not\nfinish", task_id="T-d")
        result = ledger.task("T-d", "owner")["result"]
        assert "plan" not in (result.get("outputs") or {}) and not (workdir / "PLAN.md").exists()
        [line] = _log_lines(workdir)
        fields = line.split(" | ")
        assert fields[3].startswith("failed: could not finish") and fields[4:] == ["未填", "未填"]
    finally:
        ledger.close()


async def test_a_read_receipt_is_not_logged(tmp_path):
    workdir, ledger, hub, _ = _post(tmp_path)
    env = Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id="T-n",
                   body=request_body("fyi", "test", reply="none"))
    ledger.ingest(env)
    ledger.create_owned_task(env)
    ledger.mark_handled(env.message_id)
    try:
        await tools.inbox(hub, "B:desk")                      # reading it closes it with a read receipt
        assert ledger.task("T-n", "owner")["status"] == "COMPLETED" and _log_lines(workdir) == []
    finally:
        ledger.close()


async def test_a_workers_how_and_notes_reach_the_line_through_its_draft(tmp_path, monkeypatch):
    workdir, ledger, hub, daemon = _post(tmp_path)
    owned_task(ledger, "T-d", "RUNNING", claim="worker", ingest=True)
    ledger.set_runner_pid("T-d", os.getpid(), proc_start(os.getpid()))    # this process is the worker
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-d")
    try:
        out = await tools.submit_result(hub, "B:desk", "complete", "done", task_id="T-d", how="ran it", notes="-")
        assert out["recorded"] is True and _log_lines(workdir) == []      # a draft: nothing written yet
        monkeypatch.delenv("MUTMUAS_TASK_ID")
        assert await daemon._deliver_draft("T-d")
        [line] = _log_lines(workdir)
        assert line.split(" | ")[4:] == ["ran it", "-"]
    finally:
        ledger.close()


async def test_agentctl_submit_result_takes_how_and_notes(monkeypatch):
    calls = []

    async def fake_submit(hub, me, status, summary, **kw):
        calls.append(kw)
        return {}

    monkeypatch.setattr(tools, "submit_result", fake_submit)
    args = cli.agentctl_parser().parse_args(["submit-result", "--task", "T-d", "--status", "complete", "--summary",
                                             "s", "--how", "h", "--notes", "n", "--as", "B:desk"])
    await cli.cmd_submit(args, None)
    assert calls[0]["how"] == "h" and calls[0]["notes"] == "n"


async def test_agentctl_submit_result_reads_the_rest_from_a_file(tmp_path, monkeypatch):
    """A script worker writes its result as YAML and passes --file; the flags win over the file."""
    calls = []

    async def fake_submit(hub, me, status, summary, **kw):
        calls.append((status, summary, kw))
        return {}

    monkeypatch.setattr(tools, "submit_result", fake_submit)
    result = tmp_path / "result.yaml"
    result.write_text("status: partial\nsummary: from the file\noutputs: {loss: 0.2}\nnotes: n\n"
                      "artifacts: [{uri: 'artifact://p/B/desk/T-d/x.json'}]\n")
    args = cli.agentctl_parser().parse_args(["submit-result", "--task", "T-d", "--file", str(result),
                                             "--status", "complete", "--as", "B:desk"])
    await cli.cmd_submit(args, None)
    [(status, summary, kw)] = calls
    assert (status, summary) == ("complete", "from the file")
    assert kw["outputs"] == {"loss": 0.2} and kw["notes"] == "n" and kw["artifacts"][0]["uri"].endswith("x.json")


def test_the_worker_prompt_leaves_the_log_to_the_node(tmp_path):
    from test_worker_setup import _claude_ctx

    from mutmuas.runtime import worker_prompt
    _, ctx, _ = _claude_ctx(tmp_path)
    prompt = worker_prompt(ctx)
    assert "how" in prompt and "notes" in prompt and "the node writes" in prompt
    assert "append a short record" not in prompt


def test_the_plan_section_ignores_code_blocks_and_similar_task_ids():
    """Codex light review T-20260930035834-1a21c09a: a '# comment' inside a fenced command is not a heading, and
    T-d is not T-d2 (whole task ids only); an indented ATX heading (up to 3 spaces) counts."""
    from mutmuas.hub import drop_plan_section, plan_section
    board = """# T-d2: a similar id
- [x] not this one

  ## T-d: the one
- [>] run it:
```sh
# T-d comment that looks like a heading
make test
```
- [ ] check

## T-later
- [ ] no
"""
    section = plan_section(board, "T-d")
    assert section.startswith("  ## T-d: the one") and "make test" in section and "- [ ] check" in section
    assert "T-later" not in section and "T-d2" not in section
    rest = drop_plan_section(board, "T-d")
    assert "# T-d2: a similar id" in rest and "## T-later" in rest and "make test" not in rest
    assert plan_section(board, "T-d3") is None and drop_plan_section(board, "T-d3") == board


def _request(task_id: str, objective: str = "label the desk images", **extra) -> Envelope:
    return Envelope(type="REQUEST", sender="A:sender", to="B:desk", task_id=task_id,
                    body={**request_body(objective, "reason"), **extra})


async def _arrive(daemon, agent, env):
    daemon.hub.ledger.ingest(env)
    state = await daemon._on_request(agent, env)
    daemon.hub.ledger.mark_handled(env.message_id, state or "handled")


@pytest.fixture
def session():
    proc = Orphan("import time; time.sleep(60)")
    yield proc
    proc.kill()


def _out(ledger, type_, task_id):
    return [e for e in ledger.outbox() if e.type == type_ and e.task_id == task_id]


async def test_arriving_work_is_written_into_the_plan_and_the_sender_told_its_place(tmp_path, session):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    ledger.session_beat("B:desk", session.pid, str(agent.workdir_path), session_pid=session.pid)
    plan = agent.workdir_path / "PLAN.md"
    try:
        await _arrive(daemon, agent, _request("T-1", "first job\nwith details"))
        await _arrive(daemon, agent, _request("T-2", "second job"))
        text = plan.read_text()
        assert "## 收件" in text and "- [ ] T-1 from A:sender: first job" in text and "with details" not in text
        assert "- [ ] T-2 from A:sender: second job" in text
        [receipt] = [e for e in _out(ledger, "UPDATE", "T-2") if e.body.get("state") == "PENDING"]
        assert "2 in the queue" in receipt.body["message"] and receipt.body.get("position") == 2
        await hub.finish("T-1", result_body("complete", "done"))
        assert "T-1" not in plan.read_text() and "T-2" in plan.read_text()
    finally:
        ledger.close()


async def test_a_workers_acknowledgement_carries_its_place_too(tmp_path):
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    try:
        await _arrive(daemon, agent, _request("T-1"))
        [ack] = _out(ledger, "ACK", "T-1")
        assert ack.body.get("position") == 1 and "1 in the queue" in ack.body["message"]
        assert "T-1" in (agent.workdir_path / "PLAN.md").read_text()
    finally:
        ledger.close()


async def test_finish_does_not_overwrite_a_line_the_node_adds_meanwhile(tmp_path, monkeypatch):
    """Another node writer (a new 收件 line) arrives while finish rewrites PLAN.md: it must not be lost."""
    import threading
    from pathlib import Path
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    board = agent.workdir_path / "PLAN.md"
    owned_task(ledger, "T-done", ingest=True)
    owned_task(ledger, "T-new", ingest=True)
    board.parent.mkdir(parents=True, exist_ok=True)
    board.write_text("# PLAN\n## [>] T-done test\n- [x] done\n\n## 收件\n")
    other = []

    def meanwhile():                                   # the moment finish writes the board back
        if not other:
            other.append(threading.Thread(target=hub.add_inbox_line, args=(ledger.task("T-new", "owner"),)))
            other[0].start()
            other[0].join(0.5)
    write_text, replace = Path.write_text, Path.replace

    def patched_write(path, *a, **k):
        if path == board:
            meanwhile()
        return write_text(path, *a, **k)

    def patched_replace(path, target):
        if target == board:
            meanwhile()
        return replace(path, target)
    monkeypatch.setattr(Path, "write_text", patched_write)
    monkeypatch.setattr(Path, "replace", patched_replace)
    try:
        await hub.finish("T-done", result_body("complete", "done"))
        other[0].join(5)
        text = board.read_text()
        assert "T-new" in text and "T-done test" not in text
        assert "- [x] done" in ledger.task("T-done", "owner")["result"]["outputs"]["plan"]
    finally:
        ledger.close()


async def test_a_refused_result_leaves_the_plan_section_alone(tmp_path):
    """Codex review of ea40b88: an invalid RESULT (empty summary) was refused after the task's PLAN section had
    already been taken off the board."""
    agent, _, ledger, hub, daemon = auto_worker_node(tmp_path)
    owned_task(ledger, "T-s", "RUNNING", ingest=True)
    board = agent.workdir_path / "PLAN.md"
    board.parent.mkdir(parents=True, exist_ok=True)
    board.write_text("# PLAN\n## [>] T-s\n- [ ] important unsaved work\n")
    try:
        with pytest.raises(Exception):
            await tools.submit_result(hub, "B:desk", "complete", "", task_id="T-s")
        task = ledger.task("T-s", "owner")
        assert task["status"] == "RUNNING" and task["result"] is None
        assert "important unsaved work" in board.read_text()
    finally:
        ledger.close()


async def test_every_run_keeps_its_own_log(tmp_path):
    """r19 F1: a woken task starts again at attempt 1, and its log overwrote the first run's."""
    from mutmuas.config import AgentConfig, NodeConfig
    from mutmuas.protocol import Envelope, request_body
    agent = AgentConfig(id="desk", mode="worker", runtime="script", workdir=str(tmp_path / "work"),
                        command=[sys.executable, "-c", "print('run')"])
    node = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), agents=[agent])
    req = Envelope(type="REQUEST", sender="A:s", to="B:desk", task_id="T-j", body=request_body("x", "y"))
    runtime = ScriptRuntime(agent, node)
    first = await runtime.run(TaskContext("T-j", req, agent, node, 1))
    second = await runtime.run(TaskContext("T-j", req, agent, node, 1))
    assert first.log_path != second.log_path
    assert "run" in open(first.log_path).read() and "run" in open(second.log_path).read()
