"""D-052: the node, not the model, does the mechanical part of delivery. When an owned task gets its RESULT it
appends the R7.11 line to the post's worker-log.md (how and notes from submit_result, '未填' when missing) and
attaches this task's PLAN.md section (the heading with the task id) to the result, then takes it off the board."""

from __future__ import annotations

import os

from conftest import auto_worker_node, owned_task

from mutmuas import cli, tools
from mutmuas.node import proc_start
from mutmuas.protocol import Envelope, request_body

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

