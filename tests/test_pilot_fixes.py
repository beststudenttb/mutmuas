"""Small fixes from the pilot (T-20260929081919-bed3d95a): a worker's accept of its own task, the worker prompt's
git and accept notes, auto_worker in agents --json, Skill for LLM workers."""

from __future__ import annotations

import os

from conftest import auto_worker_node, owned_task
from test_staff_v4 import _claude_ctx

from mutmuas import tools
from mutmuas.node import proc_start
from mutmuas.runtime import worker_prompt


async def test_a_worker_accepting_its_own_task_gets_success(tmp_path):
    """The pilot's worker called accept_task on the task the daemon had already claimed for it and got the
    misleading 'being done by the worker (D-032a)' error; for the worker itself it is a no-op."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-own", "RUNNING", claim="worker")
    ledger.set_runner_pid("T-own", os.getpid(), proc_start(os.getpid()))    # this process is the worker
    try:
        out = await tools.accept_task(hub, "B:desk", "T-own")
        assert out["accepted"] is True
        task = ledger.task("T-own", "owner")
        assert task["status"] == "RUNNING" and task["runner"] == "worker"
    finally:
        ledger.close()


async def test_a_plain_workers_accept_does_not_hand_its_task_to_the_session(tmp_path, monkeypatch):
    """A worker-mode task has no runner claim; an accept from its worker must not claim it for the session,
    or the worker's own submit_result is refused afterwards."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-own", "RUNNING")
    monkeypatch.setenv("MUTMUAS_TASK_ID", "T-own")
    try:
        assert (await tools.accept_task(hub, "B:desk", "T-own"))["accepted"] is True
        assert ledger.task("T-own", "owner")["runner"] is None
        out = await tools.submit_result(hub, "B:desk", "complete", "done", task_id="T-own")
        assert out["recorded"] is True                                     # not refused as the session's task
    finally:
        ledger.close()



def test_the_worker_prompt_says_the_task_is_already_accepted(tmp_path):
    _, ctx, _ = _claude_ctx(tmp_path)
    assert "do not call accept_task" in worker_prompt(ctx)


def test_the_worker_prompt_says_how_to_run_git(tmp_path):
    """The pilot's worker ran 'cd <worktree> && git ...', which Bash(git:*) does not match."""
    _, ctx, _ = _claude_ctx(tmp_path, kind="code", permissions=("READ", "WRITE_WORKTREE"))
    ctx.git_branch = "mutmuas/C/paper-visualrl/T-1"
    prompt = worker_prompt(ctx)
    assert "git -C <dir>" in prompt and "cd <dir> && git" in prompt


def test_agents_json_shows_auto_worker():
    """The card carries auto_worker (CARD_KEYS); the summary agents and find_agent print dropped it."""
    card = {"address": "B:desk", "mode": "interactive", "auto_worker": True}
    assert tools.card_summary(card)["auto_worker"] is True


def test_llm_workers_get_skill(tmp_path):
    """The pilot showed a worker uses project skills once Skill is in --tools (it was not by default)."""
    runtime, ctx, _ = _claude_ctx(tmp_path)
    argv, _ = runtime.command(ctx)
    assert "Skill" in argv[argv.index("--tools") + 1].split(",")
    assert "Skill" in argv[argv.index("--allowedTools") + 1].split(",")
