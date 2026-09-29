"""D-049: the leader's tasks go first. The session that sends on the leader's behalf marks the REQUEST
(leader: true; D-035, honest marking); a worker's queue takes those first, the rest in arrival order, and the
session's inbox lists them first."""

from __future__ import annotations

from conftest import auto_worker_node

from mutmuas import cli, tools
from mutmuas.protocol import Envelope, request_body


def _request(ledger, task_id: str, leader: bool = False) -> None:
    env = Envelope(type="REQUEST", sender="B:secretary", to="B:desk", task_id=task_id,
                   body=request_body(f"do {task_id}", "test", leader=leader))
    ledger.ingest(env)
    ledger.create_owned_task(env)
    ledger.mark_handled(env.message_id)                  # what the dispatcher does: now it shows in the inbox


def test_the_leader_mark_goes_into_the_request_body():
    assert request_body("x", "y", leader=True)["leader"] is True
    assert "leader" not in request_body("x", "y")


async def test_a_workers_queue_takes_the_leaders_tasks_first(tmp_path):
    _, _, ledger, _, daemon = auto_worker_node(tmp_path)
    try:
        for task_id, leader in (("T-1", False), ("T-2", False), ("T-L1", True), ("T-3", False), ("T-L2", True)):
            _request(ledger, task_id, leader)
            daemon._enqueue("B:desk", task_id)
        queue = daemon._queues["B:desk"]
        order = [queue.get_nowait()[-1] for _ in range(queue.qsize())]
        assert order == ["T-L1", "T-L2", "T-1", "T-2", "T-3"]
    finally:
        ledger.close()


async def test_the_sessions_inbox_lists_the_leaders_mail_first(tmp_path):
    """A watcher's --peek keeps arrival order: its cursor is the last row's received_at."""
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    try:
        for task_id, leader in (("T-1", False), ("T-L", True), ("T-2", False)):
            _request(ledger, task_id, leader)
        peeked = await tools.inbox(hub, "B:desk", peek=True)
        assert [r["task_id"] for r in peeked] == ["T-1", "T-L", "T-2"]
        read = await tools.inbox(hub, "B:desk")
        assert [r["task_id"] for r in read] == ["T-L", "T-1", "T-2"]
    finally:
        ledger.close()


async def test_ask_and_send_carry_the_leader_mark(monkeypatch, tmp_path):
    sent = []

    async def fake_send_request(hub, me, to, objective, reason, **kw):
        sent.append(kw.get("leader"))
        return {"task_id": "T-x"}

    monkeypatch.setattr(tools, "send_request", fake_send_request)
    parser = cli.agentctl_parser()
    await cli.cmd_ask(parser.parse_args(["ask", "B:desk", "obj", "--leader", "--as", "A:me"]), None)
    await cli.cmd_ask(parser.parse_args(["ask", "B:desk", "obj", "--as", "A:me"]), None)
    request = tmp_path / "r.json"
    request.write_text('{"objective": "obj", "reason": "r", "leader": true}')
    await cli.cmd_send(parser.parse_args(["send", "B:desk", "--type", "REQUEST", "--file", str(request),
                                          "--as", "A:me"]), None)
    assert sent == [True, False, True]
