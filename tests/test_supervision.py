"""D-040: errors are skipped and recorded, and a failed task is laid out once more (supervision instead of
defensive code)."""

from __future__ import annotations

from mutmuas import cli
from mutmuas.config import AgentConfig, NodeConfig
from mutmuas.ledger import Ledger


def _ledger(tmp_path):
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"),
                     agents=[AgentConfig(id="desk", mode="interactive")])
    return cfg, Ledger(cfg.db_path)


def test_failures_are_recorded_and_listed_newest_first(tmp_path):
    _, ledger = _ledger(tmp_path)
    try:
        ledger.record_failure("heartbeat", RuntimeError("bus down"))
        ledger.record_failure("run", TimeoutError("timed out"), address="B:desk", task_id="T-1", attempt=1)
        rows = ledger.failures(limit=10)
        assert [r["stage"] for r in rows] == ["run", "heartbeat"]
        run = rows[0]
        assert (run["address"], run["task_id"], run["attempt"]) == ("B:desk", "T-1", 1)
        assert run["error"] == "TimeoutError: timed out" and run["at"]
    finally:
        ledger.close()


def test_agentctl_failures_lists_them(tmp_path, capsys, monkeypatch):
    cfg, ledger = _ledger(tmp_path)
    ledger.record_failure("recover", RuntimeError("old worker still running"), address="B:desk", task_id="T-9")
    ledger.close()
    args = cli.agentctl_parser().parse_args(["failures", "--config", str(tmp_path / "node.yaml")])
    assert args.fn.__name__ == "cmd_failures" and args.bus is False
