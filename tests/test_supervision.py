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


async def test_a_background_loop_error_is_recorded_and_the_loop_goes_on(tmp_path):
    """Heartbeat errors were only log.debug'ed; now they are recorded and the loop keeps running."""
    import asyncio

    from mutmuas.hub import Hub
    from mutmuas.node import NodeDaemon
    cfg = NodeConfig(project="p", node="B", data_dir=str(tmp_path / "data"), heartbeat_s=0.02,
                     agents=[AgentConfig(id="desk", mode="interactive")])
    ledger = Ledger(cfg.db_path)
    daemon = NodeDaemon(cfg)
    daemon.hub = Hub(cfg, None, ledger)
    calls = 0

    async def broken():
        nonlocal calls
        calls += 1
        raise RuntimeError("registry unavailable")

    daemon._publish_cards = broken
    loop = asyncio.create_task(daemon._heartbeat())
    try:
        await asyncio.sleep(0.2)
        assert calls >= 2                                          # it went on after the first error
        stages = {r["stage"] for r in ledger.failures()}
        assert "heartbeat" in stages
    finally:
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        ledger.close()
