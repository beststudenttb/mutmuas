"""D-109: every letter follows a fixed template (src/mutmuas/letters.yaml). The framework renders its title; the
agent fills in the blanks; a required blank left empty refuses the letter at the MCP tool or agentctl command,
before anything is sent."""

from __future__ import annotations

import pytest
from conftest import auto_worker_node, owned_task

from mutmuas import cli, letters, tools
from mutmuas.hub import Hub
from mutmuas.ledger import Ledger

LETTER_TOOLS = {"send_request", "send_notice", "send_data", "submit_result", "accept_task", "reject_task",
                "ask_question", "answer_question", "chase_task", "report_progress", "control_task", "remind_me",
                "accept_delivery", "withdraw_delivery", "send_relay"}


def _sent(ledger, type_):
    return [e for e in ledger.outbox() if e.type == type_]


async def _server(tmp_path, monkeypatch):
    from mutmuas.mcp_server import build_server

    async def no_bus(cfg, *_args, **_kwargs):
        return Hub(cfg, None, Ledger(cfg.db_path))
    monkeypatch.setattr(Hub, "open", no_bus)
    agent, cfg, ledger, hub, _ = auto_worker_node(tmp_path)
    return build_server(cfg, "B:desk"), ledger


async def _call(server, tool, args) -> str:
    try:
        out = await server.call_tool(tool, args)
    except Exception as e:                      # the MCP layer may raise the tool's error
        return str(e)
    return out.content[0].text


async def test_every_letter_tool_has_a_template_and_every_template_a_tool(tmp_path, monkeypatch):
    server, ledger = await _server(tmp_path, monkeypatch)
    try:
        names = {t.name for t in await server.list_tools()}
        assert {t["tool"] for t in letters.TEMPLATES.values()} == LETTER_TOOLS <= names
        for kind, template in letters.TEMPLATES.items():
            assert template["name"] and template["title"] and template["fields"], kind
            assert all(set(spec) <= {"required", "label"} for spec in template["fields"].values()), kind
    finally:
        ledger.close()


async def test_a_letter_with_an_empty_required_blank_is_refused_before_anything_is_sent(tmp_path, monkeypatch):
    server, ledger = await _server(tmp_path, monkeypatch)
    try:
        async with server.settings.lifespan(server):
            text = await _call(server, "send_request", {"to": "C:far", "objective": "train it", "reason": "need it",
                                                        "expected_outputs": ["a model"]})
            assert "acceptance_criteria" in text and "验收标准" in text
            owned_task(ledger, "T-r")
            text = await _call(server, "reject_task", {"task_id": "T-r", "reason": "not mine"})
            assert "suggest" in text and "建议找谁" in text
            assert ledger.outbox() == [] and ledger.task("T-r", "owner")["status"] == "PENDING"
            text = await _call(server, "send_request", {"to": "C:far", "objective": "train it", "reason": "need it",
                                                        "expected_outputs": ["a model"], "deadline": "+2h",
                                                        "acceptance_criteria": ["loss < 0.2"]})
            assert "task_id" in text and _sent(ledger, "REQUEST")[0].body["acceptance"] == "manual"   # an agent's
    finally:
        ledger.close()


async def test_the_framework_renders_each_letters_title(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    owned_task(ledger, "T-2", ingest=True)
    try:
        await tools.send_request(hub, "B:desk", "C:far", "label the images", "for training")
        assert _sent(ledger, "REQUEST")[-1].body["title"] == "【需求】label the images"
        await tools.submit_result(hub, "B:desk", "complete", "done", task_id="T-1")
        assert _sent(ledger, "RESULT")[-1].body["title"] == "Re:test task"
        await tools.reject_task(hub, "B:desk", "T-2", "not my field", suggest="C:vision")
        [refusal] = _sent(ledger, "REJECT")
        assert refusal.body["title"] == "退回:test task" and refusal.body["suggest"] == "C:vision"
    finally:
        ledger.close()


async def test_agentctl_refuses_an_incomplete_request_and_sends_a_notice(tmp_path, capsys):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    try:
        parser = cli.agentctl_parser()
        with pytest.raises(SystemExit, match="reason"):        # (outputs and criteria: see test_acceptance, P5)
            await cli.cmd_ask(parser.parse_args(["ask", "C:far", "train it", "--expect", "x", "--as", "B:desk"]), hub)
        assert ledger.outbox() == []
        await cli.cmd_notice(parser.parse_args(["notice", "C:far", "the GPU is free again", "--as", "B:desk"]), hub)
        [notice] = _sent(ledger, "REQUEST")
        assert notice.body["title"] == "【告知】the GPU is free again" and notice.body["reply"] == "none"
    finally:
        ledger.close()


async def test_data_and_a_chase_go_on_the_task(tmp_path):
    _, _, ledger, hub, _ = auto_worker_node(tmp_path)
    owned_task(ledger, "T-1", "RUNNING", claim="session", ingest=True)
    asked = (await tools.send_request(hub, "B:desk", "C:far", "train it", "need it"))["task_id"]
    try:
        await tools.send_data(hub, "B:desk", "A:sender", [{"uri": "artifact://p/B/desk/T-1/x.csv"}],
                              "the labels: put them in data/", task_id="T-1")
        [data] = [e for e in _sent(ledger, "UPDATE") if e.body.get("data")]
        assert data.to == "A:sender" and data.artifacts[0].uri.endswith("x.csv")
        assert data.body["title"] == "【数据】the labels: put them in data/"
        await tools.chase_task(hub, "B:desk", asked)
        [chase] = [e for e in _sent(ledger, "UPDATE") if e.task_id == asked]
        assert chase.to == "C:far" and chase.body["next"] == "C:far" and chase.body["title"] == "【催交】train it"
        with pytest.raises(PermissionError):
            await tools.chase_task(hub, "B:desk", "T-1")                  # not one it requested
    finally:
        ledger.close()


async def test_a_relay_carries_the_leaders_words_the_relayers_understanding_and_who_answers(tmp_path, monkeypatch):
    """D-111: C:claude passes on the leader's word: his exact words, its own understanding (to be corrected), and
    who should answer what. It is the leader's (leader: true); the reply needs no acceptance."""
    server, ledger = await _server(tmp_path, monkeypatch)
    try:
        async with server.settings.lifespan(server):
            text = await _call(server, "send_relay", {"to": "B:secretary", "words": "切"})
            assert "understanding" in text and "ask" in text and ledger.outbox() == []
            text = await _call(server, "send_relay", {"to": "B:secretary", "words": "切",
                                                      "understanding": "switch the run to the new env",
                                                      "ask": "B:secretary: confirm and give it a D number"})
            [relay] = _sent(ledger, "REQUEST")
            assert relay.body["title"] == "【转达】切" and relay.body["leader"] is True
            assert relay.body["inputs"] == {"words": "切", "understanding": "switch the run to the new env",
                                            "ask": "B:secretary: confirm and give it a D number"}
            assert "acceptance" not in relay.body
    finally:
        ledger.close()
