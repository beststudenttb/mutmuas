"""RESULT evidence: 'complete' on code/experiment/artifact tasks needs a verified, repeatable evidence item."""

import pytest
from conftest import eventually, interactive, worker

from mutmuas import hub as hub_module
from mutmuas import tools
from mutmuas.protocol import EVIDENCE_DOWNGRADE, enforce_evidence, evidence_items, evidence_required

GOOD = {"claim": "tests pass", "how": "pytest -q tests/test_x.py", "verified": True}


def test_evidence_rules():
    assert evidence_items("sha256 matches") == [{"claim": "sha256 matches", "how": "", "verified": False}]
    assert evidence_items([{"claim": "c", "how": "h", "verified": "yes", "source": "f.py:3"}]) == [
        {"claim": "c", "how": "h", "verified": False, "source": "f.py:3"}]       # only a real True counts
    assert evidence_required({"kind": "code"}) and evidence_required({"kind": "experiment"})
    assert not evidence_required({"kind": "query"}) and not evidence_required(None)
    assert evidence_required({"kind": "query", "evidence_required": True})
    assert not evidence_required({"kind": "code", "evidence_required": False})

    code = {"kind": "code"}
    for evidence in (None, ["I checked it"], [{**GOOD, "how": ""}], [{**GOOD, "verified": False}]):
        out = enforce_evidence({"status": "complete", "summary": "s", "evidence": evidence}, code)
        assert out["status"] == "partial" and out["limitations"] == [EVIDENCE_DOWNGRADE]
    kept = {"status": "complete", "summary": "s", "evidence": [GOOD]}
    assert enforce_evidence(kept, code) == kept
    assert enforce_evidence({"status": "failed", "summary": "s"}, code)["status"] == "failed"
    once = enforce_evidence({"status": "complete", "summary": "s", "limitations": "slow"}, code)
    assert enforce_evidence(once, code) == once and once["limitations"] == ["slow", EVIDENCE_DOWNGRADE]


@pytest.mark.parametrize("evidence, kw, expected", [
    (None, {"kind": "experiment"}, "partial"),
    ([{"claim": "trust me", "verified": True}], {"kind": "experiment"}, "partial"),     # verified but no 'how'
    ([GOOD], {"kind": "experiment"}, "complete"),
    (None, {"kind": "experiment", "evidence_required": False}, "complete"),
    (None, {"kind": "query"}, "complete"),
    (None, {"kind": "query", "evidence_required": True}, "partial"),
])
async def test_requester_sees_unbacked_complete_as_partial(make_config, cluster, evidence, kw, expected):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py", permissions=["READ", "RUN_EXPERIMENT"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:lab", "claim", "evidence test",
                                    inputs={"action": "claim", "evidence": evidence}, **kw)
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    assert result["status"] == "COMPLETED" and result["result_status"] == expected
    assert (EVIDENCE_DOWNGRADE in (result["result"].get("limitations") or [])) == (expected == "partial")


async def test_owner_that_skips_the_check_gains_nothing(make_config, cluster, monkeypatch):
    # An old or dishonest owner sends 'complete' without evidence; the requester's daemon re-checks.
    monkeypatch.setattr(hub_module, "enforce_evidence", lambda result, request: result)
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("lab", "lab.py", permissions=["READ", "RUN_EXPERIMENT"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:lab", "claim", "evidence test",
                                    inputs={"action": "claim"}, kind="experiment")
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    assert result["result_status"] == "partial"
    owner_side = cluster.daemons["B"].hub.ledger.task(sent["task_id"], "owner")
    assert owner_side["result_status"] == "complete"          # proves the owner really skipped it


async def test_interactive_owner_can_add_evidence_before_anything_is_sent(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("helper", permissions=["READ", "RUN_EXPERIMENT"])])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    sent = await tools.send_request(hub_a, "A:main", "B:helper", "measure", "evidence test", kind="experiment")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner") is not None, what="request arrived")
    out = await tools.submit_result(hub_b, "B:helper", "complete", "done", task_id=sent["task_id"])
    assert out["sent"] is False and "Nothing was sent" in out["error"]
    assert hub_b.ledger.task(sent["task_id"], "owner")["status"] not in ("COMPLETED", "FAILED")
    out = await tools.submit_result(hub_b, "B:helper", "complete", "done", task_id=sent["task_id"], evidence=[GOOD])
    assert out["delivered"] and out["status"] == "complete"
    result = await tools.wait_for_result(hub_a, sent["task_id"], 30)
    assert result["result_status"] == "complete" and result["result"]["evidence"] == [GOOD]


async def test_interactive_owner_may_still_report_honest_partial(make_config, cluster):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [interactive("helper", permissions=["READ", "RUN_EXPERIMENT"])])
    await cluster.start(a)
    await cluster.start(b)
    hub_a, hub_b = await cluster.client(a), await cluster.client(b)
    sent = await tools.send_request(hub_a, "A:main", "B:helper", "measure", "evidence test", kind="experiment")
    await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner") is not None, what="request arrived")
    out = await tools.submit_result(hub_b, "B:helper", "partial", "measured, could not re-check",
                                    task_id=sent["task_id"])
    assert out["delivered"] and out["status"] == "partial"
