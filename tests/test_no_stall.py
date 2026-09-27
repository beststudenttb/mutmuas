"""No-stall mechanism v1 (secretary's NO-STALL-DESIGN.md, r11/r11b), the parts that need no leader:
G3 a request that needs a reply gets a default deadline; G2 the RESULT of my own request wakes me."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from conftest import eventually, interactive, worker
from mutmuas import tools
from mutmuas.ids import parse_iso


async def _pair(make_config, cluster, **a_extra):
    a = make_config("A", [interactive("main"), interactive("desk2")], **a_extra)
    b = make_config("B", [interactive("desk"), worker("lab", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    return a, b, await cluster.client(a), await cluster.client(b)


async def test_g3_a_request_that_needs_a_reply_gets_the_default_deadline(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, default_reply_deadline_s=7200)
    before = datetime.now(timezone.utc)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "report", "g3")
    deadline = parse_iso(hub_a.ledger.task(sent["task_id"], "requester")["request"]["deadline"])
    assert 7190 <= (deadline - before).total_seconds() <= 7260
    assert "default deadline" in sent["note_deadline"]
    fyi = await tools.send_request(hub_a, "A:main", "B:desk", "notice", "g3", reply="none")
    assert "deadline" not in hub_a.ledger.task(fyi["task_id"], "requester")["request"]
    own = await tools.send_request(hub_a, "A:main", "B:desk", "x", "g3", deadline="2030-01-01T00:00:00+00:00")
    assert hub_a.ledger.task(own["task_id"], "requester")["request"]["deadline"] == "2030-01-01T00:00:00+00:00"


async def test_g3_default_deadline_zero_means_off(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, default_reply_deadline_s=0)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "report", "g3 off")
    assert "deadline" not in hub_a.ledger.task(sent["task_id"], "requester")["request"]


async def test_g3_the_default_deadline_triggers_the_overdue_follow_up(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, default_reply_deadline_s=1)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "report", "g3 overdue")
    await eventually(lambda: hub_a.task_view(sent["task_id"]), what="task known")
    await asyncio.sleep(1.5)
    await cluster.daemons["A"]._follow_ups()
    got = await eventually(lambda: [m for m in hub_a.ledger.thread(sent["task_id"])
                                    if (m.get("body") or {}).get("follow_up") == "overdue"], what="overdue follow-up")
    assert got


async def test_g2_the_result_of_my_own_request_wakes_me(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, wake_on_own_results=True)
    asked = await tools.send_request(hub_a, "A:main", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "hi"})
    await tools.wait_for_result(hub_a, asked["task_id"], 30)
    wake = await tools.inbox(hub_a, "A:main", peek=True, types=tools.WAKE)
    assert asked["task_id"] in {m["task_id"] for m in wake if m["type"] == "RESULT"}
    assert await tools.inbox(hub_a, "A:main", peek=True, wait_s=1, types=tools.WAKE)   # a waiting watcher wakes


async def test_g2_no_wake_for_a_notice_or_for_someone_elses_request(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, wake_on_own_results=True)
    notice = await tools.send_request(hub_a, "A:main", "B:lab", "echo", "g2", reply="none",
                                      inputs={"action": "echo", "text": "fyi"})
    await tools.wait_for_result(hub_a, notice["task_id"], 30)
    other = await tools.send_request(hub_a, "A:desk2", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "x"})
    await tools.wait_for_result(hub_a, other["task_id"], 30, me="A:desk2")
    wake = {m["task_id"] for m in await tools.inbox(hub_a, "A:main", peek=True, types=tools.WAKE)}
    assert notice["task_id"] not in wake and other["task_id"] not in wake
