"""No-stall mechanism v1 (secretary's NO-STALL-DESIGN.md, r11/r11b), the parts that need no leader:
G3 a request that needs a reply gets a default deadline; G2 the RESULT of my own request wakes me."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from conftest import eventually, interactive, worker
from mutmuas import tools
from mutmuas.ids import parse_iso


async def _pair(make_config, cluster, main_wakes=False, **a_extra):
    a = make_config("A", [interactive("main", wake_on_own_results=main_wakes), interactive("desk2")], **a_extra)
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
    # the owner can tell a default deadline from one the requester chose (C's review of 6a5e2f1, point 3)
    got = await eventually(lambda: hub_b.ledger.task(sent["task_id"], "owner"), what="owner has it")
    assert got["request"].get("deadline_default") is True
    fyi = await tools.send_request(hub_a, "A:main", "B:desk", "notice", "g3", reply="none")
    assert "deadline" not in hub_a.ledger.task(fyi["task_id"], "requester")["request"]
    own = await tools.send_request(hub_a, "A:main", "B:desk", "x", "g3", deadline="+30d")
    chosen = hub_a.ledger.task(own["task_id"], "requester")["request"]
    assert parse_iso(chosen["deadline"]) - datetime.now(timezone.utc) > timedelta(days=29)
    assert not chosen.get("deadline_default")


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
    a, b, hub_a, hub_b = await _pair(make_config, cluster, main_wakes=True)
    asked = await tools.send_request(hub_a, "A:main", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "hi"})
    await tools.wait_for_result(hub_a, asked["task_id"], 30)
    wake = await tools.inbox(hub_a, "A:main", peek=True, types=tools.WAKE)
    assert asked["task_id"] in {m["task_id"] for m in wake if m["type"] == "RESULT"}
    assert await tools.inbox(hub_a, "A:main", peek=True, wait_s=1, types=tools.WAKE)   # a waiting watcher wakes


async def test_g2_no_wake_for_a_notice_or_for_someone_elses_request(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, main_wakes=True)
    notice = await tools.send_request(hub_a, "A:main", "B:lab", "echo", "g2", reply="none",
                                      inputs={"action": "echo", "text": "fyi"})
    await tools.wait_for_result(hub_a, notice["task_id"], 30)
    other = await tools.send_request(hub_a, "A:desk2", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "x"})
    await tools.wait_for_result(hub_a, other["task_id"], 30, me="A:desk2")
    wake = {m["task_id"] for m in await tools.inbox(hub_a, "A:main", peek=True, types=tools.WAKE)}
    assert notice["task_id"] not in wake and other["task_id"] not in wake


# C's review of 6a5e2f1 (T-20260927190001-fcb94ea4)

async def test_g3_default_deadline_leaves_room_for_the_task_timeout(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, default_reply_deadline_s=3600)
    before = datetime.now(timezone.utc)
    sent = await tools.send_request(hub_a, "A:main", "B:desk", "train", "long", timeout_s=8 * 3600)
    deadline = parse_iso(hub_a.ledger.task(sent["task_id"], "requester")["request"]["deadline"])
    assert (deadline - before).total_seconds() > 8 * 3600          # not chased while it may still run


async def test_g3_the_cli_ask_path_gets_the_default_too(make_config, cluster):
    import subprocess
    import sys
    from pathlib import Path
    a, b, hub_a, hub_b = await _pair(make_config, cluster, default_reply_deadline_s=7200)
    out = subprocess.run([str(Path(sys.executable).parent / "agentctl"), "ask", "B:desk", "report", "--reason", "cli",
                          "--json", "--config", str(a.path), "--as", "A:main"], capture_output=True, text=True,
                         timeout=60)
    assert out.returncode == 0, out.stderr
    import json
    task_id = json.loads(out.stdout)["task_id"]
    request = hub_a.ledger.task(task_id, "requester")["request"]
    assert request.get("deadline") and request.get("deadline_default") is True


async def test_g2_is_switched_per_agent(make_config, cluster):
    a, b, hub_a, hub_b = await _pair(make_config, cluster, main_wakes=True)      # A:main on, A:desk2 off
    mine = await tools.send_request(hub_a, "A:main", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "1"})
    theirs = await tools.send_request(hub_a, "A:desk2", "B:lab", "echo", "g2", inputs={"action": "echo", "text": "2"})
    await tools.wait_for_result(hub_a, mine["task_id"], 30)
    await tools.wait_for_result(hub_a, theirs["task_id"], 30, me="A:desk2")
    assert mine["task_id"] in {m["task_id"] for m in await tools.inbox(hub_a, "A:main", peek=True, types=tools.WAKE)}
    assert theirs["task_id"] not in {m["task_id"] for m in await tools.inbox(hub_a, "A:desk2", peek=True,
                                                                            types=tools.WAKE)}
