"""TEST 2 — cross-machine delegation: ACK -> RUNNING -> UPDATEs -> RESULT + artifact, then A continues."""

import json

from conftest import eventually, interactive, thread_types, worker

from mutmuas import tools


async def test_delegated_experiment(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("experimenter", "lab.py", capabilities=["isaac_lab"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)

    sent = await tools.send_request(
        hub, "A:main", "B:experimenter", "Run the ablation for module B with seed 7",
        "A found that module A's result depends on module B's encoder", kind="experiment",
        inputs={"action": "experiment", "steps": 4, "step_s": 0.1, "seed": 7},
        expected_outputs=["metrics.json"], acceptance_criteria=["4 loss values", "final loss reported"])
    task_id = sent["task_id"]

    # While it runs, the owner's state is visible from A through the shared task ledger.
    await eventually(lambda: _status(hub, task_id, "RUNNING"), what="RUNNING seen from A")
    result = await tools.wait_for_result(hub, task_id, 30)
    assert result["status"] == "COMPLETED" and result["result_status"] == "complete"
    assert result["result"]["outputs"]["final_loss"] == 0.25

    types = thread_types(hub, task_id)
    assert types[:3] == ["REQUEST", "ACK", "UPDATE"]           # ACK, then UPDATE(state RUNNING)
    assert types.count("UPDATE") >= 5 and types[-1] == "RESULT"  # RUNNING + 4 progress updates

    (ref,) = result["output_refs"]
    metrics = await tools.fetch_artifact(hub, ref["uri"], str(tmp_path / "A"))
    assert json.loads(open(metrics["path"]).read())["seed"] == 7

    # "A continues": its next task can reference the finished one as parent.
    follow = await tools.send_request(hub, "A:main", "B:experimenter", "echo", "continue after result",
                                      inputs={"action": "echo", "text": "next"})
    assert (await tools.wait_for_result(hub, follow["task_id"], 30))["result"]["summary"] == "echo: next"


async def test_three_node_chain(make_config, cluster):
    """A -> B -> C -> B -> A without any human relay; the child records its parent task."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("planner", "lab.py")])
    c = make_config("C", [worker("helper", "lab.py")])
    for cfg in (a, b, c):
        await cluster.start(cfg)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:planner", "coordinate", "chain test",
                                    inputs={"action": "delegate", "to": "C:helper"})
    result = await tools.wait_for_result(hub, sent["task_id"], 40)
    assert result["result_status"] == "complete"
    assert result["result"]["summary"] == "sub-task said: echo: from the chain"
    child = await hub.task_view(result["result"]["outputs"]["child_task"])
    assert child["parent_task"] == sent["task_id"] and child["requester"] == "B:planner"


async def test_sender_offline_after_sending(make_config, cluster):
    """A sends and goes away; B finishes; A's node comes back later and gets the RESULT."""
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("experimenter", "lab.py")])
    await cluster.start(b)
    hub = await cluster.client(a)          # A has no daemon running at all
    sent = await tools.send_request(hub, "A:main", "B:experimenter", "echo", "offline sender",
                                    inputs={"action": "echo", "text": "hi"})
    await hub.close()
    cluster.hubs.remove(hub)

    # B finishes while A is offline: visible in the shared ledger, waiting in A's durable inbox.
    probe = await cluster.client(b)
    await eventually(lambda: _status(probe, sent["task_id"], "COMPLETED"), what="B finished")

    await cluster.start(a)
    hub = await cluster.client(a)
    await eventually(lambda: (hub.ledger.task(sent["task_id"], "requester") or {}).get("status") == "COMPLETED",
                     what="A received RESULT after coming back")
    assert hub.ledger.task(sent["task_id"], "requester")["result"]["summary"] == "echo: hi"


async def _status(hub, task_id, status):
    view = await hub.task_view(task_id)
    return view and view.get("status") == status
