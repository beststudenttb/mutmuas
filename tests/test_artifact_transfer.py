"""TEST 1 — cross-machine data request: A asks B for a file, B publishes it, A downloads and verifies."""

import json

import pytest
from conftest import eventually, interactive, thread_types, worker

from mutmuas import tools
from mutmuas.artifacts import ArtifactError, ArtifactUnavailable
from mutmuas.protocol import ArtifactRef


async def test_file_request_end_to_end(make_config, cluster, tmp_path):
    data = tmp_path / "B-disk" / "representation_exp082" / "latents.json"
    data.parent.mkdir(parents=True)
    payload = {"exp": 82, "latents": [[i * 0.5, -i] for i in range(2000)]}
    data.write_text(json.dumps(payload))

    a = make_config("A", [interactive("main", display="A:a1")])
    b = make_config("B", [worker("data", "lab.py", capabilities=["representation_data"], display="B:a1")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)

    # A:a1 does not know who has the data; it asks the registry.
    who = (await tools.find_agent(hub, "representation_data"))["best"]["address"]
    sent = await tools.send_request(
        hub, "A:main", who, "Return the latent data of representation experiment 82",
        "A:a1 needs it to continue the probing analysis", kind="artifact",
        inputs={"action": "fetch_file", "path": str(data)}, expected_outputs=["latents.json as artifact"])
    result = await tools.wait_for_result(hub, sent["task_id"], 30)

    assert result["status"] == "COMPLETED" and result["result_status"] == "complete", result
    (ref,) = result["output_refs"]
    assert ref["uri"].startswith("artifact://testproj/B/data/")
    fetched = await tools.fetch_artifact(hub, ref["uri"], str(tmp_path / "A-disk"), ref["sha256"])
    assert json.loads(open(fetched["path"]).read()) == payload
    types = thread_types(hub, sent["task_id"])
    assert types[0] == "REQUEST" and types[-1] == "RESULT" and "ACK" in types and "UPDATE" in types


async def test_directory_artifact_roundtrip(make_config, cluster, tmp_path):
    b = make_config("B", [interactive("main")])
    hub = await cluster.client(b)
    src = tmp_path / "run42"
    (src / "ckpt").mkdir(parents=True)
    (src / "ckpt" / "model.bin").write_bytes(bytes(range(256)) * 4096)   # 1 MiB, several chunks
    (src / "config.yaml").write_text("lr: 0.0003\n")

    ref = await tools.publish_artifact(hub, "B:main", str(src))
    out = await tools.fetch_artifact(hub, ref["uri"], str(tmp_path / "dest"))
    assert (tmp_path / "dest" / "run42" / "ckpt" / "model.bin").read_bytes() == (src / "ckpt" / "model.bin").read_bytes()
    assert out["path"].endswith("run42")


async def test_artifact_unavailable_and_integrity(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    hub = await cluster.client(a)
    with pytest.raises(ArtifactUnavailable):
        await hub.artifacts.fetch("artifact://testproj/B/data/never-published.bin", tmp_path)
    with pytest.raises(ArtifactUnavailable, match="lives on node B"):
        await hub.artifacts.fetch("file://B/definitely/not/on/this/machine.pt", tmp_path)
    with pytest.raises(ArtifactUnavailable):
        await hub.artifacts.fetch("git://github.com/x/y@48af2c1", tmp_path)

    f = tmp_path / "m.json"
    f.write_text("{}")
    ref = await tools.publish_artifact(hub, "A:main", str(f))
    with pytest.raises(ArtifactError, match="checksum"):
        await hub.artifacts.fetch(ArtifactRef(uri=ref["uri"], sha256="0" * 64), tmp_path / "x")


async def test_missing_file_is_reported_as_failed_not_complete(make_config, cluster, tmp_path):
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("data", "lab.py")])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:data", "return file", "test", kind="artifact",
                                    inputs={"action": "fetch_file", "path": str(tmp_path / "nope.npz")})
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    assert result["status"] == "FAILED" and result["result_status"] == "failed"
    assert "does not exist" in result["result"]["summary"]
