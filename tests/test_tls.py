"""Public-internet setup: TLS + per-node auth. Nodes verify the server with the generated private CA."""

import pytest
import yaml
from conftest import NatsServer, free_port, interactive, worker

from mutmuas import tools
from mutmuas.bus import Bus, BusUnavailable
from mutmuas.config import NatsConfig
from mutmuas.server_config import generate


async def test_full_flow_over_tls(tmp_path, make_config, cluster):
    written = generate("testproj", ["A", "B"], tmp_path / "server", tls_hosts=["127.0.0.1"],
                       listen_host="127.0.0.1", store_dir=str(tmp_path / "js-tls"), monitor_port=free_port())
    server = NatsServer(tmp_path / "js-tls", conf=written["server"])
    server.start()
    try:
        def cfg(node, agents):
            c = make_config(node, agents)
            raw = yaml.safe_load(c.path.read_text())
            raw["nats"] = {"servers": [server.url], "credentials_file": str(written[node]),
                           "tls_ca": str(written["ca"])}
            c.path.write_text(yaml.safe_dump(raw))      # worker subprocesses reload it from disk
            from mutmuas.config import load_config
            return load_config(c.path)

        a = cfg("A", [interactive("main")])
        b = cfg("B", [worker("lab", "lab.py")])
        await cluster.start(a)
        await cluster.start(b)
        hub = await cluster.client(a)
        assert hub.bus.nc.connected_url.hostname == "127.0.0.1"
        sent = await tools.send_request(hub, "A:main", "B:lab", "run", "tls test", kind="experiment",
                                        inputs={"action": "experiment", "steps": 2, "step_s": 0.05})
        result = await tools.wait_for_result(hub, sent["task_id"], 30)
        assert result["result_status"] == "complete"
        got = await tools.fetch_artifact(hub, result["output_refs"][0]["uri"], str(tmp_path / "dl"))
        assert got["size"] > 0

        creds = {"user": "node_A", "password": a.nats.password}
        with pytest.raises(BusUnavailable):          # no CA: the self-signed server cert is not trusted
            await Bus.open(NatsConfig(servers=[server.url], **creds), "testproj", "x", reconnect=False)
    finally:
        await cluster.close()
        server.stop()
