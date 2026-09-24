"""Example configs stay loadable; config errors are caught early; generated server config is valid."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from conftest import ROOT, nats_binary

from mutmuas.config import ConfigError, load_config
from mutmuas.server_config import generate


@pytest.mark.parametrize("name", ["example-node-A.yaml", "example-node-B.yaml"])
def test_example_configs_load(tmp_path, name):
    raw = yaml.safe_load((ROOT / "config" / name).read_text())
    creds = tmp_path / "node.env"
    creds.write_text("MUTMUAS_NATS_USER=node_X\nMUTMUAS_NATS_PASSWORD=secret\n")
    raw["nats"]["credentials_file"] = str(creds)
    path = tmp_path / name
    path.write_text(yaml.safe_dump(raw))
    cfg = load_config(path)
    assert cfg.nats.user == "node_X" and cfg.nats.password == "secret"
    assert all(a.workdir_path.is_absolute() for a in cfg.agents)
    assert any(a.mode == "worker" for a in cfg.agents)


@pytest.mark.parametrize("patch, message", [
    ({"node": "B.1"}, "invalid node id"),
    ({"agents": [{"id": "x", "mode": "worker"}]}, "need runtime"),
    ({"agents": [{"id": "x", "mode": "interactive", "permissions": ["GOD"]}]}, "unknown permission"),
    ({"agents": [{"id": "x", "mode": "interactive"}, {"id": "x", "mode": "interactive"}]}, "duplicate"),
    ({"agnets": []}, "unknown key"),
])
def test_config_errors(tmp_path, patch, message):
    path = tmp_path / "n.yaml"
    path.write_text(yaml.safe_dump({"project": "p", "node": "A", **patch}))
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_relative_paths_resolve_against_config_file(tmp_path):
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "A.env").write_text("MUTMUAS_NATS_USER=node_A\nMUTMUAS_NATS_PASSWORD=pw\n")
    path = tmp_path / "cfg" / "node.yaml"
    path.write_text(yaml.safe_dump({"project": "p", "node": "A", "data_dir": "./data",
                                    "nats": {"credentials_file": "A.env"},
                                    "agents": [{"id": "x", "mode": "interactive", "workdir": "work"}]}))
    cfg = load_config(path)
    assert cfg.data_path == (tmp_path / "cfg" / "data").resolve()
    assert cfg.agents[0].workdir_path == (tmp_path / "cfg" / "work").resolve()
    assert cfg.nats.user == "node_A"


def test_generated_server_config_is_valid(tmp_path):
    written = generate("visual_rl", ["A", "B", "C"], tmp_path, store_dir=str(tmp_path / "js"),
                       tls_hosts=["127.0.0.1", "gpu.example.org"])
    assert set(written) == {"server", "A", "B", "C", "admin", "ca"}
    assert oct(written["A"].stat().st_mode)[-3:] == "600"
    text = written["server"].read_text()
    assert '"mm.visual_rl.msg.*.*.A"' in text and '"mm.visual_rl.msg.*.*.B"' in text
    for conf in (written["server"], generate("p", ["A"], tmp_path / "plain")["server"]):
        out = subprocess.run([nats_binary(), "-c", str(conf), "-t"], capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
    san = subprocess.run(["openssl", "x509", "-in", str(tmp_path / "tls/server.crt"), "-noout", "-ext",
                          "subjectAltName"], capture_output=True, text=True).stdout
    assert "127.0.0.1" in san and "gpu.example.org" in san
    ca_before = written["ca"].read_bytes()
    assert generate("visual_rl", ["A", "B", "C", "D"], tmp_path, tls_hosts=["127.0.0.1"])["ca"].read_bytes() == ca_before


def test_adding_a_node_keeps_existing_credentials(tmp_path):
    first = generate("p", ["A", "B"], tmp_path)
    old = {n: first[n].read_text() for n in ("A", "B", "admin")}
    second = generate("p", ["A", "B", "C"], tmp_path)
    assert {n: second[n].read_text() for n in ("A", "B", "admin")} == old
    assert "node_C" in second["server"].read_text()
