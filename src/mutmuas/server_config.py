"""Generate a nats-server configuration with one user per node.

Per-node publish permissions make "node A may only send as A" a server-side
guarantee: messages go to mm.<p>.msg.<to_node>.<to_agent>.<from_node>, and user
node_A may only publish subjects ending in ``.A``. Registry and task-record
writes are likewise limited to the node's own keys.

Known gap (documented in ARCHITECTURE_V1): nodes need the JetStream API
($JS.API.>) to manage their consumers, which also lets them read other
mailboxes. Isolating that requires NATS accounts/JWT — a Phase 2 item.
"""

from __future__ import annotations

import ipaddress
import secrets
import subprocess
from pathlib import Path

from .bus import inbox_prefix
from .ids import check_token


def node_permissions(project: str, node: str) -> dict[str, list[str]]:
    p = project
    return {
        "publish": [
            f"mm.{p}.msg.*.*.{node}",              # send messages, only as this node
            f"$KV.mm_{p}_agents.{node}.>",         # own agent cards
            f"$KV.mm_{p}_nodes.{node}",            # own node card
            f"$KV.mm_{p}_tasks.{node}.>",          # own task records
            f"$O.mm_{p}_artifacts.>",              # artifact uploads
            "$JS.API.>",                           # stream/consumer/kv management (see module doc)
            "$JS.ACK.>",                           # acks for pulled messages
            "$JS.FC.>",                            # flow control for object-store reads
        ],
        # Replies (JetStream API answers and pulled mailbox messages) arrive on the node's own prefix
        # only; a shared _INBOX.> would let one node read every other node's mail.
        "subscribe": [f"{inbox_prefix(f'node_{node}')}.>"],
    }


def _perm_block(perms: dict[str, list[str]], indent: str) -> str:
    lines = [f"{indent}permissions: {{"]
    for kind, subjects in perms.items():
        quoted = ", ".join(f'"{s}"' for s in subjects)
        lines.append(f"{indent}  {kind}: {{ allow: [{quoted}] }}")
    lines.append(f"{indent}}}")
    return "\n".join(lines)


def render(project: str, nodes: list[str], passwords: dict[str, str], admin_password: str,
           *, port: int = 4222, monitor_port: int = 8222, store_dir: str = "/data/jetstream",
           listen_host: str = "0.0.0.0", tls: dict[str, Path] | None = None) -> str:
    check_token(project, "project")
    users = [f'    {{ user: "admin", password: "{admin_password}" }}']
    for node in nodes:
        check_token(node, "node id")
        users.append(f'    {{ user: "node_{node}", password: "{passwords[node]}",\n'
                     f'{_perm_block(node_permissions(project, node), "      ")} }}')
    tls_block = f"""
# TLS: every client connection is encrypted; nodes verify the server with ca.crt (tls_ca).
tls {{
  cert_file: "{tls['cert']}"
  key_file:  "{tls['key']}"
  timeout: 5
}}
""" if tls else """
# TLS is off: only acceptable when the listen address is a private overlay
# (Tailscale/WireGuard) or loopback. Re-run with --tls <public-ip-or-name> otherwise.
"""
    return f"""# mutmuas NATS server config — generated for project "{project}"
server_name: mutmuas-{project}
listen: {listen_host}:{port}
http: 127.0.0.1:{monitor_port}          # monitoring, local only

jetstream {{
  store_dir: "{store_dir}"
  max_file_store: 50G
}}
{tls_block}
authorization {{
  users: [
{(","+chr(10)).join(users)}
  ]
}}
"""


def _existing_password(path: Path) -> str | None:
    if not path.exists():
        return None
    for line in path.read_text().splitlines():
        if line.startswith("MUTMUAS_NATS_PASSWORD="):
            return line.split("=", 1)[1].strip()
    return None


def make_tls(out_dir: Path, hosts: list[str], project: str) -> dict[str, Path]:
    """Private CA + server certificate for ``hosts`` (IPs or DNS names), via the openssl CLI.

    Existing files are reused, so re-running server-config never invalidates ca.crt on the nodes.
    """
    tls = out_dir / "tls"
    tls.mkdir(parents=True, exist_ok=True)
    files = {"ca": tls / "ca.crt", "ca_key": tls / "ca.key", "cert": tls / "server.crt", "key": tls / "server.key"}
    if all(f.exists() for f in files.values()):
        return files

    def openssl(*args: str) -> None:
        out = subprocess.run(["openssl", *args], capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(f"openssl {args[0]} failed: {out.stderr.strip()}")

    sans = ",".join(f"IP:{h}" if _is_ip(h) else f"DNS:{h}" for h in hosts)
    # Extensions are required by strict verifiers (e.g. Python >= 3.13's default X509 strict mode).
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3650", "-subj", f"/CN=mutmuas-{project}-ca",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            "-addext", "subjectKeyIdentifier=hash", "-keyout", str(files["ca_key"]), "-out", str(files["ca"]))
    csr, ext = tls / "server.csr", tls / "server.ext"
    ext.write_text(f"subjectAltName={sans}\nextendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n"
                   "keyUsage=critical,digitalSignature,keyEncipherment\n"
                   "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid\n")
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN=mutmuas-{project}-server",
            "-keyout", str(files["key"]), "-out", str(csr))
    openssl("x509", "-req", "-in", str(csr), "-CA", str(files["ca"]), "-CAkey", str(files["ca_key"]),
            "-CAcreateserial", "-days", "825", "-extfile", str(ext), "-out", str(files["cert"]))
    csr.unlink()
    for key in ("ca_key", "key"):
        files[key].chmod(0o600)
    return files


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def generate(project: str, nodes: list[str], out_dir: Path, tls_hosts: list[str] | None = None,
             **kwargs) -> dict[str, Path]:
    """Write nats-server.conf plus one credentials file per node (mode 600).

    Re-running with an extra node keeps the passwords of nodes that already have a
    <NODE>.env in out_dir, so adding a machine never invalidates the others.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    passwords = {n: _existing_password(out_dir / f"{n}.env") or secrets.token_urlsafe(24) for n in nodes}
    admin = _existing_password(out_dir / "admin.env") or secrets.token_urlsafe(24)
    tls = make_tls(out_dir.resolve(), tls_hosts, project) if tls_hosts else None
    conf = out_dir / "nats-server.conf"
    conf.write_text(render(project, nodes, passwords, admin, tls=tls, **kwargs))
    conf.chmod(0o600)
    written = {"server": conf}
    for node, pw in {**passwords, "admin": admin}.items():
        user = "admin" if node == "admin" else f"node_{node}"
        path = out_dir / f"{node}.env"
        path.write_text(f"# credentials for {user}; copy to the node and keep it private\n"
                        f"MUTMUAS_NATS_USER={user}\nMUTMUAS_NATS_PASSWORD={pw}\n")
        path.chmod(0o600)
        written[node] = path
    if tls:
        written["ca"] = tls["ca"]
    return written
