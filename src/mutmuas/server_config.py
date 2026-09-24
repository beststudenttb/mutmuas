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

import secrets
from pathlib import Path

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
            "_INBOX.>",
        ],
        "subscribe": ["_INBOX.>"],
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
           listen_host: str = "0.0.0.0", tls: bool = False) -> str:
    check_token(project, "project")
    users = [f'    {{ user: "admin", password: "{admin_password}" }}']
    for node in nodes:
        check_token(node, "node id")
        users.append(f'    {{ user: "node_{node}", password: "{passwords[node]}",\n'
                     f'{_perm_block(node_permissions(project, node), "      ")} }}')
    tls_block = """
# TLS: required when the server is reachable outside a private overlay network.
tls {
  cert_file: "/etc/nats/certs/server.crt"
  key_file:  "/etc/nats/certs/server.key"
  ca_file:   "/etc/nats/certs/ca.crt"
  timeout: 2
}
""" if tls else """
# TLS is off: only acceptable when the listen address is a private overlay
# (Tailscale/WireGuard). Re-run with --tls to add a tls {} block.
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


def generate(project: str, nodes: list[str], out_dir: Path, **kwargs) -> dict[str, Path]:
    """Write nats-server.conf plus one credentials file per node (mode 600).

    Re-running with an extra node keeps the passwords of nodes that already have a
    <NODE>.env in out_dir, so adding a machine never invalidates the others.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    passwords = {n: _existing_password(out_dir / f"{n}.env") or secrets.token_urlsafe(24) for n in nodes}
    admin = _existing_password(out_dir / "admin.env") or secrets.token_urlsafe(24)
    conf = out_dir / "nats-server.conf"
    conf.write_text(render(project, nodes, passwords, admin, **kwargs))
    conf.chmod(0o600)
    written = {"server": conf}
    for node, pw in {**passwords, "admin": admin}.items():
        user = "admin" if node == "admin" else f"node_{node}"
        path = out_dir / f"{node}.env"
        path.write_text(f"# credentials for {user}; copy to the node and keep it private\n"
                        f"MUTMUAS_NATS_USER={user}\nMUTMUAS_NATS_PASSWORD={pw}\n")
        path.chmod(0o600)
        written[node] = path
    return written
