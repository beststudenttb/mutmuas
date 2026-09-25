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
import re
import secrets
import subprocess
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


def reception_subject(project: str) -> str:
    """Where newcomers without a node identity hand in their onboarding request (core NATS request-reply)."""
    return f"mm.{project}.reception.in"


def invite_user(invite_id: str) -> str:
    return f"invite_{invite_id}"


def invite_inbox_prefix(invite_id: str) -> str:
    """The newcomer's client must connect with this inbox_prefix: it may subscribe nowhere else. Its own
    token level (not _INV_<id>) so that the desk's permission _INV.*.bundle can match every invite."""
    return f"_INV.{invite_id}"


def invite_bundle_subject(invite_id: str) -> str:
    """Where the desk pushes the sealed credential bundle once the node identity is issued."""
    return f"{invite_inbox_prefix(invite_id)}.bundle"


INVITE_ID = re.compile(r"^[a-z0-9]{1,32}$")


def check_invite_id(invite_id: str) -> str:
    if not INVITE_ID.match(invite_id):
        raise ValueError(f"invalid invite id {invite_id!r}: lowercase letters and digits only (e.g. 8 of them)")
    return invite_id


def invite_permissions(project: str, invite_id: str) -> dict:
    """A temporary mailbox for one newcomer: hand a request to the reception desk and read its own
    replies. No JetStream, no registry, no other mailboxes. Deleted once the real node identity exists."""
    return {"publish": [reception_subject(project)],
            "subscribe": [f"{invite_inbox_prefix(invite_id)}.>"]}


def reception_permissions(project: str) -> dict:
    """The secretary's reception desk: receives newcomer requests and answers each once (allow_responses: the
    receipt), then pushes the sealed bundle to _INV.<id>.bundle when issuance is done. Nothing else, so it
    cannot touch mailboxes."""
    return {"publish": [invite_bundle_subject("*")], "subscribe": [reception_subject(project)],
            "allow_responses": {"max": 1, "expires": "1m"}}


def _perm_block(perms: dict, indent: str) -> str:
    lines = [f"{indent}permissions: {{"]
    for kind, value in perms.items():
        if kind == "allow_responses":
            lines.append(f'{indent}  allow_responses: {{ max: {value["max"]}, expires: "{value["expires"]}" }}')
        elif value:
            quoted = ", ".join(f'"{s}"' for s in value)
            lines.append(f"{indent}  {kind}: {{ allow: [{quoted}] }}")
        else:
            lines.append(f'{indent}  {kind}: {{ deny: [">"] }}')
    lines.append(f"{indent}}}")
    return "\n".join(lines)


def render(project: str, nodes: list[str], passwords: dict[str, str], admin_password: str,
           *, port: int = 4222, monitor_port: int = 8222, store_dir: str = "/data/jetstream",
           listen_host: str = "0.0.0.0", tls: dict[str, Path] | None = None,
           reception_password: str | None = None, invites: dict[str, str] | None = None) -> str:
    check_token(project, "project")
    users = [f'    {{ user: "admin", password: "{admin_password}" }}']
    for node in nodes:
        check_token(node, "node id")
        users.append(f'    {{ user: "node_{node}", password: "{passwords[node]}",\n'
                     f'{_perm_block(node_permissions(project, node), "      ")} }}')
    if reception_password:
        users.append(f'    {{ user: "reception", password: "{reception_password}",\n'
                     f'{_perm_block(reception_permissions(project), "      ")} }}')
    for invite_id, code in (invites or {}).items():
        check_invite_id(invite_id)
        users.append(f'    {{ user: "{invite_user(invite_id)}", password: "{code}",\n'
                     f'{_perm_block(invite_permissions(project, invite_id), "      ")} }}')
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
             reception: bool = False, invites: list[str] | None = None, **kwargs) -> dict[str, Path]:
    """Write nats-server.conf plus one credentials file per node (mode 600).

    Re-running with an extra node keeps the passwords of nodes that already have a
    <NODE>.env in out_dir, so adding a machine never invalidates the others.
    reception: also a user for the secretary's reception desk (reception.env).
    invites: temporary mailboxes, one per newcomer (invites/<id>.env; the password is the invite code the
    leader hands over). An invite left out of a later run is deleted: its user and its file are gone.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    passwords = {n: _existing_password(out_dir / f"{n}.env") or secrets.token_urlsafe(24) for n in nodes}
    admin = _existing_password(out_dir / "admin.env") or secrets.token_urlsafe(24)
    reception_pw = (_existing_password(out_dir / "reception.env") or secrets.token_urlsafe(24)) if reception else None
    invite_dir = out_dir / "invites"
    codes = {}
    for invite_id in invites or []:
        check_invite_id(invite_id)
        codes[invite_id] = _existing_password(invite_dir / f"{invite_id}.env") or secrets.token_urlsafe(24)
    tls = make_tls(out_dir.resolve(), tls_hosts, project) if tls_hosts else None
    conf = out_dir / "nats-server.conf"
    conf.write_text(render(project, nodes, passwords, admin, tls=tls, reception_password=reception_pw,
                           invites=codes, **kwargs))
    conf.chmod(0o600)
    written = {"server": conf}
    creds = {n: (f"node_{n}", pw, out_dir / f"{n}.env") for n, pw in passwords.items()}
    creds["admin"] = ("admin", admin, out_dir / "admin.env")
    if reception_pw:
        creds["reception"] = ("reception", reception_pw, out_dir / "reception.env")
    if codes:
        invite_dir.mkdir(mode=0o700, exist_ok=True)
    for invite_id, code in codes.items():
        creds[f"invite:{invite_id}"] = (invite_user(invite_id), code, invite_dir / f"{invite_id}.env")
    for stale in (invite_dir.glob("*.env") if invite_dir.exists() else []):
        if stale.stem not in codes:
            stale.unlink()
    for name, (user, pw, path) in creds.items():
        path.write_text(f"# credentials for {user}; copy to the node and keep it private\n"
                        f"MUTMUAS_NATS_USER={user}\nMUTMUAS_NATS_PASSWORD={pw}\n")
        path.chmod(0o600)
        written[name] = path
    if tls:
        written["ca"] = tls["ca"]
    return written
