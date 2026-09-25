#!/usr/bin/env python3
"""Front desk (runs on the secretary's machine). Newcomers without a node identity talk to it through a
temporary mailbox (NATS user invite_<id>, inbox prefix _INV.<id>) created for them when the leader announces
a hire; after onboarding the temporary mailbox is deleted.

  desk.py serve  --state DIR --env reception.env --server URL --ca ca.crt
      answer enroll/status requests: queue them, bind each invite to the first public key it presents
  desk.py send   --state DIR --env reception.env --server URL --ca ca.crt --signer KEY --invite ID FILE...
      seal FILEs for that newcomer (their public key), sign, publish to _INV.<id>.bundle
  desk.py list   --state DIR
The desk only acknowledges; issuing credentials stays a secretary step (reviewed, backed up, reversible).
"""
import argparse, asyncio, json, os, ssl, sys, time
from pathlib import Path

import nats

sys.path.insert(0, str(Path(__file__).parent))
import crypto  # noqa: E402

SUBJECT = "mm.mutmuas.reception.in"


def read_env(path):
    return dict(l.strip().split("=", 1) for l in open(path) if "=" in l and not l.startswith("#"))


async def connect(a):
    env = read_env(a.env)
    tls = ssl.create_default_context(cafile=a.ca) if a.ca else None
    return await nats.connect(a.server, user=env.get("MUTMUAS_NATS_USER"), password=env.get("MUTMUAS_NATS_PASSWORD"), tls=tls,
                              name="mutmuas:reception", max_reconnect_attempts=-1)


def invites(state):
    """state/invites.tsv: <id> <node> <expires-epoch> <status: open|used|revoked>"""
    out = {}
    p = state / "invites.tsv"
    if p.exists():
        for line in p.read_text().splitlines():
            if line.strip() and not line.startswith("#"):
                i, node, exp, st = line.split("\t")[:4]
                out[i] = {"node": node, "expires": float(exp), "status": st}
    return out


async def serve(a):
    state = Path(a.state); (state / "requests").mkdir(parents=True, exist_ok=True)
    nc = await connect(a)

    async def on(msg):
        try:
            req = json.loads(msg.data)
            inv = invites(state).get(req.get("invite", ""))
            if not inv or inv["status"] != "open" or inv["expires"] < time.time():
                return                                   # silent: unknown, used or expired invites get nothing
            rec_path = state / "requests" / f"{req['invite']}.json"
            rec = json.loads(rec_path.read_text()) if rec_path.exists() else None
            if req.get("type") == "enroll":
                pub = req["pub"]
                if rec and rec["pub"] != pub:
                    await msg.respond(json.dumps({"status": "rejected", "why": "invite already bound to another key; "
                                                  "ask the leader for a new invite"}).encode()); return
                if not rec:
                    rec = {"invite": req["invite"], "node": inv["node"], "pub": pub, "fpr": crypto.fingerprint(pub),
                           "questionnaire": req.get("questionnaire"), "received": time.time(), "sent": None}
                    fd = os.open(rec_path, os.O_WRONLY | os.O_CREAT, 0o600)
                    with os.fdopen(fd, "w") as f: json.dump(rec, f, ensure_ascii=False, indent=1)
                    print(f"[desk] enroll {req['invite']} node={inv['node']} fpr={rec['fpr']}", flush=True)
                await msg.respond(json.dumps({"status": "queued", "fpr": rec["fpr"],
                                              "note": "the secretary will issue your node; keep enroll.py running"}).encode())
            elif req.get("type") == "status" and rec:
                await msg.respond(json.dumps({"status": "sent" if rec.get("sent") else "queued"}).encode())
                if rec.get("sent") and (state / "bundles" / f"{req['invite']}.json").exists():
                    await nc.publish(f"_INV.{req['invite']}.bundle", (state / "bundles" / f"{req['invite']}.json").read_bytes())
        except Exception as e:                          # never crash the desk on a bad request
            print(f"[desk] bad request: {e}", flush=True)

    await nc.subscribe(SUBJECT, cb=on)
    print(f"[desk] serving {SUBJECT}", flush=True)
    await asyncio.Event().wait()


async def send(a):
    state = Path(a.state); rec_path = state / "requests" / f"{a.invite}.json"
    rec = json.loads(rec_path.read_text())
    files = {Path(f).name: Path(f).read_bytes() for f in a.files}
    env = crypto.seal(files, rec["pub"], a.invite, crypto.load_signer(a.signer))
    (state / "bundles").mkdir(exist_ok=True)
    bp = state / "bundles" / f"{a.invite}.json"
    fd = os.open(bp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f: json.dump(env, f)
    nc = await connect(a)
    await nc.publish(f"_INV.{a.invite}.bundle", bp.read_bytes()); await nc.flush(); await nc.close()
    rec["sent"] = time.time(); rec_path.write_text(json.dumps(rec, ensure_ascii=False, indent=1))
    print(f"sent {sorted(files)} to invite {a.invite} (fpr {rec['fpr']})")


def main():
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("serve", "send", "list"):
        s = sub.add_parser(name); s.add_argument("--state", required=True)
        if name != "list":
            s.add_argument("--env"); s.add_argument("--server"); s.add_argument("--ca")
        if name == "send":
            s.add_argument("--signer", required=True); s.add_argument("--invite", required=True)
            s.add_argument("files", nargs="+")
    a = p.parse_args()
    if a.cmd == "list":
        for f in sorted(Path(a.state, "requests").glob("*.json")):
            r = json.loads(f.read_text()); print(r["invite"], r["node"], r["fpr"], "sent" if r["sent"] else "queued")
        return
    asyncio.run(serve(a) if a.cmd == "serve" else send(a))


if __name__ == "__main__":
    main()
