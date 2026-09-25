#!/usr/bin/env python3
"""Newcomer side of the front desk. Run it with the invite the leader gave you:

  enroll.py --invite <id>:<code> --questionnaire q.yaml --out ~/mutmuas-join \
            [--server nats://150.89.170.193:4222] [--ca ca.crt]

It makes a key pair here (the private key never leaves this machine), sends your questionnaire and public key
to the secretary through your temporary mailbox, then waits. When the secretary has issued your node, it
receives the sealed bundle, checks the secretary's signature (secretary_ed25519.pub next to this file),
decrypts it and writes the files to --out (700 dir, 600 files). It never prints secrets.
"""
import argparse, asyncio, hashlib, json, os, ssl, sys
from pathlib import Path

import nats, yaml

sys.path.insert(0, str(Path(__file__).parent))
import crypto  # noqa: E402

HERE = Path(__file__).parent


async def run(a):
    inv, code = a.invite.split(":", 1)
    key, pub = crypto.new_x25519()
    print(f"your key fingerprint: {crypto.fingerprint(pub)} (tell the leader if asked)")
    tls = ssl.create_default_context(cafile=a.ca) if a.ca else None
    nc = await nats.connect(a.server, user=f"invite_{inv}", password=code, tls=tls, inbox_prefix=f"_INV.{inv}",
                            name=f"mutmuas:invite:{inv}", max_reconnect_attempts=-1)
    got = asyncio.get_running_loop().create_future()

    async def on_bundle(msg):
        try:
            files = crypto.open_sealed(json.loads(msg.data), key, a.secretary_pub, inv)
        except Exception as e:
            print(f"ignored a bundle that failed verification: {e}"); return
        if not got.done(): got.set_result(files)

    await nc.subscribe(f"_INV.{inv}.bundle", cb=on_bundle)
    q = yaml.safe_load(open(a.questionnaire)) if a.questionnaire else None
    for attempt in range(10):
        try:
            r = json.loads((await nc.request("mm.mutmuas.reception.in", json.dumps(
                {"type": "enroll", "invite": inv, "pub": pub, "questionnaire": q}).encode(), timeout=10)).data)
            print(f"desk: {r}"); break
        except Exception:
            print(f"desk not answering (attempt {attempt + 1}/10); retrying in 30s"); await asyncio.sleep(30)
    else:
        sys.exit("the desk did not answer; tell the leader")
    while not got.done():
        try:
            await asyncio.wait_for(asyncio.shield(got), a.poll)
        except asyncio.TimeoutError:
            try:
                await nc.request("mm.mutmuas.reception.in", json.dumps({"type": "status", "invite": inv}).encode(), timeout=10)
            except Exception:
                pass
    files = got.result()
    out = Path(os.path.expanduser(a.out)); out.mkdir(parents=True, exist_ok=True); os.chmod(out, 0o700)
    for name, data in files.items():
        fd = os.open(out / name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f: f.write(data)
        print(f"wrote {out / name}  sha256 {hashlib.sha256(data).hexdigest()[:16]}")
    await nc.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--invite", required=True); p.add_argument("--questionnaire"); p.add_argument("--out", required=True)
    p.add_argument("--server", default="nats://150.89.170.193:4222"); p.add_argument("--ca")
    p.add_argument("--poll", type=float, default=60)
    p.add_argument("--secretary-pub", default=None)
    a = p.parse_args()
    a.secretary_pub = a.secretary_pub or (HERE / "secretary_ed25519.pub").read_text().strip()
    asyncio.run(run(a))


if __name__ == "__main__":
    main()
