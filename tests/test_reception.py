"""Onboarding front desk: a temporary mailbox per newcomer that can only talk to the reception desk."""

import asyncio

import nats
import pytest
from conftest import NatsServer, free_port

from mutmuas.config import read_env_file
from mutmuas.server_config import generate, invite_bundle_subject, invite_inbox_prefix, reception_subject


def _creds(path):
    env = read_env_file(path)
    return env["MUTMUAS_NATS_USER"], env["MUTMUAS_NATS_PASSWORD"]


async def _connect(server, path, **kw):
    user, pw = _creds(path)
    errors = []

    async def on_error(e):
        errors.append(str(e).lower())
    nc = await nats.connect(server.url, user=user, password=pw, error_cb=on_error, allow_reconnect=False, **kw)
    return nc, errors


async def test_invite_can_only_reach_the_reception_desk(tmp_path):
    written = generate("p", ["A", "B"], tmp_path / "srv", listen_host="127.0.0.1", reception=True,
                       invites=["x1", "y2"], store_dir=str(tmp_path / "js"), monitor_port=free_port())
    with pytest.raises(ValueError):
        generate("p", ["A"], tmp_path / "bad", invites=["Bad_ID"])
    assert written["invite:x1"] == tmp_path / "srv" / "invites" / "x1.env"
    assert oct(written["invite:x1"].stat().st_mode)[-3:] == "600"
    server = NatsServer(tmp_path / "js", conf=written["server"])
    server.start()
    try:
        desk, desk_err = await _connect(server, written["reception"])

        async def answer(msg):
            await msg.respond(b"sealed-reply-for:" + msg.data)
        await desk.subscribe(reception_subject("p"), cb=answer)
        await desk.flush()

        guest, guest_err = await _connect(server, written["invite:x1"], inbox_prefix=invite_inbox_prefix("x1"))
        reply = await guest.request(reception_subject("p"), b"hello", timeout=5)
        assert reply.data == b"sealed-reply-for:hello"          # the one thing a guest can do

        other, other_err = await _connect(server, written["invite:y2"], inbox_prefix=invite_inbox_prefix("y2"))
        seen = []

        async def grab(m):
            seen.append(m.data)
        for subject in ("_INBOX.>", f"{invite_inbox_prefix('x1')}.>", "mm.p.>", ">"):
            await other.subscribe(subject, cb=grab)
        await other.publish("mm.p.msg.A.main.B", b"forged mail")     # cannot post into a mailbox
        await other.publish(f"{invite_inbox_prefix('x1')}.fake", b"forged reply")
        await other.flush()
        with pytest.raises(Exception):                                # no JetStream at all
            await other.jetstream().account_info()
        await guest.request(reception_subject("p"), b"again", timeout=5)
        bundles = []

        async def got_bundle(m):
            bundles.append(m.data)
        await guest.subscribe(invite_bundle_subject("x1"), cb=got_bundle)
        await guest.flush()
        await desk.publish(invite_bundle_subject("x1"), b"sealed-bundle")    # deliver after issuing
        await desk.flush()
        await asyncio.sleep(0.3)
        assert bundles == [b"sealed-bundle"]
        assert seen == []                                             # y2 saw neither x1's replies nor mail
        denied_subs = [e for e in other_err if "permissions violation for subscription" in e]
        denied_pubs = [e for e in other_err if "permissions violation for publish" in e]
        assert len(denied_subs) == 4 and len(denied_pubs) >= 2, other_err

        await desk.publish("mm.p.msg.A.main.B", b"desk cannot post mail")   # only replies are allowed
        await desk.flush()
        await asyncio.sleep(0.2)
        assert any("permissions violation for publish" in e for e in desk_err), desk_err
        for nc in (desk, guest, other):
            await nc.close()
    finally:
        server.stop()


async def test_dropping_an_invite_deletes_the_mailbox(tmp_path):
    out = tmp_path / "srv"
    first = generate("p", ["A"], out, listen_host="127.0.0.1", invites=["x1", "y2"],
                     store_dir=str(tmp_path / "js"), monitor_port=free_port())
    code_y2 = _creds(first["invite:y2"])[1]
    second = generate("p", ["A"], out, listen_host="127.0.0.1", invites=["y2"],
                      store_dir=str(tmp_path / "js"), monitor_port=free_port())
    assert not (out / "invites" / "x1.env").exists() and "invite:x1" not in second
    assert _creds(second["invite:y2"])[1] == code_y2                 # other invites keep their code
    conf = second["server"].read_text()
    assert "invite_x1" not in conf and "invite_y2" in conf
    assert _creds(second["A"]) == _creds(first["A"])                  # nodes are untouched
    server = NatsServer(tmp_path / "js", conf=second["server"])
    server.start()
    try:
        with pytest.raises(Exception):
            await nats.connect(server.url, user="invite_x1", password=_creds(first["invite:x1"])[1],
                               allow_reconnect=False, max_reconnect_attempts=1, connect_timeout=2)
    finally:
        server.stop()
