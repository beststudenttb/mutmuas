import pytest

from mutmuas.ids import Address, InvalidAddress
from mutmuas.protocol import ArtifactRef, Envelope, ProtocolError, request_body, result_body


def req(**kw):
    return Envelope(type="REQUEST", sender="A:main", to="B:lab",
                    body=request_body("return exp 82 latents", "need them for analysis", kind="artifact"),
                    task_id="T-1", **kw)


def test_roundtrip_preserves_fields():
    env = req(artifacts=[ArtifactRef(uri="artifact://p/x.json", id="X", sha256="ab")])
    back = Envelope.from_json(env.to_json())
    assert back.to_dict() == env.to_dict()
    assert back.to_dict()["from"] == "A:main"


def test_address_parsing():
    assert Address.parse("B:representation") == Address("B", "representation")
    for bad in ("B", "B:", ":x", "B:x.y", "B:x y", "B:*"):
        with pytest.raises(InvalidAddress):
            Address.parse(bad)


@pytest.mark.parametrize("mutate, code", [
    (lambda d: d.update(type="CHAT"), "unknown_type"),
    (lambda d: d.update(to="nobody"), "invalid_address"),
    (lambda d: d["body"].pop("reason"), "invalid_message"),
    (lambda d: d["body"].update(kind="teleport"), "invalid_message"),
    (lambda d: d.update(protocol="other/1"), "unsupported_protocol"),
])
def test_invalid_messages_are_rejected(mutate, code):
    d = req().to_dict()
    mutate(d)
    with pytest.raises(ProtocolError) as e:
        Envelope.from_dict(d)
    assert e.value.code == code


def test_not_json():
    with pytest.raises(ProtocolError):
        Envelope.from_json(b"\xff not json")


def test_result_status_must_be_honest_enum():
    with pytest.raises(ProtocolError):
        result_body("mostly-done", "x")
    env = Envelope(type="RESULT", sender="B:lab", to="A:main", task_id="T-1",
                   body={"status": "done", "summary": "x"})
    with pytest.raises(ProtocolError):
        env.validate()


def test_replies_need_task_id():
    with pytest.raises(ProtocolError):
        Envelope(type="UPDATE", sender="B:lab", to="A:main", body={"message": "hi"}).validate()


def test_reply_threads_conversation():
    original = req()
    ack = Envelope.reply(original, "ACK", {"state": "ACCEPTED"})
    assert (ack.sender, ack.to, ack.task_id, ack.conversation_id, ack.reply_to) == (
        "B:lab", "A:main", "T-1", original.conversation_id, original.message_id)
