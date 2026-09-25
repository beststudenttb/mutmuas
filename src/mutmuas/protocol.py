"""Message protocol: a fixed envelope plus per-type body validation.

Messages are structured, never free chat. Every message is an ``Envelope``
serialised as JSON. Large payloads never travel in messages — only
``ArtifactRef`` references (see ``artifacts.py``).

See docs/MESSAGE_PROTOCOL.md for the human-readable spec.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from . import PROTOCOL_VERSION
from .ids import Address, InvalidAddress, new_conversation_id, new_message_id, now_iso, parse_iso


class ProtocolError(ValueError):
    """The message is malformed. Carries a short machine-readable code."""

    def __init__(self, message: str, code: str = "invalid_message"):
        super().__init__(message)
        self.code = code


MESSAGE_TYPES = (
    "REQUEST",   # ask another agent to do something (creates a task)
    "ACK",       # owner accepted the task into its queue
    "QUESTION",  # needs information from the other side
    "ANSWER",    # reply to a QUESTION
    "UPDATE",    # progress / state change on a task
    "RESULT",    # final outcome: complete | partial | failed
    "BLOCKED",   # owner cannot proceed without something
    "REJECT",    # owner refuses the task (permission, capability, policy)
    "CANCEL",    # requester withdraws the task
    "ERROR",     # infrastructure/protocol level failure
)

PRIORITIES = ("low", "normal", "high")
RESULT_STATUSES = ("complete", "partial", "failed")

# Task lifecycle. Terminal states never transition again.
TASK_STATES = ("PENDING", "ACCEPTED", "RUNNING", "WAITING", "BLOCKED", "COMPLETED", "FAILED", "CANCELLED")
TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})

# What kind of work a REQUEST asks for; each maps to the permission the owner must hold.
REQUEST_KINDS = {
    "query": "READ",                  # answer a question / look something up
    "artifact": "PUBLISH_ARTIFACT",   # find or produce data and hand it back
    "experiment": "RUN_EXPERIMENT",   # run a test / training / evaluation
    "code": "WRITE_WORKTREE",         # change code in an isolated worktree
}

# Required body fields per type. Optional fields are documented in MESSAGE_PROTOCOL.md.
_REQUIRED_BODY = {
    "REQUEST": ("objective", "reason"),
    "ACK": (),
    "QUESTION": ("question",),
    "ANSWER": ("answer",),
    "UPDATE": ("message",),
    "RESULT": ("status", "summary"),
    "BLOCKED": ("reason",),
    "REJECT": ("reason",),
    "CANCEL": (),
    "ERROR": ("code", "message"),
}

# Everything except a REQUEST refers to an existing task.
_NEEDS_TASK = frozenset(MESSAGE_TYPES) - {"REQUEST", "ERROR"}


@dataclass
class ArtifactRef:
    """A pointer to data that lives outside the message bus."""

    uri: str                      # artifact://proj/key | file:///abs/path | git://repo@sha | https://...
    id: str = ""                  # short human label, e.g. EXP092-METRICS
    size: int | None = None
    sha256: str | None = None
    media_type: str | None = None
    description: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ArtifactRef:
        if not isinstance(d, dict) or not isinstance(d.get("uri"), str) or not d["uri"]:
            raise ProtocolError("artifact reference needs a non-empty 'uri'")
        known = {k: d[k] for k in cls.__dataclass_fields__ if k in d}
        return cls(**known)

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v not in (None, "")}


@dataclass
class Envelope:
    type: str
    sender: str                   # serialised as "from"
    to: str
    body: dict[str, Any] = field(default_factory=dict)
    task_id: str | None = None
    conversation_id: str = field(default_factory=new_conversation_id)
    message_id: str = field(default_factory=new_message_id)
    timestamp: str = field(default_factory=now_iso)
    priority: str = "normal"
    artifacts: list[ArtifactRef] = field(default_factory=list)
    reply_to: str | None = None
    protocol: str = PROTOCOL_VERSION

    # ---- construction -------------------------------------------------

    @classmethod
    def reply(cls, original: Envelope, type: str, body: dict[str, Any] | None = None,
              artifacts: list[ArtifactRef] | None = None, sender: str | None = None) -> Envelope:
        """Build a message answering ``original`` in the same task/conversation."""
        return cls(
            type=type,
            sender=sender or original.to,
            to=original.sender,
            body=body or {},
            task_id=original.task_id,
            conversation_id=original.conversation_id,
            priority=original.priority,
            artifacts=artifacts or [],
            reply_to=original.message_id,
        )

    # ---- (de)serialisation -------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "task_id": self.task_id,
            "from": self.sender,
            "to": self.to,
            "type": self.type,
            "timestamp": self.timestamp,
            "priority": self.priority,
            "body": self.body,
            "artifacts": [a.to_dict() for a in self.artifacts],
            "reply_to": self.reply_to,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False).encode()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Envelope:
        if not isinstance(d, dict):
            raise ProtocolError("message must be a JSON object")
        for key in ("message_id", "from", "to", "type"):
            if not isinstance(d.get(key), str) or not d[key]:
                raise ProtocolError(f"missing or invalid field {key!r}")
        body = d.get("body") or {}
        if not isinstance(body, dict):
            raise ProtocolError("'body' must be an object")
        artifacts = d.get("artifacts") or []
        if not isinstance(artifacts, list):
            raise ProtocolError("'artifacts' must be a list")
        env = cls(
            type=d["type"],
            sender=d["from"],
            to=d["to"],
            body=body,
            task_id=d.get("task_id"),
            conversation_id=d.get("conversation_id") or new_conversation_id(),
            message_id=d["message_id"],
            timestamp=d.get("timestamp") or now_iso(),
            priority=d.get("priority") or "normal",
            artifacts=[ArtifactRef.from_dict(a) for a in artifacts],
            reply_to=d.get("reply_to"),
            protocol=d.get("protocol") or PROTOCOL_VERSION,
        )
        env.validate()
        return env

    @classmethod
    def from_json(cls, data: bytes | str) -> Envelope:
        try:
            d = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ProtocolError(f"not valid JSON: {e}") from e
        return cls.from_dict(d)

    # ---- validation ---------------------------------------------------

    def validate(self) -> Envelope:
        if self.protocol.split("/")[0] != PROTOCOL_VERSION.split("/")[0]:
            raise ProtocolError(f"unsupported protocol {self.protocol!r}", "unsupported_protocol")
        if self.type not in MESSAGE_TYPES:
            raise ProtocolError(f"unknown message type {self.type!r}", "unknown_type")
        try:
            Address.parse(self.sender)
            Address.parse(self.to)
        except InvalidAddress as e:
            raise ProtocolError(str(e), "invalid_address") from e
        if self.priority not in PRIORITIES:
            raise ProtocolError(f"priority must be one of {PRIORITIES}")
        if self.type in _NEEDS_TASK and not self.task_id:
            raise ProtocolError(f"{self.type} must carry a task_id")
        missing = [k for k in _REQUIRED_BODY[self.type] if self.body.get(k) in (None, "")]
        if missing:
            raise ProtocolError(f"{self.type} body is missing required field(s): {', '.join(missing)}")
        if self.type == "REQUEST":
            kind = self.body.get("kind", "query")
            if kind not in REQUEST_KINDS:
                raise ProtocolError(f"REQUEST kind must be one of {sorted(REQUEST_KINDS)}")
            timeout = self.body.get("timeout_s")
            if timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0):
                raise ProtocolError("timeout_s must be a positive number")
            if self.body.get("reply", "required") not in REPLY_MODES:
                raise ProtocolError(f"REQUEST reply must be one of {REPLY_MODES}")
            if self.body.get("deadline") is not None:
                try:
                    if parse_iso(self.body["deadline"]).tzinfo is None:
                        raise ValueError("no timezone")
                except (TypeError, ValueError) as e:
                    raise ProtocolError("deadline must be an ISO 8601 time with a timezone") from e
        if self.body.get("next") is not None:
            # "who moves next": the baton in a thread. Whoever it names is woken like by a REQUEST.
            try:
                Address.parse(self.body["next"])
            except (InvalidAddress, TypeError) as e:
                raise ProtocolError(f"next must be an agent address, got {self.body['next']!r}") from e
        if self.type == "RESULT" and self.body["status"] not in RESULT_STATUSES:
            raise ProtocolError(f"RESULT status must be one of {RESULT_STATUSES} (never report partial as complete)")
        if self.type == "UPDATE" and self.body.get("state") not in (None, *TASK_STATES):
            raise ProtocolError(f"UPDATE state must be one of {TASK_STATES}")
        return self

    # ---- convenience --------------------------------------------------

    @property
    def sender_addr(self) -> Address:
        return Address.parse(self.sender)

    @property
    def to_addr(self) -> Address:
        return Address.parse(self.to)

    def short(self) -> str:
        return f"{self.type} {self.sender}->{self.to} task={self.task_id} id={self.message_id}"


# Like an email's "reply requested" flag. required (the default): the task stays open until the owner
# answers with a RESULT. none: a notice; the receiving session reading it is the answer (read receipt).
REPLY_MODES = ("required", "none")


def reply_required(request: dict[str, Any] | None) -> bool:
    return (request or {}).get("reply", "required") != "none"


def request_body(objective: str, reason: str, *, kind: str = "query", inputs: Any = None,
                 expected_outputs: Any = None, constraints: Any = None, acceptance_criteria: Any = None,
                 deadline: str | None = None, timeout_s: float | None = None,
                 reply: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"objective": objective, "reason": reason, "kind": kind}
    for key, value in (("inputs", inputs), ("expected_outputs", expected_outputs), ("constraints", constraints),
                       ("acceptance_criteria", acceptance_criteria), ("deadline", deadline), ("timeout_s", timeout_s),
                       ("reply", reply)):
        if value not in (None, "", [], {}):
            body[key] = value
    return body


def result_body(status: str, summary: str, *, outputs: Any = None, evidence: Any = None,
                limitations: Any = None, follow_up: Any = None) -> dict[str, Any]:
    if status not in RESULT_STATUSES:
        raise ProtocolError(f"RESULT status must be one of {RESULT_STATUSES}")
    body: dict[str, Any] = {"status": status, "summary": summary}
    for key, value in (("outputs", outputs), ("evidence", evidence), ("limitations", limitations),
                       ("follow_up", follow_up)):
        if value not in (None, "", [], {}):
            body[key] = value
    return body


def task_state_for_result(status: str) -> str:
    """complete/partial finish the task; failed fails it. Partial stays visible via result_status."""
    return "FAILED" if status == "failed" else "COMPLETED"
