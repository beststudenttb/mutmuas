"""Identifiers: agent addresses (``NODE:agent``), message ids, task ids.

An address is the stable *logical* identity of an agent — the node it lives on
plus its role-like agent id (``B:representation``). Provider and model are
attributes of the agent, never part of its address, so swapping Opus for Fable
keeps the address (and its inbox) intact.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

# Addresses become NATS subject tokens, so only a conservative charset is allowed.
TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class InvalidAddress(ValueError):
    pass


def check_token(value: str, what: str) -> str:
    if not TOKEN_RE.match(value or ""):
        raise InvalidAddress(f"invalid {what} {value!r}: use 1-64 chars of [A-Za-z0-9_-]")
    return value


@dataclass(frozen=True, order=True)
class Address:
    node: str
    agent: str

    @classmethod
    def parse(cls, text: str) -> Address:
        if isinstance(text, Address):
            return text
        node, sep, agent = (text or "").partition(":")
        if not sep:
            raise InvalidAddress(f"invalid address {text!r}: expected NODE:agent, e.g. B:representation")
        return cls(check_token(node, "node id"), check_token(agent, "agent id"))

    def __str__(self) -> str:
        return f"{self.node}:{self.agent}"


def new_message_id() -> str:
    return f"msg-{uuid.uuid4().hex}"


def new_task_id() -> str:
    # Sortable-ish prefix makes task lists readable; uuid suffix makes it unique.
    return f"T-{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}"


def new_conversation_id() -> str:
    return f"conv-{uuid.uuid4().hex[:16]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text)
