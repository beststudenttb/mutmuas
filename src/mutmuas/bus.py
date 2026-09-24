"""The messaging bus: NATS + JetStream, and the only module that knows subject names.

Topology for project ``p`` (all created idempotently by whoever connects first):

  stream  MM_p_MSG          subjects mm.p.msg.<to_node>.<to_agent>.<from_node>
                            limits retention (= durable history / audit log),
                            dedup window keyed on Nats-Msg-Id = message_id
  consumer inbox_<node>_<agent>   durable pull consumer per agent = its mailbox
  kv      mm_p_agents       key <node>.<agent>  -> AgentCard (registry + presence)
  kv      mm_p_nodes        key <node>          -> NodeCard
  kv      mm_p_tasks        key <node>.<agent>.<task_id> -> TaskRecord (written by the owner only)
  objects mm_p_artifacts    artifact payloads (small/medium files)

The sender's node id is part of the subject so the server can enforce
"node A may only publish as A" with per-user publish permissions
(see ``server_config.py``); receivers check it against the envelope.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.js import api
from nats.js.errors import BucketNotFoundError, KeyNotFoundError, NoKeysError, NotFoundError

from .config import NatsConfig
from .ids import Address
from .protocol import Envelope

log = logging.getLogger(__name__)

DUPLICATE_WINDOW_S = 600       # resend of the same message_id within this window is dropped server-side
ACK_WAIT_S = 30                # node must commit a message to its local ledger within this time


class BusUnavailable(RuntimeError):
    """NATS could not be reached. Outbound messages stay in the local outbox."""


class Names:
    def __init__(self, project: str):
        self.project = project
        self.stream = f"MM_{project}_MSG"
        self.msg_prefix = f"mm.{project}.msg"
        self.agents_kv = f"mm_{project}_agents"
        self.nodes_kv = f"mm_{project}_nodes"
        self.tasks_kv = f"mm_{project}_tasks"
        self.artifacts = f"mm_{project}_artifacts"

    def inbox_subject(self, to: Address, from_node: str) -> str:
        return f"{self.msg_prefix}.{to.node}.{to.agent}.{from_node}"

    def inbox_filter(self, agent: Address) -> str:
        return f"{self.msg_prefix}.{agent.node}.{agent.agent}.*"

    @staticmethod
    def consumer(agent: Address) -> str:
        return f"inbox_{agent.node}_{agent.agent}"

    @staticmethod
    def sender_node_from_subject(subject: str) -> str:
        return subject.rsplit(".", 1)[-1]

    @staticmethod
    def task_key(owner: Address, task_id: str) -> str:
        return f"{owner.node}.{owner.agent}.{task_id}"


async def connect(cfg: NatsConfig, name: str, *, reconnect: bool = True, connect_timeout: float = 5) -> NATS:
    options: dict[str, Any] = {
        "servers": cfg.servers,
        "name": name,
        "connect_timeout": connect_timeout,
        "allow_reconnect": reconnect,
        # nats-py treats 0 as "unlimited"; one attempt per server is what a CLI call wants.
        "max_reconnect_attempts": -1 if reconnect else 1,
        "reconnect_time_wait": 1,
    }
    if cfg.user:
        options["user"] = cfg.user
        options["password"] = cfg.resolved_password()
    elif cfg.resolved_token():
        options["token"] = cfg.resolved_token()
    if cfg.tls_ca or cfg.tls_cert:
        ctx = ssl.create_default_context(cafile=cfg.tls_ca)
        if cfg.tls_cert:
            ctx.load_cert_chain(cfg.tls_cert, cfg.tls_key)
        options["tls"] = ctx

    holder: dict[str, NATS] = {}

    def _closing() -> bool:
        nc = holder.get("nc")
        return nc is not None and (nc.is_closed or nc.is_draining)

    async def _error_cb(e: Exception) -> None:
        if not _closing() and holder.get("nc") is not None:   # initial connect failures are raised instead
            log.warning("nats: %s", e)

    async def _disconnected_cb() -> None:
        if not _closing():
            log.warning("nats: disconnected (will keep retrying; outbound messages wait in the outbox)")

    async def _reconnected_cb() -> None:
        log.info("nats: reconnected")

    options.update(error_cb=_error_cb, disconnected_cb=_disconnected_cb, reconnected_cb=_reconnected_cb)
    try:
        holder["nc"] = await nats.connect(**options)
        return holder["nc"]
    except Exception as e:  # nats raises a zoo of exception types for "cannot connect"
        raise BusUnavailable(f"cannot reach NATS at {cfg.servers}: {e}") from e


class Bus:
    """Thin, replaceable wrapper. Everything above this module speaks Envelopes and dicts."""

    def __init__(self, nc: NATS, project: str, retention_days: float = 30):
        self.nc = nc
        self.js = nc.jetstream()
        self.names = Names(project)
        self.retention_days = retention_days
        self._kv: dict[str, Any] = {}
        self._obs = None

    @classmethod
    async def open(cls, cfg: NatsConfig, project: str, name: str, retention_days: float = 30,
                   reconnect: bool = True) -> Bus:
        bus = cls(await connect(cfg, name, reconnect=reconnect), project, retention_days)
        await bus.ensure_topology()
        return bus

    async def close(self) -> None:
        if self.nc.is_closed:
            return
        try:
            if self.nc.is_connected:
                await asyncio.wait_for(self.nc.drain(), 5)
                return
        except (Exception, asyncio.CancelledError):   # nats-py surfaces lost connections as CancelledError here
            pass
        try:
            await self.nc.close()
        except Exception:
            pass

    @property
    def connected(self) -> bool:
        return self.nc.is_connected

    # ---- topology -----------------------------------------------------

    async def ensure_topology(self) -> None:
        n = self.names
        stream_cfg = api.StreamConfig(
            name=n.stream,
            subjects=[f"{n.msg_prefix}.>"],
            retention=api.RetentionPolicy.LIMITS,
            storage=api.StorageType.FILE,
            max_age=self.retention_days * 86400,
            duplicate_window=DUPLICATE_WINDOW_S,
            discard=api.DiscardPolicy.OLD,
        )
        try:
            await self.js.stream_info(n.stream)
        except NotFoundError:
            await self.js.add_stream(stream_cfg)
        for bucket, history in ((n.agents_kv, 1), (n.nodes_kv, 1), (n.tasks_kv, 5)):
            self._kv[bucket] = await self._ensure_kv(bucket, history)
        try:
            self._obs = await self.js.object_store(n.artifacts)
        except (BucketNotFoundError, NotFoundError):
            self._obs = await self.js.create_object_store(n.artifacts, config=api.ObjectStoreConfig(
                storage=api.StorageType.FILE, description="mutmuas artifacts"))

    async def _ensure_kv(self, bucket: str, history: int):
        try:
            return await self.js.key_value(bucket)
        except (BucketNotFoundError, NotFoundError):
            return await self.js.create_key_value(api.KeyValueConfig(
                bucket=bucket, history=history, storage=api.StorageType.FILE))

    async def ensure_inbox(self, agent: Address):
        """Create (idempotently) the durable mailbox for ``agent`` and bind a pull subscription."""
        durable = self.names.consumer(agent)
        try:
            await self.js.consumer_info(self.names.stream, durable)
        except NotFoundError:
            await self.js.add_consumer(self.names.stream, api.ConsumerConfig(
                durable_name=durable,
                filter_subject=self.names.inbox_filter(agent),
                deliver_policy=api.DeliverPolicy.ALL,
                ack_policy=api.AckPolicy.EXPLICIT,
                ack_wait=ACK_WAIT_S,
                max_deliver=-1,
                description=f"mailbox of {agent}",
            ))
        return await self.js.pull_subscribe_bind(durable=durable, stream=self.names.stream)

    async def inbox_pending(self, agent: Address) -> int | None:
        try:
            info = await self.js.consumer_info(self.names.stream, self.names.consumer(agent))
            return info.num_pending + info.num_ack_pending
        except NotFoundError:
            return None

    # ---- messages -----------------------------------------------------

    async def publish(self, env: Envelope, timeout: float = 5) -> bool:
        """Publish durably. Returns True if the server already had this message_id (dedup)."""
        subject = self.names.inbox_subject(env.to_addr, env.sender_addr.node)
        ack = await self.js.publish(subject, env.to_json(), timeout=timeout,
                                    headers={"Nats-Msg-Id": env.message_id})
        return bool(ack.duplicate)

    async def history(self, subject_filter: str | None = None, limit: int = 500) -> list[tuple[str, bytes]]:
        """Read raw messages back from the stream (audit). Uses an ephemeral ordered consumer."""
        subject_filter = subject_filter or f"{self.names.msg_prefix}.>"
        out: list[tuple[str, bytes]] = []
        sub = await self.js.subscribe(subject_filter, ordered_consumer=True,
                                      deliver_policy=api.DeliverPolicy.ALL)
        try:
            while len(out) < limit:
                try:
                    msg = await sub.next_msg(timeout=0.5)
                except TimeoutError:
                    break
                except Exception as e:  # nats.errors.TimeoutError is not always builtins.TimeoutError
                    if "Timeout" in type(e).__name__:
                        break
                    raise
                out.append((msg.subject, msg.data))
        finally:
            await sub.unsubscribe()
        return out

    # ---- key/value helpers --------------------------------------------

    def kv(self, bucket: str):
        return self._kv[bucket]

    async def kv_put(self, bucket: str, key: str, value: dict[str, Any]) -> int:
        return await self.kv(bucket).put(key, json.dumps(value, ensure_ascii=False).encode())

    async def kv_get(self, bucket: str, key: str) -> dict[str, Any] | None:
        try:
            entry = await self.kv(bucket).get(key)
        except KeyNotFoundError:
            return None
        if entry is None or not entry.value:
            return None
        return json.loads(entry.value)

    async def kv_keys(self, bucket: str, filters: list[str] | None = None) -> list[str]:
        # nats-py 2.16's server-side key filters return nothing, so filter client-side (fine at Phase 1 scale).
        try:
            keys = list(await self.kv(bucket).keys())
        except NoKeysError:
            return []
        if filters:
            keys = [k for k in keys if any(_subject_match(f, k) for f in filters)]
        return keys

    async def kv_all(self, bucket: str, filters: list[str] | None = None) -> dict[str, dict[str, Any]]:
        out = {}
        for key in await self.kv_keys(bucket, filters):
            value = await self.kv_get(bucket, key)
            if value is not None:
                out[key] = value
        return out

    async def kv_delete(self, bucket: str, key: str) -> None:
        await self.kv(bucket).delete(key)

    # ---- object store -------------------------------------------------

    @property
    def objects(self):
        return self._obs


def _subject_match(pattern: str, subject: str) -> bool:
    """NATS wildcard semantics: '*' matches one token, '>' the rest."""
    p, s = pattern.split("."), subject.split(".")
    for i, token in enumerate(p):
        if token == ">":
            return len(s) > i
        if i >= len(s) or (token != "*" and token != s[i]):
            return False
    return len(p) == len(s)
