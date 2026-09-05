"""Headless PeerChat peer: one process per StatefulSet pod.

Runs the existing ``BroadcastNode`` with two deployment-specific inputs —
a stable identity (:mod:`deploy.identity`) and DNS-backed discovery
(:mod:`deploy.dns_registry`) — plus a small HTTP control surface the
verification harness drives.

The causal path is untouched. This module only observes it: ``on_message``
appends to a delivery log, which is exactly the sequence ``BroadcastNode``
chose to deliver in, stamped with the vector clock that came with the message.

Control API
-----------
``GET  /healthz``     process is up (liveness)
``GET  /readyz``      200 only after this peer has joined (readiness)
``GET  /status``      identity, peer set, counters
``POST /send``        originate N messages  {"count": int, "rate": float}
``GET  /deliveries``  the local delivery log, in delivery order
``GET  /sent``        messages originated here
``POST /reset``       clear both logs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from distribution.broadcast_node import BroadcastNode
from distribution.message import Message
from deploy.dns_registry import DnsPeerRegistry
from deploy.identity import identity_from_env

try:
    import websockets
except ModuleNotFoundError:  # pragma: no cover
    websockets = None

logger = logging.getLogger("peerchat.peer")

JOIN_PROBE_INTERVAL = 2.0
JOIN_PROBE_TIMEOUT = 2.0
HOLDBACK_SAMPLE_INTERVAL = 0.25
# Recovery is quiescence-based: peers answer a recover_request with a stream of
# history_chunks, and there is no single "all peers done" signal. Ready when no
# chunk has landed for SETTLE seconds, capped at MAX.
RECOVERY_SETTLE = 3.0
RECOVERY_MAX = 30.0
# How many stored message IDs to re-seed into the node's dedup set on restart.
SEEN_SEED_LIMIT = 50_000


def short_key(address: str) -> str:
    """Compress a vector-clock key to the pod short name.

    ``peerchat-3.peerchat-hl.peerchat.svc.cluster.local:5678`` -> ``peerchat-3``.

    The map is one-to-one (pod names are unique within a StatefulSet), so the
    causal check is unaffected; it keeps a 10,000-message log from carrying
    ten copies of a 50-character FQDN on every single record.
    """
    return address.split(".", 1)[0]


def short_vc(vc: dict) -> dict:
    return {short_key(k): v for k, v in (vc or {}).items()}


class PeerRuntime:
    """Owns the BroadcastNode, the delivery log, and the join state."""

    def __init__(self, identity, replicas: int, min_peers: int,
                 history_dir: str | None = None) -> None:
        self.identity = identity
        self.replicas = replicas
        self.min_peers = min_peers

        self.registry = DnsPeerRegistry(identity, replicas=replicas)
        self.node = BroadcastNode(
            host=identity.fqdn,
            port=identity.chat_port,
            peer_registry=self.registry,
        )
        self.node.on_message = self._on_message

        self._log_lock = threading.Lock()
        self._deliveries: list[dict] = []
        self._sent: list[dict] = []
        # The vector clock as of the moment the delivery log started. Empty at
        # process start; re-captured by /reset. The offline checker replays
        # from here — without it, clearing the log between runs on a live
        # cluster makes every first delivery look like a causal violation
        # because the clock kept advancing while the log did not.
        self._baseline_vc: dict = {}

        self._joined = threading.Event()
        self._joined_at: float | None = None
        self._history = None
        self._history_dir = history_dir
        self._restored_vc: dict = {}
        self._recovery_requested_at: float | None = None
        self._recovery_last_chunk_at: float | None = None
        self._recovery_chunks = 0
        self._recovery_took_s: float | None = None
        self._deliveries_before_ready = 0
        self._seen_seeded = 0
        self._started_at = time.time()
        self._probe_stop = threading.Event()
        self._probe_thread: threading.Thread | None = None
        self._reachable: list[str] = []

        # Hold-back occupancy, sampled rather than instrumented: the queue is
        # read-only here so HoldBackQueue itself stays exactly as written.
        # After a restart a peer's clock is empty, so inbound traffic piles up
        # here until it either becomes deliverable or ages past the 5s timeout;
        # this is what the chaos run measures.
        self._hb_thread: threading.Thread | None = None
        self._hb_peak = 0
        self._hb_first_nonzero: float | None = None
        self._hb_last_nonzero: float | None = None
        self._hb_samples = 0

    # ── Delivery observation ────────────────────────────────────────────────

    def _on_message(self, msg: Message) -> None:
        """Persist, then record the delivery in the order BroadcastNode chose.

        History sees every delivered message first: it stores chat messages
        (so the clock survives a restart) and absorbs recovery frames, which
        are transport plumbing rather than chat deliveries and must not enter
        the causal log.
        """
        if self._history is not None:
            try:
                result = self._history.handle_message(msg)
            except Exception as exc:
                logger.warning("history.handle_message failed for %s: %s", msg.id[:8], exc)
                result = {}
            if result.get("handled"):
                if result.get("type") == "history_chunk":
                    self._recovery_chunks += 1
                    self._recovery_last_chunk_at = time.time()
                return

        now = time.time()
        with self._log_lock:
            self._deliveries.append(
                {
                    "seq": len(self._deliveries),
                    "id": msg.id,
                    "sender": short_key(msg.sender),
                    "vc": short_vc(msg.vector_clock),
                    "ts_recv": now,
                    "ts_send": msg.timestamp,
                }
            )

    # ── Sending ─────────────────────────────────────────────────────────────

    def send(self, count: int, rate: float | None, prefix: str) -> dict:
        """Originate ``count`` messages from this peer.

        ``rate`` (messages/second) paces origination. The existing
        ``BroadcastNode.broadcast`` is fire-and-forget onto its own event
        loop, so pacing here bounds how much work is queued at once.
        """
        interval = (1.0 / rate) if rate and rate > 0 else 0.0
        started = time.time()
        for i in range(count):
            msg = Message(
                content=f"{prefix}:{self.identity.pod_name}:{i}",
                sender=self.node.address,
            )
            with self._log_lock:
                self._sent.append({"id": msg.id, "ts_send": msg.timestamp, "n": i})
            self.node.broadcast(msg)
            if interval:
                time.sleep(interval)
        return {
            "sent": count,
            "started": started,
            "finished": time.time(),
            "peer": self.identity.pod_name,
        }

    # ── Join / readiness ────────────────────────────────────────────────────

    def start(self) -> None:
        if self._history_dir:
            # wire_node() merges the persisted vector clock into node._vc, so a
            # restarted peer is already at the clock it had when it died —
            # provided the store is on a volume that outlived the pod. That is
            # what the StatefulSet's volumeClaimTemplate is for.
            from message_history.storage import HistoryService

            self._history = HistoryService(
                node=self.node,
                host=self.identity.fqdn,
                port=self.identity.chat_port,
                storage_root=self._history_dir,
            )
            self._history.start()
            self._restored_vc = short_vc(self.node._vc.snapshot())
            self._seed_seen_set()
            logger.info(
                "history store %s — restored clock with %d entries",
                self._history_dir, len(self._restored_vc),
            )

        self.node.start()
        self._probe_thread = threading.Thread(target=self._probe_loop, daemon=True)
        self._probe_thread.start()
        self._hb_thread = threading.Thread(target=self._holdback_sampler, daemon=True)
        self._hb_thread.start()

    def _holdback_sampler(self) -> None:
        while not self._probe_stop.is_set():
            depth = len(self.node._hold_back._queue)
            self._hb_samples += 1
            if depth:
                now = time.time()
                if self._hb_first_nonzero is None:
                    self._hb_first_nonzero = now
                self._hb_last_nonzero = now
                self._hb_peak = max(self._hb_peak, depth)
            self._probe_stop.wait(HOLDBACK_SAMPLE_INTERVAL)

    def recovery_stats(self) -> dict:
        """Timings a restarted peer needs to report: rejoin, then hold-back drain."""
        first = self._hb_first_nonzero
        last = self._hb_last_nonzero
        depth_now = len(self.node._hold_back._queue)
        return {
            "pod": self.identity.pod_name,
            "uptime_s": round(time.time() - self._started_at, 3),
            "rejoin_s": (
                round(self._joined_at - self._started_at, 3) if self._joined_at else None
            ),
            "holdback_peak": self._hb_peak,
            "history_enabled": self._history is not None,
            "restored_vc_entries": len(self._restored_vc),
            "recovery_chunks": self._recovery_chunks,
            "recovery_took_s": self._recovery_took_s,
            "deliveries_before_ready": self._deliveries_before_ready,
            "holdback_depth_now": depth_now,
            "holdback_first_nonzero_s": (
                round(first - self._started_at, 3) if first else None
            ),
            "holdback_last_nonzero_s": (
                round(last - self._started_at, 3) if last else None
            ),
            "holdback_drain_span_s": (
                round(last - first, 3) if first and last else None
            ),
            "delivered": len(self._deliveries),
        }

    def stop(self) -> None:
        self._probe_stop.set()
        self.node.stop()

    @property
    def joined(self) -> bool:
        return self._joined.is_set()

    def _probe_loop(self) -> None:
        """Mark this peer joined once it can hello/hello_ack with min_peers.

        'Joined' deliberately means a completed application-level handshake,
        not just a resolvable DNS name or an open TCP port: the readiness
        gate should only open when this peer can actually exchange messages
        on the chat protocol, since that is what the harness depends on.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while not self._probe_stop.is_set():
                peers = self.registry.get_peers()
                reachable = loop.run_until_complete(self._probe_peers(peers))
                self._reachable = reachable
                if len(reachable) >= self.min_peers and not self._joined.is_set():
                    if not self._recovery_settled():
                        self._probe_stop.wait(JOIN_PROBE_INTERVAL)
                        continue
                    self._begin_causal_log()
                    self._joined_at = time.time()
                    self._joined.set()
                    logger.info(
                        "JOINED after %.2fs — %d/%d peers reachable, "
                        "recovery=%ss, %d chunks",
                        self._joined_at - self._started_at, len(reachable),
                        self.min_peers, self._recovery_took_s, self._recovery_chunks,
                    )
                self._probe_stop.wait(JOIN_PROBE_INTERVAL)
        finally:
            loop.close()

    def _seed_seen_set(self) -> int:
        """Re-mark already-stored messages as seen after a restart.

        BroadcastNode._seen is in-memory, so a restarted peer treats everything
        as new. Senders hold undelivered messages in _retry_queue and flush
        them on reconnect, so the peer receives messages that recovery has
        already accounted for in its clock. deduplicate() lets them through
        (new to this process), the causal layer sees a stamp behind the
        restored clock, and they sit in hold-back until the 5s timeout releases
        them out of order.

        Seeding the set from the store closes that: a retried message the peer
        already has is dropped where duplicates are supposed to be dropped,
        before it ever reaches the causal path. This uses deduplicate()'s
        documented public contract and touches neither VectorClock nor
        HoldBackQueue.
        """
        if self._history is None:
            return 0
        seeded = 0
        try:
            for stored in self._history.get_recent_messages(limit=SEEN_SEED_LIMIT):
                if self.node.deduplicate(stored.id):
                    seeded += 1
        except Exception as exc:
            logger.warning("could not seed dedup set from store: %s", exc)
            return 0
        self._seen_seeded += seeded
        if seeded:
            logger.info("seeded %d stored message IDs into the dedup set", seeded)
        return seeded

    def _recovery_settled(self) -> bool:
        """True once history recovery has been asked for and has gone quiet.

        Issued from the probe loop rather than start(): recover_request goes
        out over the peer registry, so it is pointless until peers are actually
        reachable.
        """
        if self._history is None:
            return True

        now = time.time()
        if self._recovery_requested_at is None:
            try:
                result = self._history.request_missing_history()
                logger.info(
                    "recover_request sent to %d peers, have_vc=%d entries",
                    result.get("peers_requested", 0),
                    len(result.get("have_vector_clock") or {}),
                )
            except Exception as exc:
                logger.warning("request_missing_history failed: %s", exc)
            self._recovery_requested_at = now
            return False

        last = self._recovery_last_chunk_at or self._recovery_requested_at
        if (now - last) >= RECOVERY_SETTLE or (now - self._recovery_requested_at) >= RECOVERY_MAX:
            self._recovery_took_s = round(now - self._recovery_requested_at, 3)
            return True
        return False

    def _begin_causal_log(self) -> None:
        """Anchor the causal log at the post-recovery clock.

        Messages delivered before recovery settled are genuinely unordered —
        the peer had not finished rebuilding its state — so they are dropped
        from the log rather than scored. The count is reported, not hidden.
        """
        # Recovery has just added messages to the store; senders may retry
        # those same messages, so fold them into the dedup set before the
        # causal log opens.
        self._seed_seen_set()
        with self._log_lock:
            self._deliveries_before_ready = len(self._deliveries)
            self._deliveries.clear()
            self._baseline_vc = short_vc(self.node._vc.snapshot())
        if self._deliveries_before_ready:
            logger.info(
                "causal log anchored after recovery: discarded %d pre-ready deliveries, "
                "baseline has %d entries",
                self._deliveries_before_ready, len(self._baseline_vc),
            )

    async def _probe_peers(self, peers) -> list[str]:
        if websockets is None:
            return []
        results = await asyncio.gather(
            *[self._hello(h, p) for h, p in peers], return_exceptions=True
        )
        return [r for r in results if isinstance(r, str)]

    async def _hello(self, host: str, port: int) -> str | None:
        """One hello/hello_ack round trip against BroadcastNode's own handler."""
        try:
            async with websockets.connect(
                f"ws://{host}:{port}", open_timeout=JOIN_PROBE_TIMEOUT, close_timeout=1
            ) as ws:
                await ws.send(json.dumps({"type": "hello", "sender": self.node.address}))
                raw = await asyncio.wait_for(ws.recv(), timeout=JOIN_PROBE_TIMEOUT)
                if json.loads(raw).get("type") == "hello_ack":
                    return f"{host}:{port}"
        except Exception:
            return None
        return None

    # ── Introspection ───────────────────────────────────────────────────────

    def status(self) -> dict:
        with self._log_lock:
            delivered, sent = len(self._deliveries), len(self._sent)
        return {
            "pod": self.identity.pod_name,
            "ordinal": self.identity.ordinal,
            "address": self.node.address,
            "vc_key": short_key(self.node.address),
            "joined": self.joined,
            "joined_after_s": (
                round(self._joined_at - self._started_at, 3) if self._joined_at else None
            ),
            "uptime_s": round(time.time() - self._started_at, 3),
            "reachable_peers": self._reachable,
            "min_peers": self.min_peers,
            "delivered": delivered,
            "sent": sent,
            "holdback_depth": len(self.node._hold_back._queue),
            "local_vc": short_vc(self.node._vc.snapshot()),
            "baseline_vc": self.baseline_vc(),
            "history_enabled": self._history is not None,
            "restored_vc_entries": len(self._restored_vc),
            "recovery_chunks": self._recovery_chunks,
            "recovery_took_s": self._recovery_took_s,
            "deliveries_before_ready": self._deliveries_before_ready,
            "seen_seeded": self._seen_seeded,
            "discovery": self.registry.stats(),
        }

    def deliveries(self) -> list[dict]:
        with self._log_lock:
            return list(self._deliveries)

    def sent(self) -> list[dict]:
        with self._log_lock:
            return list(self._sent)

    def reset(self) -> dict:
        """Clear both logs and re-anchor the causal replay baseline."""
        with self._log_lock:
            self._deliveries.clear()
            self._sent.clear()
            self._baseline_vc = short_vc(self.node._vc.snapshot())
            return dict(self._baseline_vc)

    def baseline_vc(self) -> dict:
        with self._log_lock:
            return dict(self._baseline_vc)


def make_handler(runtime: PeerRuntime):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # quieter than the default stderr spam
            logger.debug("control: " + fmt, *args)

        def _respond(self, code: int, payload) -> None:
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/healthz":
                self._respond(200, {"ok": True})
            elif path == "/readyz":
                if runtime.joined:
                    self._respond(200, {"joined": True})
                else:
                    self._respond(503, {"joined": False, "reachable": len(runtime._reachable)})
            elif path == "/status":
                self._respond(200, runtime.status())
            elif path == "/clock":
                # Full address keys: this feeds sync_vector_clock, which is
                # keyed on BroadcastNode.address, not the shortened log key.
                self._respond(200, {"vc": runtime.node._vc.snapshot()})
            elif path == "/recovery":
                self._respond(200, runtime.recovery_stats())
            elif path == "/deliveries":
                self._respond(200, {
                    "pod": runtime.identity.pod_name,
                    "baseline_vc": runtime.baseline_vc(),
                    "deliveries": runtime.deliveries(),
                })
            elif path == "/sent":
                self._respond(200, {"pod": runtime.identity.pod_name, "sent": runtime.sent()})
            else:
                self._respond(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._respond(400, {"error": "invalid json"})
                return

            if path == "/send":
                result = runtime.send(
                    count=int(body.get("count", 1)),
                    rate=body.get("rate"),
                    prefix=str(body.get("prefix", "m")),
                )
                self._respond(200, result)
            elif path == "/reset":
                baseline = runtime.reset()
                self._respond(200, {"reset": True, "baseline_vc": baseline})
            else:
                self._respond(404, {"error": "not found"})

    return Handler


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PEERCHAT_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    identity = identity_from_env()
    replicas = int(os.environ.get("PEERCHAT_REPLICAS", "3"))
    min_peers = int(os.environ.get("PEERCHAT_MIN_PEERS", str(max(1, replicas - 1))))

    runtime = PeerRuntime(
        identity, replicas=replicas, min_peers=min_peers,
        history_dir=os.environ.get("PEERCHAT_HISTORY_DIR") or None,
    )
    logger.info(
        "starting %s addr=%s replicas=%d min_peers=%d",
        identity.pod_name, runtime.node.address, replicas, min_peers,
    )
    runtime.start()

    server = ThreadingHTTPServer(("0.0.0.0", identity.control_port), make_handler(runtime))
    server.daemon_threads = True

    def shutdown(signum, _frame):
        logger.info("signal %s — shutting down", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logger.info("control API on :%d", identity.control_port)
    try:
        server.serve_forever()
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
