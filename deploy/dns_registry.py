"""Peer discovery backed by the headless Service's DNS records.

Replaces the seed/gossip bootstrap (``DiscoveryConfig.bootstrap_peers`` plus
``peer_discovery.network.gossip``) for the Kubernetes deployment. Nothing is
hardcoded: membership is whatever DNS currently publishes for the headless
Service, so scaling the StatefulSet changes the peer set with no config edit.

Resolution order
----------------
1. **SRV** on ``_chat._tcp.<service>.<ns>.svc.<domain>``. A headless Service
   with a *named* port publishes one SRV record per endpoint whose target is
   the per-pod DNS name — the stable name, not the pod IP. This is the path
   that runs in the cluster.
2. **Per-pod A records** for ordinals ``0..replicas-1``. Used when SRV is
   unavailable (no dnspython, or a resolver that filters SRV). Still DNS: a
   name that does not resolve is not a peer.

The headless Service sets ``publishNotReadyAddresses: true`` so a starting pod
can see its siblings before any of them is Ready — otherwise readiness (which
requires having found peers) and DNS publication (which requires readiness)
would deadlock the whole StatefulSet at boot.

This class implements the existing ``distribution.peer_registry.PeerRegistry``
interface, so ``BroadcastNode`` consumes it unchanged.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import List, Tuple

from distribution.peer_registry import PeerRegistry

try:  # optional: only needed for the SRV path
    import dns.resolver as _dns_resolver
except ModuleNotFoundError:  # pragma: no cover - exercised by the fallback path
    _dns_resolver = None

logger = logging.getLogger(__name__)

DEFAULT_REFRESH_INTERVAL = 5.0
SRV_PORT_NAME = "chat"


class DnsPeerRegistry(PeerRegistry):
    """A ``PeerRegistry`` whose membership is a headless-Service DNS lookup.

    Results are cached for ``refresh_interval`` seconds because
    ``BroadcastNode`` calls :meth:`get_peers` on every forward, and a DNS
    round trip per forwarded message would dominate delivery latency.
    """

    def __init__(
        self,
        identity,
        replicas: int,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
        resolver=None,
    ) -> None:
        self._identity = identity
        self._replicas = replicas
        self._refresh_interval = refresh_interval
        self._resolver = resolver  # injectable for tests
        self._lock = threading.Lock()
        self._cached: List[Tuple[str, int]] = []
        self._cached_at = 0.0
        self._last_error: str | None = None

    # ── PeerRegistry interface ───────────────────────────────────────────────

    def get_peers(self) -> List[Tuple[str, int]]:
        """Current peers as ``(host, port)``, excluding self.

        Never raises: a DNS failure returns the last known good set so a
        transient CoreDNS blip does not empty the peer list mid-broadcast.
        """
        now = time.monotonic()
        with self._lock:
            fresh = self._cached and (now - self._cached_at) < self._refresh_interval
            if fresh:
                return list(self._cached)

        try:
            peers = self._resolve()
            with self._lock:
                self._cached = peers
                self._cached_at = now
                self._last_error = None
            return list(peers)
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)
                stale = list(self._cached)
            logger.warning("peer DNS lookup failed (%s); using %d cached peers", exc, len(stale))
            return stale

    def get_pub_key(self, host: str, port: int) -> str:
        """No key distribution over DNS.

        Returning empty keeps ``BroadcastNode._has_any_peer_key()`` False, so
        the node runs in its existing unsigned mode unless
        ``enforce_signatures`` is set. Signing is orthogonal to causal
        ordering and is left exactly as the security module configures it.
        """
        return ""

    # ── Resolution ───────────────────────────────────────────────────────────

    def _resolve(self) -> List[Tuple[str, int]]:
        """Resolve current membership, or raise so the caller keeps its cache.

        A failed SRV query must not be silently downgraded to "no peers": the
        per-pod fallback returns an empty list for names that do not resolve,
        which is indistinguishable from a cluster that really has no peers. If
        neither path finds anything *and* SRV errored, the error propagates and
        get_peers() serves the last known good set instead.
        """
        srv_error: Exception | None = None
        peers: List[Tuple[str, int]] | None = None

        if self._resolver is not None or _dns_resolver is not None:
            try:
                peers = self._resolve_srv()
            except Exception as exc:
                logger.debug("SRV lookup failed (%s); trying per-pod A records", exc)
                srv_error = exc

        if not peers:
            peers = self._resolve_per_pod()
        if not peers and srv_error is not None:
            raise srv_error

        me = (self._identity.fqdn, self._identity.chat_port)
        return sorted(p for p in peers if p != me)

    def _resolve_srv(self) -> List[Tuple[str, int]] | None:
        """SRV lookup. Returns None when SRV answers nothing; raises on query error."""
        resolver = self._resolver or _dns_resolver
        if resolver is None:
            return None
        ident = self._identity
        qname = (
            f"_{SRV_PORT_NAME}._tcp.{ident.service}."
            f"{ident.namespace}.svc.{ident.cluster_domain}"
        )
        answer = resolver.resolve(qname, "SRV")

        peers: List[Tuple[str, int]] = []
        for record in answer:
            target = str(record.target).rstrip(".")
            if target:
                peers.append((target, int(record.port)))
        if not peers:
            return None
        logger.debug("SRV %s -> %d peers", qname, len(peers))
        return peers

    def _resolve_per_pod(self) -> List[Tuple[str, int]]:
        """Resolve each ordinal's per-pod DNS name; keep the ones that answer."""
        ident = self._identity
        peers: List[Tuple[str, int]] = []
        for ordinal in range(self._replicas):
            fqdn = ident.peer_fqdn(ordinal)
            try:
                socket.getaddrinfo(fqdn, ident.chat_port, proto=socket.IPPROTO_TCP)
            except socket.gaierror:
                continue  # pod not scheduled yet, or scaled away
            peers.append((fqdn, ident.chat_port))
        return peers

    # ── Introspection (surfaced on the control server's /status) ─────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "peers": [f"{h}:{p}" for h, p in self._cached],
                "peer_count": len(self._cached),
                "cached_age_s": round(time.monotonic() - self._cached_at, 2) if self._cached_at else None,
                "last_error": self._last_error,
                "srv_available": _dns_resolver is not None,
            }
