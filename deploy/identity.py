"""Stable peer identity derived from the StatefulSet ordinal.

Why this module exists
----------------------
``BroadcastNode`` keys its vector clock on ``self.address`` (``host:port``),
which ``main.py`` builds from :func:`peer_discovery.network.net_utils.get_lan_ip`.
Inside Kubernetes that resolves to the *pod IP*, which is reassigned on every
restart. A peer that crashed and came back would therefore re-enter the cluster
under a new vector-clock key, and every clock in the cluster would carry a dead
entry for the old address while the restarted peer started from zero.

A StatefulSet pod keeps its name (``peerchat-0``, ``peerchat-1``, ...) and its
per-pod DNS record across restarts and rescheduling. Deriving the advertised
address from that name gives the vector clock a key with the same lifetime as
the logical peer, which is what causal continuity requires.

No UUIDs are involved anywhere: identity is a pure function of the ordinal.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

DEFAULT_CHAT_PORT = 5678
DEFAULT_CONTROL_PORT = 8080
DEFAULT_CLUSTER_DOMAIN = "cluster.local"


class IdentityError(RuntimeError):
    """Raised when the pod identity cannot be determined."""


@dataclass(frozen=True)
class PeerIdentity:
    """Everything derived from the pod's position in the StatefulSet."""

    pod_name: str          # "peerchat-3"
    ordinal: int           # 3
    service: str           # "peerchat-hl" (the headless Service)
    namespace: str         # "peerchat"
    cluster_domain: str    # "cluster.local"
    chat_port: int
    control_port: int

    @property
    def fqdn(self) -> str:
        """The per-pod DNS name the headless Service publishes.

        Stable across restarts, unlike the pod IP.
        """
        return f"{self.pod_name}.{self.service}.{self.namespace}.svc.{self.cluster_domain}"

    @property
    def address(self) -> str:
        """The value that becomes ``BroadcastNode.address`` and the vector-clock key."""
        return f"{self.fqdn}:{self.chat_port}"

    def peer_fqdn(self, ordinal: int) -> str:
        """The DNS name of a sibling pod by ordinal."""
        stem = self.pod_name.rsplit("-", 1)[0]
        return f"{stem}-{ordinal}.{self.service}.{self.namespace}.svc.{self.cluster_domain}"


def _split_ordinal(pod_name: str) -> int:
    """Extract the trailing ordinal from a StatefulSet pod name."""
    stem, _, tail = pod_name.rpartition("-")
    if not stem or not tail.isdigit():
        raise IdentityError(
            f"pod name {pod_name!r} is not a StatefulSet pod name "
            "(expected '<statefulset>-<ordinal>'); set PEERCHAT_POD_NAME"
        )
    return int(tail)


def identity_from_env(env: dict[str, str] | None = None) -> PeerIdentity:
    """Build the peer identity from the downward-API environment.

    The StatefulSet supplies ``PEERCHAT_POD_NAME`` and ``PEERCHAT_NAMESPACE``
    via ``fieldRef``. Outside Kubernetes the pod name falls back to the
    hostname, which lets the same entrypoint run under docker-compose or
    locally for tests.
    """
    env = os.environ if env is None else env

    pod_name = env.get("PEERCHAT_POD_NAME") or socket.gethostname()
    service = env.get("PEERCHAT_SERVICE")
    if not service:
        raise IdentityError("PEERCHAT_SERVICE (headless Service name) is required")

    return PeerIdentity(
        pod_name=pod_name,
        ordinal=_split_ordinal(pod_name),
        service=service,
        namespace=env.get("PEERCHAT_NAMESPACE", "default"),
        cluster_domain=env.get("PEERCHAT_CLUSTER_DOMAIN", DEFAULT_CLUSTER_DOMAIN),
        chat_port=int(env.get("PEERCHAT_PORT", DEFAULT_CHAT_PORT)),
        control_port=int(env.get("PEERCHAT_CONTROL_PORT", DEFAULT_CONTROL_PORT)),
    )
