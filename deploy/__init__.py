"""Kubernetes deployment support for PeerChat.

This package adapts the existing peer to run inside a StatefulSet. It adds
nothing to the causal-delivery path: identity and peer discovery are supplied
to ``BroadcastNode`` through its existing constructor arguments and the
``PeerRegistry`` interface, so ``distribution/vector_clock.py`` and the
hold-back queue are used exactly as written.
"""
