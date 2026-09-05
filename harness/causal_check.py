"""Offline verification that causal delivery order held at every peer.

Deliberately does not import ``distribution.vector_clock``. Reusing
``VectorClock.is_ready`` as the oracle would make the check tautological — a
bug in the readiness predicate would validate itself. The condition below is
restated from the CBCAST rule (Birman et al.) against the recorded logs.

The invariant
-------------
A peer ``p`` delivers message ``m``, sent by ``s`` and stamped ``V(m)``.
Let ``D`` be the clock of everything ``p`` has already delivered. Delivery is
causally correct iff

    (1) V(m)[s] == D[s] + 1          m is the next message from s that p owes
    (2) V(m)[k] <= D[k]  for k != s  every message m depends on is already in

Violating (1) upward means p skipped an earlier message from the same sender.
Violating (2) means p delivered m before a message that happens-before it —
the failure the harness exists to detect.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Iterable


def check_peer_log(pod: str, deliveries: list[dict], baseline: dict | None = None) -> dict:
    """Replay one peer's delivery sequence and report every ordering breach.

    ``baseline`` is the peer's vector clock at the moment its delivery log
    began. It is empty for a peer logging from process start; it is non-empty
    when the log was reset on a running cluster, and replaying from zero in
    that case would report a spurious violation on the very first record.
    """
    delivered: dict[str, int] = defaultdict(int)
    if baseline:
        delivered.update(baseline)
    violations: list[dict] = []
    seen_ids: set[str] = set()
    duplicates = 0

    for record in deliveries:
        sender = record["sender"]
        vc = record.get("vc") or {}
        msg_id = record["id"]

        if msg_id in seen_ids:
            duplicates += 1
        seen_ids.add(msg_id)

        held_ms = None
        if record.get("ts_recv") is not None and record.get("ts_send") is not None:
            held_ms = round((record["ts_recv"] - record["ts_send"]) * 1000, 2)

        # (1) own-sender sequencing
        expected = delivered[sender] + 1
        got = vc.get(sender, 0)
        if got != expected:
            violations.append(
                {
                    "pod": pod,
                    "seq": record["seq"],
                    "id": msg_id,
                    "sender": sender,
                    "kind": "sender_gap" if got > expected else "sender_regress",
                    "detail": f"V[{sender}]={got}, expected {expected}",
                    "held_ms": held_ms,
                }
            )

        # (2) dependencies from other senders
        for node, count in vc.items():
            if node == sender:
                continue
            if count > delivered[node]:
                violations.append(
                    {
                        "pod": pod,
                        "seq": record["seq"],
                        "id": msg_id,
                        "sender": sender,
                        "kind": "missing_dependency",
                        "detail": (
                            f"needs {node}<={count} but only {delivered[node]} "
                            f"delivered from {node}"
                        ),
                        "held_ms": held_ms,
                    }
                )

        # Advance past the breach so one violation does not cascade into
        # thousands of spurious follow-on reports.
        for node, count in vc.items():
            if count > delivered[node]:
                delivered[node] = count

    return {
        "pod": pod,
        "delivered": len(deliveries),
        "unique": len(seen_ids),
        "duplicates": duplicates,
        "violations": violations,
        "final_clock": dict(delivered),
    }


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile: the smallest value at or above pct of the data.

    rank = ceil(pct/100 * N), 1-indexed. Using round() here instead of ceil()
    lands on the wrong element for even N (Python rounds .5 to even).
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(pct / 100.0 * len(ordered))
    idx = min(max(rank - 1, 0), len(ordered) - 1)
    return ordered[idx]


def latency_stats(deliveries: Iterable[dict], *, remote_only: bool, pod: str | None = None) -> dict:
    """End-to-end latency: message creation at the sender -> delivery here.

    ``remote_only`` drops a peer's deliveries of its own messages. Those are
    in-process callbacks with near-zero latency; leaving them in would pull the
    median down by roughly 1/N and describe nothing about the network.
    """
    samples: list[float] = []
    for record in deliveries:
        if record.get("ts_send") is None or record.get("ts_recv") is None:
            continue
        if remote_only and pod is not None and record["sender"] == pod:
            continue
        samples.append((record["ts_recv"] - record["ts_send"]) * 1000.0)

    if not samples:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    return {
        "count": len(samples),
        "p50_ms": round(percentile(samples, 50), 2),
        "p95_ms": round(percentile(samples, 95), 2),
        "p99_ms": round(percentile(samples, 99), 2),
        "max_ms": round(max(samples), 2),
        "mean_ms": round(sum(samples) / len(samples), 2),
    }


def analyze(
    per_pod_deliveries: dict[str, list[dict]],
    per_pod_sent: dict[str, list[dict]],
    per_pod_baseline: dict[str, dict] | None = None,
) -> dict:
    """Full report over every peer's collected log."""
    per_pod_baseline = per_pod_baseline or {}
    total_sent = sum(len(v) for v in per_pod_sent.values())
    sent_ids = {m["id"] for records in per_pod_sent.values() for m in records}

    per_pod: list[dict] = []
    all_violations: list[dict] = []
    all_latency_samples: list[float] = []

    for pod, deliveries in sorted(per_pod_deliveries.items()):
        result = check_peer_log(pod, deliveries, per_pod_baseline.get(pod))
        lat = latency_stats(deliveries, remote_only=True, pod=pod)
        result["latency_remote"] = lat
        result["sent"] = len(per_pod_sent.get(pod, []))
        result["missing"] = len(sent_ids - {d["id"] for d in deliveries})
        all_violations.extend(result["violations"])
        for record in deliveries:
            if record.get("ts_send") is not None and record["sender"] != pod:
                all_latency_samples.append((record["ts_recv"] - record["ts_send"]) * 1000.0)
        per_pod.append(
            {
                "pod": pod,
                "sent": result["sent"],
                "delivered": result["delivered"],
                "unique": result["unique"],
                "duplicates": result["duplicates"],
                "missing": result["missing"],
                "violations": len(result["violations"]),
                "latency_remote": lat,
                "final_clock": result["final_clock"],
            }
        )

    by_kind: dict[str, int] = defaultdict(int)
    for violation in all_violations:
        by_kind[violation["kind"]] += 1

    # One bad delivery can raise several entries (a sender_gap plus one
    # missing_dependency per unmet sender), and a single timeout-induced clock
    # jump cascades into a sender_regress on every later message from that
    # sender. Counting distinct (pod, seq) pairs gives the number that actually
    # answers "how many deliveries were out of causal order".
    violating_records = len({(v["pod"], v["seq"]) for v in all_violations})

    # Violations whose message sat longer than the hold-back timeout are the
    # ones HoldBackQueue released deliberately (HOLDBACK_TIMEOUT = 5.0s).
    timeout_attributable = sum(
        1 for v in all_violations if (v.get("held_ms") or 0) >= 5000.0
    )

    return {
        "messages_sent": total_sent,
        "unique_message_ids": len(sent_ids),
        "peers": len(per_pod_deliveries),
        "total_delivered": sum(len(v) for v in per_pod_deliveries.values()),
        "expected_deliveries": total_sent * len(per_pod_deliveries),
        "violations_total": len(all_violations),
        "violating_deliveries": violating_records,
        "violations_by_kind": dict(by_kind),
        "violations_past_holdback_timeout": timeout_attributable,
        "latency_remote_all_peers": {
            "count": len(all_latency_samples),
            "p50_ms": round(percentile(all_latency_samples, 50), 2) if all_latency_samples else None,
            "p95_ms": round(percentile(all_latency_samples, 95), 2) if all_latency_samples else None,
            "p99_ms": round(percentile(all_latency_samples, 99), 2) if all_latency_samples else None,
            "max_ms": round(max(all_latency_samples), 2) if all_latency_samples else None,
        },
        "per_pod": per_pod,
        "violation_samples": all_violations[:20],
    }
