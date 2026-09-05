"""The checker must catch violations, not just bless clean logs."""

from harness.causal_check import analyze, check_peer_log, latency_stats, percentile


def rec(seq, msg_id, sender, vc, ts_send=0.0, ts_recv=0.0):
    return {"seq": seq, "id": msg_id, "sender": sender, "vc": vc,
            "ts_send": ts_send, "ts_recv": ts_recv}


def test_causally_ordered_log_has_no_violations():
    log = [
        rec(0, "a1", "p0", {"p0": 1}),
        rec(1, "b1", "p1", {"p0": 1, "p1": 1}),   # depends on a1, already in
        rec(2, "a2", "p0", {"p0": 2, "p1": 1}),
    ]
    result = check_peer_log("p2", log)
    assert result["violations"] == []


def test_detects_delivery_before_a_causal_predecessor():
    """b1 happens-after a1, but the peer delivered b1 first."""
    log = [
        rec(0, "b1", "p1", {"p0": 1, "p1": 1}),   # needs p0=1, has none
        rec(1, "a1", "p0", {"p0": 1}),
    ]
    result = check_peer_log("p2", log)
    kinds = {v["kind"] for v in result["violations"]}
    assert "missing_dependency" in kinds


def test_detects_gap_in_one_senders_own_sequence():
    log = [
        rec(0, "a1", "p0", {"p0": 1}),
        rec(1, "a3", "p0", {"p0": 3}),            # a2 never delivered
    ]
    result = check_peer_log("p2", log)
    assert [v["kind"] for v in result["violations"]] == ["sender_gap"]


def test_violation_does_not_cascade():
    """One breach must not report every later message as broken too."""
    log = [
        rec(0, "a2", "p0", {"p0": 2}),            # gap: a1 missing
        rec(1, "a3", "p0", {"p0": 3}),
        rec(2, "a4", "p0", {"p0": 4}),
    ]
    result = check_peer_log("p2", log)
    assert len(result["violations"]) == 1


def test_duplicate_delivery_counted():
    log = [rec(0, "a1", "p0", {"p0": 1}), rec(1, "a1", "p0", {"p0": 1})]
    assert check_peer_log("p2", log)["duplicates"] == 1


def test_latency_excludes_self_deliveries():
    log = [
        rec(0, "own", "p0", {"p0": 1}, ts_send=1.0, ts_recv=1.0),      # own message
        rec(1, "rem", "p1", {"p0": 1, "p1": 1}, ts_send=1.0, ts_recv=1.2),
    ]
    stats = latency_stats(log, remote_only=True, pod="p0")
    assert stats["count"] == 1
    assert abs(stats["p50_ms"] - 200.0) < 1e-6


def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50) == 5
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 95) == 10
    assert percentile([], 50) is None


def test_analyze_reports_missing_messages():
    sent = {"p0": [{"id": "a1"}, {"id": "a2"}]}
    deliveries = {
        "p0": [rec(0, "a1", "p0", {"p0": 1}), rec(1, "a2", "p0", {"p0": 2})],
        "p1": [rec(0, "a1", "p0", {"p0": 1})],   # never got a2
    }
    report = analyze(deliveries, sent)
    assert report["messages_sent"] == 2
    assert report["violations_total"] == 0
    per_pod = {p["pod"]: p for p in report["per_pod"]}
    assert per_pod["p1"]["missing"] == 1
    assert per_pod["p0"]["missing"] == 0


def test_baseline_clock_prevents_spurious_first_violation():
    """A log reset on a live cluster starts mid-clock; replay must start there."""
    log = [rec(0, "a21", "p0", {"p0": 21})]

    assert check_peer_log("p1", log)["violations"], "without a baseline this looks broken"
    assert check_peer_log("p1", log, baseline={"p0": 20})["violations"] == []


def test_violating_deliveries_counts_records_not_entries():
    """One delivery missing two dependencies is one bad delivery, not three."""
    sent = {"p0": [{"id": "x"}]}
    deliveries = {"p2": [rec(0, "x", "p0", {"p0": 5, "p1": 3, "p3": 2})]}

    report = analyze(deliveries, sent)

    assert report["violations_total"] == 3        # gap + two unmet dependencies
    assert report["violating_deliveries"] == 1    # but a single bad delivery
