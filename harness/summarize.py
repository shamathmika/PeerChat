"""Render a driver report (JSON on stdin or a file) as a readable table."""

from __future__ import annotations

import json
import sys


def main() -> int:
    raw = open(sys.argv[1]).read() if len(sys.argv) > 1 else sys.stdin.read()
    if "===REPORT-JSON-BEGIN===" in raw:
        raw = raw.split("===REPORT-JSON-BEGIN===")[1].split("===REPORT-JSON-END===")[0]
    report = json.loads(raw)
    a = report["analysis"]
    cfg = report["config"]

    print("=" * 74)
    print(f"peers={cfg['peers']}  senders={cfg['senders']}  "
          f"messages={cfg['messages_total']}  rate/sender={cfg['rate_per_sender']}/s")
    print("=" * 74)
    print(f"ready wait            : {report.get('ready_wait_s')} s")
    print(f"origination wall      : {report.get('send_wall_s')} s "
          f"({report.get('send_throughput_msg_s')} msg/s offered)")
    conv = report.get("convergence", {})
    print(f"convergence           : {conv.get('converged')} after {conv.get('seconds')} s"
          + (f"  [{conv.get('reason')}]" if conv.get("reason") else ""))
    print()
    print(f"messages sent         : {a['messages_sent']}")
    print(f"expected deliveries   : {a['expected_deliveries']}  ({a['messages_sent']} x {a['peers']} peers)")
    print(f"actual deliveries     : {a['total_delivered']}")
    print(f"CAUSAL VIOLATIONS     : {a.get('violating_deliveries', '?')} deliveries out of order "
          f"({a['violations_total']} entries)")
    print(f"  by kind             : {a['violations_by_kind'] or '{}'}")
    print(f"  of which held >5s   : {a['violations_past_holdback_timeout']} (hold-back timeout releases)")
    lat = a["latency_remote_all_peers"]
    print()
    print(f"end-to-end latency (remote deliveries, n={lat['count']})")
    print(f"  p50 {lat['p50_ms']} ms   p95 {lat['p95_ms']} ms   "
          f"p99 {lat['p99_ms']} ms   max {lat['max_ms']} ms")
    print()
    print(f"{'pod':<12}{'sent':>7}{'delivered':>11}{'dupes':>7}{'missing':>9}"
          f"{'viol':>6}{'p50ms':>9}{'p95ms':>9}")
    for row in a["per_pod"]:
        rl = row["latency_remote"]
        print(f"{row['pod']:<12}{row['sent']:>7}{row['delivered']:>11}{row['duplicates']:>7}"
              f"{row['missing']:>9}{row['violations']:>6}"
              f"{str(rl['p50_ms']):>9}{str(rl['p95_ms']):>9}")

    if a["violation_samples"]:
        print("\nfirst violations:")
        for v in a["violation_samples"][:8]:
            print(f"  {v['pod']} seq={v['seq']} {v['kind']}: {v['detail']} (held {v['held_ms']} ms)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
