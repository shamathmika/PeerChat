"""Drive N messages through the cluster, collect the logs, verify the ordering.

Runs as a Job inside the cluster so it can reach every pod's control port over
the headless Service directly. Keeping it in-cluster matters for the numbers:
sends are triggered with one request per peer and paced *inside* the pod, and
latency is timestamped pod-side, so nothing in the measurement path crosses a
kubectl port-forward.

    python -m harness.driver --messages 10000 --peers 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from harness.causal_check import analyze


def peer_urls(service: str, namespace: str, domain: str, replicas: int, port: int,
              stem: str = "peerchat") -> list[str]:
    return [
        f"http://{stem}-{i}.{service}.{namespace}.svc.{domain}:{port}"
        for i in range(replicas)
    ]


def _request(url: str, payload: dict | None = None, timeout: float = 600.0) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def wait_ready(urls: list[str], timeout: float) -> float:
    """Block until every peer's /readyz passes. Returns seconds waited."""
    started = time.time()
    pending = set(urls)
    while pending and (time.time() - started) < timeout:
        for url in list(pending):
            try:
                _request(f"{url}/readyz", timeout=5)
                pending.discard(url)
            except Exception:
                pass
        if pending:
            time.sleep(1.0)
    if pending:
        raise TimeoutError(f"peers never became ready: {sorted(pending)}")
    return time.time() - started


def wait_convergence(urls: list[str], expected_per_peer: int, timeout: float,
                     stall_after: float = 30.0) -> dict:
    """Poll /status until every peer has delivered everything, or delivery stalls.

    A stall check is the honest stopping rule here: if messages are genuinely
    lost the run must still terminate and report the shortfall rather than
    hang until the outer timeout.
    """
    started = time.time()
    last_total = -1
    last_progress = time.time()
    while (time.time() - started) < timeout:
        counts = {}
        for url in urls:
            try:
                counts[url] = _request(f"{url}/status", timeout=10)["delivered"]
            except Exception:
                counts[url] = -1
        total = sum(max(0, c) for c in counts.values())
        if all(c >= expected_per_peer for c in counts.values()):
            return {"converged": True, "seconds": round(time.time() - started, 2), "counts": counts}
        if total != last_total:
            last_total, last_progress = total, time.time()
        elif (time.time() - last_progress) > stall_after:
            return {
                "converged": False, "reason": f"no progress for {stall_after}s",
                "seconds": round(time.time() - started, 2), "counts": counts,
            }
        time.sleep(2.0)
    return {"converged": False, "reason": "timeout",
            "seconds": round(time.time() - started, 2), "counts": counts}


def collect(urls: list[str]) -> tuple[dict, dict, dict]:
    """Pull every peer's delivery log, send log, and causal replay baseline."""
    deliveries: dict[str, list[dict]] = {}
    sent: dict[str, list[dict]] = {}
    baselines: dict[str, dict] = {}

    def fetch(url: str):
        # A pod killed by the chaos run may be unreachable at collection time.
        # Losing one peer's log must not lose the other nine.
        pod_guess = url.split("//", 1)[1].split(".", 1)[0]
        try:
            d = _request(f"{url}/deliveries", timeout=600)
            s = _request(f"{url}/sent", timeout=600)
        except Exception as exc:
            print(f"[driver] WARNING: could not collect from {pod_guess}: {exc}", flush=True)
            return pod_guess, [], [], {}
        return d["pod"], d["deliveries"], s["sent"], d.get("baseline_vc", {})

    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        for pod, dlist, slist, baseline in pool.map(fetch, urls):
            deliveries[pod] = dlist
            sent[pod] = slist
            baselines[pod] = baseline
    return deliveries, sent, baselines


def run(args) -> dict:
    urls = peer_urls(args.service, args.namespace, args.domain, args.peers,
                     args.control_port, args.stem)
    senders = urls[: args.senders]

    report: dict = {
        "config": {
            "peers": args.peers,
            "senders": args.senders,
            "messages_total": args.messages,
            "rate_per_sender": args.rate,
        }
    }

    ready_s = wait_ready(urls, timeout=args.ready_timeout)
    report["ready_wait_s"] = round(ready_s, 2)
    print(f"[driver] all {len(urls)} peers ready after {ready_s:.1f}s", flush=True)

    if args.reset:
        for url in urls:
            try:
                _request(f"{url}/reset", {})
            except Exception as exc:
                print(f"[driver] reset failed for {url}: {exc}", flush=True)
        print("[driver] logs reset", flush=True)

    per_sender = args.messages // len(senders)
    remainder = args.messages - per_sender * len(senders)
    plan = [per_sender + (1 if i < remainder else 0) for i in range(len(senders))]
    print(f"[driver] sending {args.messages} across {len(senders)} senders: {plan}", flush=True)
    # Under chaos a restarted peer loses its /sent log, so the collected
    # "messages_sent" undercounts. Keep what was asked for alongside it.
    report["messages_planned"] = args.messages
    report["plan"] = {url.split("//", 1)[1].split(".", 1)[0]: n
                      for url, n in zip(senders, plan)}

    # Concurrent origination: every sender starts at the same moment, which is
    # what makes the vector clocks actually concurrent and the hold-back queue
    # do real work.
    def _drive(url: str, count: int) -> dict:
        # /send blocks for the whole paced batch, so a pod deleted mid-run
        # breaks this connection. Report the stump rather than propagating.
        try:
            return _request(f"{url}/send",
                            {"count": count, "rate": args.rate, "prefix": args.prefix})
        except Exception as exc:
            pod = url.split("//", 1)[1].split(".", 1)[0]
            print(f"[driver] sender {pod} interrupted: {exc}", flush=True)
            return {"peer": pod, "requested": count, "error": str(exc), "interrupted": True}

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=len(senders)) as pool:
        futures = [pool.submit(_drive, url, n) for url, n in zip(senders, plan)]
        send_results = [f.result() for f in futures]
    send_wall = time.time() - t0
    report["send_wall_s"] = round(send_wall, 2)
    report["send_throughput_msg_s"] = round(args.messages / send_wall, 1) if send_wall else None
    print(f"[driver] origination done in {send_wall:.1f}s", flush=True)

    conv = wait_convergence(urls, args.messages, timeout=args.converge_timeout)
    report["convergence"] = conv
    print(f"[driver] convergence: {conv.get('converged')} after {conv['seconds']}s", flush=True)

    deliveries, sent, baselines = collect(urls)
    print(f"[driver] collected logs from {len(deliveries)} peers", flush=True)

    report["analysis"] = analyze(deliveries, sent, baselines)
    report["send_results"] = send_results

    if args.dump:
        with open(args.dump, "w") as fh:
            json.dump({"deliveries": deliveries, "sent": sent, "baselines": baselines}, fh)
        report["raw_dump"] = args.dump

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--peers", type=int, default=int(os.environ.get("PEERCHAT_REPLICAS", 10)))
    parser.add_argument("--senders", type=int, default=0,
                        help="how many peers originate (0 = all peers)")
    parser.add_argument("--rate", type=float, default=50.0,
                        help="messages/second per sender; 0 = unpaced")
    parser.add_argument("--service", default=os.environ.get("PEERCHAT_SERVICE", "peerchat-hl"))
    parser.add_argument("--namespace", default=os.environ.get("PEERCHAT_NAMESPACE", "peerchat"))
    parser.add_argument("--domain", default=os.environ.get("PEERCHAT_CLUSTER_DOMAIN", "cluster.local"))
    parser.add_argument("--stem", default="peerchat")
    parser.add_argument("--control-port", type=int, default=8080)
    parser.add_argument("--prefix", default="m")
    parser.add_argument("--ready-timeout", type=float, default=300.0)
    parser.add_argument("--converge-timeout", type=float, default=1800.0)
    parser.add_argument("--reset", action="store_true", default=True)
    parser.add_argument("--dump", default=None, help="write raw logs to this path")
    parser.add_argument("--out", default=None, help="write the JSON report here too")
    args = parser.parse_args()

    if args.senders <= 0:
        args.senders = args.peers

    report = run(args)

    print("===REPORT-JSON-BEGIN===", flush=True)
    print(json.dumps(report, indent=2), flush=True)
    print("===REPORT-JSON-END===", flush=True)

    if args.out:
        with open(args.out, "w") as fh:
            json.dump(report, fh, indent=2)

    violations = report["analysis"]["violations_total"]
    return 0 if violations == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
