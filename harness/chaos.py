"""Chaos variant: delete pods with kubectl while the same workload runs.

Runs on the workstation (it needs kubectl); the driver itself still runs
in-cluster. Sequence:

    1. start the driver pod,
    2. delete a random peer every --kill-interval seconds,
    3. when the driver finishes, wait for the StatefulSet to come back and pull
       each peer's recovery timings.

Reporting splits violations into peers that were killed and peers that were
not. They are different claims: an untouched peer is expected to preserve
causal order throughout, whereas a peer that restarts comes back with an empty
in-memory vector clock and no history, so its hold-back queue can only age out
via HOLDBACK_TIMEOUT. Merging the two would hide both results.

    python -m harness.chaos --messages 2000 --kills 4 --kill-interval 12
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import threading
import time

NS = "peerchat"


def kubectl(*args: str, check: bool = True, timeout: float = 120) -> str:
    result = subprocess.run(
        ["kubectl", "-n", NS, *args],
        capture_output=True, text=True, timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def pod_control(pod: str, path: str) -> dict:
    """Read a peer's control endpoint from inside its own pod."""
    out = kubectl(
        "exec", pod, "--", "python", "-c",
        f"import urllib.request;print(urllib.request.urlopen("
        f"'http://localhost:8080{path}',timeout=10).read().decode())",
    )
    return json.loads(out)


class Killer(threading.Thread):
    """Deletes random peers on a timer while the workload runs."""

    def __init__(self, peers: int, kills: int, interval: float, delay: float) -> None:
        super().__init__(daemon=True)
        self.peers = peers
        self.kills = kills
        self.interval = interval
        self.delay = delay
        self.killed: list[dict] = []
        self.stop_event = threading.Event()

    def run(self) -> None:
        self.stop_event.wait(self.delay)
        candidates = [f"peerchat-{i}" for i in range(self.peers)]
        random.shuffle(candidates)
        for pod in candidates[: self.kills]:
            if self.stop_event.is_set():
                return
            started = time.time()
            try:
                kubectl("delete", "pod", pod, "--grace-period=1", "--wait=false")
                self.killed.append({"pod": pod, "at": started})
                print(f"[chaos] deleted {pod}", flush=True)
            except Exception as exc:
                print(f"[chaos] delete {pod} failed: {exc}", flush=True)
            self.stop_event.wait(self.interval)


def run_driver(messages: int, peers: int, rate: float) -> tuple[str, int]:
    pod = f"harness-chaos-{int(time.time())}"
    cmd = [
        "kubectl", "-n", NS, "run", pod, "--rm", "-i", "--restart=Never",
        "--image=peerchat:dev", "--image-pull-policy=IfNotPresent",
        "--command", "--", "python", "-m", "harness.driver",
        "--messages", str(messages), "--peers", str(peers), "--rate", str(rate),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    return proc.stdout, proc.returncode


def parse_report(stdout: str) -> dict | None:
    if "===REPORT-JSON-BEGIN===" not in stdout:
        return None
    body = stdout.split("===REPORT-JSON-BEGIN===")[1].split("===REPORT-JSON-END===")[0]
    return json.loads(body)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--peers", type=int, default=10)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--kills", type=int, default=4)
    parser.add_argument("--kill-interval", type=float, default=12.0)
    parser.add_argument("--kill-delay", type=float, default=8.0,
                        help="seconds to wait after start before the first kill")
    parser.add_argument("--seed", type=int, default=275)
    parser.add_argument("--out", default="results/chaos.json")
    args = parser.parse_args()
    random.seed(args.seed)

    killer = Killer(args.peers, args.kills, args.kill_interval, args.kill_delay)
    print(f"[chaos] driving {args.messages} messages, killing {args.kills} pods "
          f"every {args.kill_interval}s", flush=True)

    killer.start()
    stdout, rc = run_driver(args.messages, args.peers, args.rate)
    killer.stop_event.set()

    report = parse_report(stdout)
    if report is None:
        print("[chaos] driver produced no report:\n" + stdout[-4000:], file=sys.stderr)
        return 1

    print("[chaos] waiting for the StatefulSet to recover", flush=True)
    subprocess.run(
        ["kubectl", "-n", NS, "wait", "--for=condition=Ready", "pod",
         "-l", "app=peerchat", "--timeout=300s"],
        capture_output=True, text=True,
    )

    recovery = []
    for i in range(args.peers):
        pod = f"peerchat-{i}"
        try:
            recovery.append(pod_control(pod, "/recovery"))
        except Exception as exc:
            print(f"[chaos] recovery read failed for {pod}: {exc}", flush=True)

    killed_pods = {k["pod"] for k in killer.killed}
    per_pod = {row["pod"]: row for row in report["analysis"]["per_pod"]}
    survivor_violations = sum(
        row["violations"] for pod, row in per_pod.items() if pod not in killed_pods
    )
    killed_violations = sum(
        row["violations"] for pod, row in per_pod.items() if pod in killed_pods
    )

    result = {
        "config": vars(args),
        "killed": killer.killed,
        "driver_report": report,
        "recovery": recovery,
        "violations_survivors": survivor_violations,
        "violations_restarted": killed_violations,
    }
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=2)

    a = report["analysis"]
    print()
    print("=" * 74)
    print(f"CHAOS: {args.messages} messages, {args.peers} peers, "
          f"{len(killer.killed)} pods deleted mid-run")
    print("=" * 74)
    print(f"pods deleted          : {sorted(killed_pods)}")
    print(f"messages sent         : {a['messages_sent']}")
    print(f"total deliveries      : {a['total_delivered']} / {a['expected_deliveries']}")
    print()
    print(f"VIOLATIONS, survivors : {survivor_violations}   <- the invariant under test")
    print(f"VIOLATIONS, restarted : {killed_violations}   (empty clock after restart)")
    print(f"  held past 5s timeout: {a['violations_past_holdback_timeout']}")
    print()
    print(f"{'pod':<12}{'killed':>8}{'rejoin_s':>10}{'hb_peak':>9}"
          f"{'hb_drain_s':>12}{'delivered':>11}{'viol':>6}")
    for row in recovery:
        pod = row["pod"]
        print(f"{pod:<12}{'yes' if pod in killed_pods else '-':>8}"
              f"{str(row['rejoin_s']):>10}{row['holdback_peak']:>9}"
              f"{str(row['holdback_drain_span_s']):>12}"
              f"{row['delivered']:>11}{per_pod.get(pod, {}).get('violations', 0):>6}")
    print(f"\n[chaos] full result written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
