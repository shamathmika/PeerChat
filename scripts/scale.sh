#!/usr/bin/env bash
# Resize the peer cluster. PEERCHAT_MIN_PEERS must track the replica count or
# the readiness probe (which waits for N-1 reachable siblings) never passes.
# Usage: scripts/scale.sh <replicas>
set -euo pipefail
N="${1:?usage: scripts/scale.sh <replicas>}"
NS="${NS:-peerchat}"

kubectl -n "$NS" set env statefulset/peerchat \
  PEERCHAT_REPLICAS="$N" PEERCHAT_MIN_PEERS="$((N - 1))" >/dev/null
kubectl -n "$NS" scale statefulset/peerchat --replicas="$N" >/dev/null
kubectl -n "$NS" rollout status statefulset/peerchat --timeout=300s
kubectl -n "$NS" wait --for=condition=Ready pod -l app=peerchat --timeout=300s >/dev/null
echo "[*] cluster is $N peers"
