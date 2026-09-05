#!/usr/bin/env bash
# Run the verification harness as a one-shot pod inside the cluster and save
# the report. Usage: scripts/run-harness.sh <messages> <peers> <rate> <label>
set -euo pipefail
MESSAGES="${1:-1000}"
PEERS="${2:-10}"
RATE="${3:-50}"
LABEL="${4:-run}"
NS="${NS:-peerchat}"
OUT="results/${LABEL}.txt"

mkdir -p results
POD="harness-$(date +%s)"
echo "[*] ${LABEL}: ${MESSAGES} messages across ${PEERS} peers at ${RATE}/s per sender"
kubectl -n "$NS" run "$POD" --rm -i --restart=Never \
  --image=peerchat:dev --image-pull-policy=IfNotPresent \
  --command -- python -m harness.driver \
    --messages "$MESSAGES" --peers "$PEERS" --rate "$RATE" \
  | tee "$OUT"
echo "[*] saved $OUT"
