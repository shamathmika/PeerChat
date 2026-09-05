#!/usr/bin/env bash
# Latency vs cluster size, holding the workload identical across both runs:
# same total messages and the same aggregate offered rate, so the only thing
# that changes is how many peers the causal layer has to track.
#
#   total messages : 1500      aggregate rate : 10 msg/s
#   3 peers  -> 500 each at 3.33/s
#   10 peers -> 150 each at 1.0/s
set -euo pipefail
TOTAL="${TOTAL:-1500}"
AGG="${AGG:-10}"

for N in 3 10; do
  RATE=$(python3 -c "print($AGG / $N)")
  echo "[*] scaling to $N peers"
  scripts/scale.sh "$N"
  sleep 5
  echo "[*] latency run: $TOTAL messages, $N peers, ${RATE}/s per sender"
  scripts/run-harness.sh "$TOTAL" "$N" "$RATE" "latency-${N}peers"
done
