#!/usr/bin/env bash
# Build the peer image and load it into the kind cluster.
# Copies sources to a local build context first: the repo lives on a Google
# Drive mount where docker's context scan is painfully slow.
set -euo pipefail
CLUSTER="${CLUSTER:-peerchat}"
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

rsync -a --exclude='__pycache__' --exclude='*.pyc' --exclude='tests' --exclude='runtime' \
  distribution security deploy harness message_history "$CTX/"
cp Dockerfile .dockerignore "$CTX/"

docker build -t peerchat:dev "$CTX" >/dev/null
kind load docker-image peerchat:dev --name "$CLUSTER"
echo "[*] peerchat:dev loaded into kind/$CLUSTER"
