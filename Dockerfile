# syntax=docker/dockerfile:1

# ── Stage 1: build wheels ────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY deploy/requirements-peer.txt .

# --prefix keeps the install relocatable so stage 2 can copy it wholesale
# without pip, compilers, or build caches ending up in the final image.
RUN pip install --no-cache-dir --prefix=/install -r requirements-peer.txt

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim

# Non-root. Fixed UID so the securityContext in the StatefulSet can assert it.
RUN useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin peer

COPY --from=builder /install /usr/local

WORKDIR /app
COPY --chown=peer:peer distribution/ ./distribution/
COPY --chown=peer:peer security/ ./security/
COPY --chown=peer:peer message_history/ ./message_history/
COPY --chown=peer:peer deploy/ ./deploy/
COPY --chown=peer:peer harness/ ./harness/

USER 10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

EXPOSE 5678 8080

ENTRYPOINT ["python", "-m", "deploy.peer_node"]
