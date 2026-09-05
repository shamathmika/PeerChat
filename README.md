# P2P Chat: Message Distribution Module

This module implements the **Message Distribution** component of the peer-to-peer distributed chat system. Messages are propagated to every reachable peer over WebSockets with ACK + retry, de-duplicated by UUID, and delivered in **causal order** using vector clocks.

---

## How It Works

When a node sends a message:

1. It assigns the message a UUID and increments its own entry in its **vector clock**, then attaches the full vector to the message.
2. It delivers the message locally (fires `on_message`).
3. It sends the message to **every peer** returned by the peer registry, concurrently, over WebSockets.
4. Each receiving peer sends back an **ACK**.
5. If no ACK arrives within 2 seconds, the sender **retries up to 3 times** (0.5 s, 1.0 s, 1.5 s backoff).

When a node receives a message:

1. Atomic dedup check — if the UUID has been seen, the message is dropped.
2. Causal-readiness check against the local vector clock.
   - **Ready** → deliver via `on_message`, merge the incoming vector clock, then drain the hold-back queue for any cascading deliveries.
   - **Not ready** → buffer in the hold-back queue until predecessors arrive.
3. If `ttl > 0`, forward to all peers (excluding self and the original sender) with ACK + retry.

The combination guarantees **exactly-once delivery per online peer**, in **causal order**, with no routing loops.

---

## Project Structure

```
PeerChat/
├── distribution/
│   ├── __init__.py             # Package exports
│   ├── message.py              # Message dataclass + JSON serialization
│   ├── peer_registry.py        # PeerRegistry interface + InMemoryRegistry
│   ├── broadcast_node.py       # BroadcastNode — WebSocket server + ACK/retry + dedup + VC
│   ├── vector_clock.py         # VectorClock + HoldBackQueue — causal ordering
│   └── membership_router.py    # PeerRegistry wired to Peer Discovery's MembershipService
├── docs/
│   ├── PRD.md                       # Team plan + assignments
│   ├── INTEGRATION.md               # One-page guide for the other teams
│   ├── vector_clock.md              # Vector clock design doc
│   ├── contract_peer_discovery.md   # Peer Discovery integration contract
│   ├── contract_security.md         # Security integration contract
│   └── contract_history.md          # History / Recovery & Storage contract
├── tests/
│   ├── test_dedup_loop_prevention.py   # Dedup + TTL unit tests
│   ├── test_vector_clock.py            # VectorClock + HoldBackQueue unit tests
│   ├── test_integration.py             # End-to-end integration test
│   └── stubs/                          # Fakes for Security, History used by E2E test
├── demo.py                   # Runnable 10-node demo + causal ordering scenarios
└── requirements.txt
```

---

## Setup

**Requirements:** Python 3.11+

```bash
pip install -r requirements.txt
```

(`websockets>=10.0` is the only runtime dependency.)

---

## Quick Start

```bash
python3 demo.py
```

**What the demo does:**
1. Starts 10 broadcast nodes on ports 5001–5010.
2. Node 5001 broadcasts one message. Every other node receives, ACKs, and prints it.
3. Runs 4 causal-ordering scenarios proving out-of-order messages are buffered and released in causal order.

---

## Running the Tests

```bash
pytest tests/ -v
```

Three suites:

| Suite | What it covers |
|---|---|
| `test_dedup_loop_prevention.py` | Atomic dedup, TTL=0 non-forward, TTL decrement, `ttl`-copy safety, local duplicate broadcast, fast-fail when `websockets` is missing |
| `test_vector_clock.py` | VectorClock ops, HoldBackQueue drain + cascade, BroadcastNode causal send/receive, JSON round-trip with `vector_clock` |
| `test_integration.py` | End-to-end: signed message reaches every peer once, unsigned messages dropped, dedup across double-broadcast, multi-listener fan-out |

---

## Public API

### `BroadcastNode`

```python
from distribution import BroadcastNode, Message

node = BroadcastNode(host="127.0.0.1", port=5000, peer_registry=registry)
node.on_message = lambda msg: print(f"got: {msg.content}")
node.start()

node.broadcast(Message(content="hello", sender=node.address))

node.stop()
```

| Method / Property | Purpose |
|---|---|
| `start()` | Start the WebSocket server in a background thread. |
| `stop()` | Shut down the WebSocket server. |
| `broadcast(msg)` | Originate a message into the network. |
| `send_to_peer(host, port, msg)` | Send one message to one peer only. Intended for History/Recovery replay chunks. |
| `sync_vector_clock(vc)` | Advance the local vector clock to at least the values in `vc`, then drain the hold-back queue. Call after history recovery completes. Thread-safe. |
| `on_message` | Callback `(Message) -> None` fired once per unique delivered message, in causal order. |
| `deduplicate(msg_id)` | Atomic check-and-mark. Returns `True` for a new id, `False` for a duplicate. |

### `Message`

| Field | Type | Filled by | Notes |
|---|---|---|---|
| `content` | `str` | Originator (UI) | — |
| `sender` | `str` | Originator | `"host:port"` |
| `id` | `str` | Auto | UUID — used for dedup |
| `timestamp` | `float` | Auto | `time.time()` |
| `signature` | `str` | **Security via Distribution** | Filled by MD calling `security.sign(msg)` |
| `ttl` | `int` | Default 10 | Decremented per hop — **do not sign** |
| `vector_clock` | `dict` | **BroadcastNode** | Filled automatically on send — **do not sign** |

### `PeerRegistry` (Peer Discovery integration)

Two implementations ship in the module:

- **`InMemoryRegistry`**: hard-coded `(host, port)` list. For demos and tests.
- **`MembershipRouter`**: drop-in `PeerRegistry` wired to the Peer Discovery team's `MembershipService`. Tracks ACTIVE vs. BACKFILLING vs. SUSPECTED peers and updates in real time via a subscription.

```python
from distribution import MembershipRouter
router = MembershipRouter(service=peer_discovery_service, self_address="127.0.0.1:5001")
node = BroadcastNode("127.0.0.1", 5001, router)
```

---

## Integration with Other Teams

Short version below. Full contracts live in `docs/`.

### UI team

```python
node = BroadcastNode("127.0.0.1", 5000, registry)
node.on_message = lambda msg: display_in_chat(msg.content, msg.sender, msg.timestamp)
node.start()

# on user send:
msg = Message(content=user_text, sender=node.address)
node.broadcast(msg)
```

### Security team: `docs/contract_security.md`

Ship `sign(msg) → msg` and `verify(msg) → bool`. Distribution calls `sign(msg)` before sending and `verify(msg)` before accepting incoming messages. Sign the stable fields (`id`, `sender`, `timestamp`, `content`, with `signature=""` for canonicalization). **Do not sign `ttl` or `vector_clock`** — both are mutated in transit.

### Peer Discovery team: `docs/contract_peer_discovery.md`

Already wired via `MembershipRouter`. Confirm the event-name schema (`JOIN_ACCEPTED`, `HISTORY_BACKFILL_COMPLETE`, `DISCONNECT_SUSPECTED`, `RECONNECTED`, `LEAVE_CONFIRMED`, `DISCONNECT_TIMEOUT`) is final.

### History / Recovery & Storage team: `docs/contract_history.md`

Register a listener on `on_message` for logging. Replay backlog to newly-joined peers with `send_to_peer(host, port, msg)`, not `broadcast()` (otherwise recovery chunks are sent to every peer). Direct sends are copied with `ttl=0`, so the target receives the chunk but does not re-forward it. After replay completes, call `node.sync_vector_clock(recovered_vc)` so the causal layer is not blocked by live messages referencing replayed history.

---

## Delivery Guarantees

| Guarantee | Reality |
|---|---|
| Every online peer receives every message | Yes — ACK + 3 retries |
| Exactly once per peer | Yes — atomic UUID dedup |
| Causal order preserved | Yes — vector clocks + hold-back queue |
| Offline peers receive on reconnect | **No** — History team's replay path |
| Total order across concurrent messages | **No** — not a goal; concurrent messages have no defined cross-peer order |

---

## Design Decisions

| Decision | Rationale |
|---|---|
| Broadcast to all peers (not random fanout) | Guarantees no peer is skipped by chance. We accept the higher send cost in exchange for determinism. |
| Direct send for history replay | Recovery chunks should go only to the catching-up peer, not to the whole room. |
| ACK + retry with exponential backoff | Confirms each delivery; retries transient failures; gives up after 3 attempts and logs a warning the History team can act on. |
| WebSocket transport | Full-duplex — ACK travels back on the same connection. `asyncio` keeps the server non-blocking; a background thread bridges to sync callers. |
| Atomic `deduplicate()` | Check-and-mark in a single lock acquisition prevents races when two forwards arrive concurrently. |
| Vector clocks | Wall-clock timestamps can't establish causality; vector clocks do, with one integer per known sender. |
| Hold-back queue | Buffers out-of-order messages until predecessors arrive; drain cascades so a single delivery can unblock many. |
| Causal order, not total | Total ordering needs a coordinator, which contradicts the P2P design goal. Chat users only need replies to follow the messages they reply to. |
| `PeerRegistry` interface | Decouples us from discovery. `InMemoryRegistry` for tests, `MembershipRouter` for production. |
| MPI dropped as a transport | WebSockets + TCP sockets cover the P2P requirement; MPI assumes a static rank set at launch, which contradicts dynamic peer join/leave. |

---

## Known Limitations

- **Seen-set grows without bound.** Fine for demo scale; production would bound by time window or LRU.
- **Hold-back queue degrades to out-of-order delivery** if a predecessor message is permanently lost. After a 5-second timeout, stuck messages are delivered out-of-order with a warning rather than held indefinitely.
- **Offline delivery is out of scope.** The History team replays to reconnected peers.
- **No wire-level encryption.** The Security team signs; encryption is a stretch.

---

## Kubernetes Deployment and Causal-Order Verification

Runs the peer network as a 10-replica StatefulSet on kind or minikube, then
drives traffic through it and checks offline that no peer ever delivered a
message before one of its causal predecessors.

Nothing under `distribution/` changed. `VectorClock` and `HoldBackQueue` are
used exactly as written; identity and peer discovery are supplied through
`BroadcastNode`'s existing constructor arguments and the `PeerRegistry`
interface.

### What the deployment adds

| Path | Purpose |
|---|---|
| `Dockerfile` | Multi-stage build, non-root (UID 10001), read-only root filesystem |
| `deploy/identity.py` | Peer identity from the StatefulSet ordinal hostname |
| `deploy/dns_registry.py` | `PeerRegistry` backed by headless-Service DNS (SRV) |
| `deploy/peer_node.py` | Headless peer + control API the harness drives |
| `k8s/` | Namespace, headless Service, StatefulSet, kind cluster config |
| `harness/` | Driver, offline causal checker, chaos variant, summarizer |

### Identity: why the ordinal, not the pod IP

`BroadcastNode` keys its vector clock on `self.address`. `main.py` builds that
from `get_lan_ip()`, which inside Kubernetes resolves to the **pod IP** — and a
pod IP is reassigned on every restart. A peer that crashed and came back would
re-enter under a new clock key while every other peer kept a dead entry for the
old one, so the clock would lose continuity exactly when it matters.

A StatefulSet pod keeps its name across restarts, and the headless Service
publishes a per-pod DNS record for it. `deploy/identity.py` derives the
advertised address from that name:

```
peerchat-3.peerchat-hl.peerchat.svc.cluster.local:5678
```

The pod name comes from the downward API (`fieldRef: metadata.name`). Identity
is a pure function of the ordinal — no UUIDs, nothing generated at runtime — so
a restarted `peerchat-3` is the same clock key it was before.

### Discovery: headless-Service DNS, not a seed list

`DnsPeerRegistry` resolves `_chat._tcp.peerchat-hl.<ns>.svc.cluster.local` and
takes the SRV targets as the peer set. SRV targets are the stable per-pod names,
so discovery returns identities rather than IPs. Scaling the StatefulSet changes
the peer set with no config edit; there is no `bootstrap_peers` list.

The headless Service sets `publishNotReadyAddresses: true`. Without it the
StatefulSet deadlocks at boot: a peer is Ready only once it has found its
siblings, and DNS would only publish peers that are already Ready.

Results are cached for 5s because `BroadcastNode` calls `get_peers()` on every
forward. A failed lookup returns the last known good set rather than an empty
list, so a CoreDNS blip cannot silently empty the peer list mid-broadcast.

### Readiness

`/readyz` passes only after the peer completes a `hello`/`hello_ack` round trip
with `PEERCHAT_MIN_PEERS` siblings, using `BroadcastNode`'s own handshake
handler. A resolvable DNS name or an open TCP port is not enough — the gate
opens when the peer can actually exchange messages on the chat protocol.

Liveness (`/healthz`) is deliberately process-level only. A peer that is
isolated should keep its vector clock, not be restarted into an empty one.

### Running it

```bash
# 1. cluster + image
kind create cluster --config k8s/kind-cluster.yaml
scripts/build-load.sh

# 2. deploy 10 peers
kubectl apply -f k8s/namespace.yaml -f k8s/service-headless.yaml -f k8s/statefulset.yaml
kubectl -n peerchat wait --for=condition=Ready pod -l app=peerchat --timeout=300s

# 3. verification harness: <messages> <peers> <rate/sender> <label>
scripts/run-harness.sh 10000 10 25 run-10k-10peers
python3 harness/summarize.py results/run-10k-10peers.txt

# 4. chaos variant (deletes pods with kubectl during the run)
python3 -m harness.chaos --messages 2000 --kills 4 --kill-interval 12

# 5. latency vs cluster size (3 peers, then 10)
scripts/run-latency.sh

# teardown
kind delete cluster --name peerchat
```

`scripts/scale.sh N` resizes the cluster and keeps `PEERCHAT_MIN_PEERS` in step
with the replica count, which the readiness probe depends on.

### How the check works

Each peer records every delivery in the order `BroadcastNode` delivered it,
stamped with the vector clock that arrived on the message. The driver collects
all ten logs afterwards and replays each one offline against the CBCAST rule:

    (1) V(m)[s] == D[s] + 1          m is the next message p owes from s
    (2) V(m)[k] <= D[k]  for k != s  everything m depends on is already in

`harness/causal_check.py` deliberately does not import
`distribution.vector_clock`. Reusing `VectorClock.is_ready` as the oracle would
make the check tautological — a bug in the readiness predicate would validate
itself.

Because the peers' clocks keep advancing whether or not the logs are being
recorded, `/reset` also captures a **baseline clock**, and the replay starts
there. Without it, every first delivery after a reset reads as a violation.

---

## Team

| Member | Contribution |
|---|---|
| Bhuvana (POC) | Integration contracts; end-to-end test; report; README; PR coordination |
| Asha | Broadcast implementation; vector clock integration |
| Anukrithi | De-duplication + loop prevention; unit tests |
| Shamathmika | Vector clock design + implementation + unit tests + K8s deployment |
| Manasa | WebSocket transport |
| Peer-integration teammate | `MembershipRouter` (alignment with Peer Discovery's `MembershipService`) |
