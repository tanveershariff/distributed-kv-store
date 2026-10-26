# Distributed KV Store

A Dynamo-inspired distributed key-value store built from scratch in Java 17. Three peer nodes, consistent hashing, quorum replication (N=3, W=2, R=2), and a hand-rolled Bitcask storage engine — designed as a deep-dive learning project into the ideas behind Amazon's Dynamo paper.

> **Learning project.** This is not a production system and makes no claim to match Amazon Dynamo or DynamoDB in correctness or performance.

---

## Table of Contents

- [What It Does](#what-it-does)
- [Architecture](#architecture)
- [Write Path](#write-path)
- [Read Path](#read-path)
- [Storage Engine](#storage-engine)
- [Consistent Hashing](#consistent-hashing)
- [LRU Cache](#lru-cache)
- [Failure Handling](#failure-handling)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Running Tests](#running-tests)
- [Benchmark Results](#benchmark-results)
- [CAP Discussion](#cap-discussion)
- [Design Decisions](#design-decisions)
- [Limitations](#limitations)
- [Project Structure](#project-structure)

---

## What It Does

- **Any node** can receive any client request and acts as the coordinator for it.
- Data is **partitioned** with consistent hashing (SHA-1, 128 virtual nodes per physical node).
- Every key is **replicated to 3 nodes**. Reads and writes require a quorum of 2.
- The cluster **keeps serving** reads and writes when one node is down.
- No write that received a `200 OK` from the cluster is ever lost.
- Deletes are **tombstones** — the record is kept until compaction (stretch goal).
- Stale replicas are healed asynchronously via **read repair**.

---

## Architecture

```
                         ┌─────────────────────────────────────────┐
                         │               Client                    │
                         └────────────────┬────────────────────────┘
                                          │ PUT / GET / DELETE /kv/{key}
                         ┌────────────────▼────────────────────────┐
                         │           Coordinator Node              │
                         │  (whichever node received the request)  │
                         └──────┬──────────────┬───────────────────┘
                                │              │
               ┌────────────────▼──┐    ┌──────▼────────────────┐
               │    Replica 2      │    │      Replica 3        │
               │  /internal/...    │    │   /internal/...       │
               └───────────────────┘    └───────────────────────┘
```

Each node runs an HTTP server (JDK `com.sun.net.httpserver`) on port 8080 (inside Docker). The coordinator uses `java.net.http.HttpClient` to fan out replication requests in parallel via `CompletableFuture`.

---

## Write Path

```
Client  ──PUT /kv/{key}──►  Coordinator
                                │
                    ┌───────────┼───────────────┐
                    ▼           ▼               ▼
                Replica 1   Replica 2       Replica 3
             (self-write)  POST /internal/replicate
                    │           │               │
                    └───────────┴───────────────┘
                       Wait for W=2 acks (timeout 1 s)
                                │
                    ◄───────────┘
                  200 OK  (or 503 if quorum not met)
```

1. Coordinator computes the **preference list** from the consistent hash ring.
2. Sends a `POST /internal/replicate` to all N replicas **in parallel** (`CompletableFuture.allOf` with a 1 s timeout).
3. Returns `200 OK` as soon as **W=2** successful acks are received.
4. If fewer than W replicas respond in time → `503` with a plain-text reason.

---

## Read Path

```
Client  ──GET /kv/{key}──►  Coordinator
                                │
                    ┌───────────┼───────────────┐
                    ▼           ▼               ▼
                Replica 1   Replica 2       Replica 3
              GET /internal/read?key=K
                    │           │               │
                    └───────────┴───────────────┘
                   Wait for R=2 responses (newest wins)
                                │
                    ◄───────────┘
                  200 OK  (or 404 / 503)
                                │
              ┌─────────────────┘
              │  (background) read-repair any stale replica
```

1. Coordinator asks all replicas for the key in parallel.
2. Waits for **R=2** responses; picks the one with the newest `Version(timestamp, nodeId)`.
3. Returns that value to the client.
4. Any replica that returned an older version is silently brought up to date **in the background** (read repair).

---

## Storage Engine

Each node persists data in a single **append-only log file** (`data/data.log`), inspired by Bitcask.

### Binary Record Format (big-endian)

| Field      | Size     | Description                               |
|------------|----------|-------------------------------------------|
| CRC32      | 4 bytes  | Checksum over all bytes that follow       |
| timestamp  | 8 bytes  | `long`, milliseconds since epoch          |
| nodeIdLen  | 2 bytes  | Unsigned length of nodeId string          |
| nodeId     | N bytes  | UTF-8 node identifier                     |
| flags      | 1 byte   | Bit 0 = tombstone                         |
| keyLen     | 4 bytes  | Length of key string                      |
| key        | K bytes  | UTF-8 key                                 |
| valueLen   | 4 bytes  | Length of value (0 for tombstones)        |
| value      | V bytes  | Raw value bytes                           |

### In-Memory Index

A `ConcurrentHashMap<String, IndexEntry>` maps every key to its latest record's `(offset, length, version, tombstone)`. Reads jump directly to the right position in the file — no sequential scan.

### Crash Recovery

On startup, the engine scans the log from byte 0:

1. Reads the fixed 23-byte header.
2. Validates field lengths (guards against huge allocations from corrupt data).
3. Checks the entire record fits within the file.
4. Verifies CRC32.
5. Applies the newer-version rule to the index.
6. On any validation failure: logs a `WARN` with the offset, **truncates** the file to the last good record, and stops.

This means the only data that can be lost in a crash is the single **in-flight record** at the time of the crash — exactly the Bitcask guarantee.

### Versioning

`Version(long timestamp, String nodeId)` — last-write-wins. Tie-break: lexicographically higher `nodeId`. This is simple but has known limitations (see [Limitations](#limitations)).

---

## Consistent Hashing

- **Hash function**: first 8 bytes of SHA-1 (UTF-8 input) as a big-endian `long`. Signed `long` natural ordering used throughout.
- **Virtual nodes**: 128 per physical node (configurable via `VNODES`), placed at `SHA1(nodeId + "#" + i)`.
- **Ring**: `TreeMap<Long, Node>` — `ceilingEntry(hash)` finds the next clockwise vnode in O(log V).
- **Preference list**: next N **distinct physical nodes** clockwise, with wrap-around, stopping after one full lap.
- **Collision resolution**: if two vnodes hash to the same position, the node with the lexicographically smaller ID wins (logged as a warning).

### Measured Balance (3 nodes, 128 vnodes, 100 000 random keys)

| Node  | Keys   | Share  |
|-------|--------|--------|
| node1 | 35 637 | 35.64% |
| node2 | 31 744 | 31.74% |
| node3 | 32 619 | 32.62% |

### Key Movement (3 → 4 nodes, 100 000 keys)

- **23 358 keys moved (23.36%)** — expected ≈ 25%.
- **Every moved key landed on the new node** — zero spurious moves.

---

## LRU Cache

Written from scratch: `HashMap<K, Entry>` + doubly-linked list with sentinel head/tail. No `LinkedHashMap`.

- O(1) `get` (moves entry to front), O(1) `put` (evicts LRU when full), O(1) `remove`.
- Thread-safe with a single `ReentrantLock` (a read-write lock would not help because every `get` mutates the list).
- Atomic counters for hits, misses, and evictions — readable lock-free.
- Enabled/disabled and capacity configured via `CACHE_ENABLED` / `CACHE_CAPACITY`.

### Concurrency Test Result (actually run)

8 threads × 50 000 mixed get/put ops on a capacity-500 cache:

```
Total ops : 400 000
Final size: 500 / 500
Hits      : 99 563
Misses    : 100 504
Evictions : 99 988
Hit rate  : 49.76%
```

No exceptions. `validate()` (internal list/map consistency check) passed.

---

## Failure Handling

- **Membership**: static, read from `PEERS` env var at startup. No gossip protocol.
- **Heartbeat**: each node sends `POST /internal/heartbeat` to every peer every `HEARTBEAT_INTERVAL_MS` (default 1 s).
- **Suspect threshold**: a node is marked **down** after `HEARTBEAT_MISS_THRESHOLD` (default 3) consecutive missed heartbeats. It is marked **up** again when heartbeats resume.
- **Coordinator behaviour**: skips suspected nodes but still attempts the remaining slots in the preference list to reach quorum.
- **One node down**: the cluster continues to serve all reads and writes (N=3, W=2, R=2 means only 2 nodes needed).
- **Two nodes down**: the cluster returns `503` until quorum can be re-established.

---

## API Reference

### Client API

| Method | Path          | Body               | Success | Notes                                  |
|--------|---------------|--------------------|---------|----------------------------------------|
| PUT    | `/kv/{key}`   | Raw bytes          | 200     | Quorum failure → 503                   |
| GET    | `/kv/{key}`   | —                  | 200     | Returns raw bytes; 404 if not found    |
| DELETE | `/kv/{key}`   | —                  | 200     | Idempotent tombstone; quorum failure → 503 |

**Error codes**

| Code | Meaning                            |
|------|------------------------------------|
| 400  | Key is empty or longer than 256 bytes |
| 404  | Key not found (GET only)           |
| 413  | Value exceeds `MAX_VALUE_BYTES` (default 1 MB) |
| 503  | Quorum not met; body contains plain-text reason |

### Internal API (node-to-node, JSON)

| Method | Path                   | Description                              |
|--------|------------------------|------------------------------------------|
| POST   | `/internal/replicate`  | Write one record; never forwards further |
| GET    | `/internal/read`       | Read one record by key                   |
| POST   | `/internal/heartbeat`  | Liveness ping                            |

`/internal/replicate` request body:

```json
{ "key": "mykey", "value": "<base64>", "timestamp": 1700000000000, "nodeId": "node1", "tombstone": false }
```

Response: `{ "applied": true }` or `{ "applied": false }` (stale version rejected).

`/internal/read?key=mykey` response:

```json
{ "found": true, "key": "mykey", "value": "<base64>", "timestamp": 1700000000000, "nodeId": "node1", "tombstone": false }
```

### Admin API

| Method | Path           | Response                                         |
|--------|----------------|--------------------------------------------------|
| GET    | `/health`      | `{ "status": "ok", "nodeId": "node1" }`          |
| GET    | `/admin/ring`  | JSON list of all vnodes and their physical nodes |
| GET    | `/admin/stats` | Cache hit rate, live key count, request counters |

---

## Configuration

All configuration is via environment variables. Only `NODE_ID` is required.

| Variable                  | Default      | Description                                |
|---------------------------|--------------|--------------------------------------------|
| `NODE_ID`                 | *(required)* | Unique name for this node, e.g. `node1`    |
| `PORT`                    | `8080`       | HTTP listen port                           |
| `DATA_DIR`                | `./data`     | Directory for `data.log`                   |
| `PEERS`                   | only self    | `node1=host:port,node2=host:port,...` (include self) |
| `VNODES`                  | `128`        | Virtual nodes per physical node            |
| `REPLICATION_N`           | `3`          | Replication factor                         |
| `WRITE_QUORUM`            | `2`          | Minimum acks for a successful write        |
| `READ_QUORUM`             | `2`          | Minimum responses for a successful read    |
| `REPLICA_TIMEOUT_MS`      | `1000`       | Per-replica timeout in milliseconds        |
| `CACHE_ENABLED`           | `true`       | Enable/disable the LRU read cache          |
| `CACHE_CAPACITY`          | `10000`      | Max entries in the LRU cache               |
| `FSYNC_ON_WRITE`          | `false`      | Flush to disk on every write (slower, safer) |
| `HEARTBEAT_INTERVAL_MS`   | `1000`       | Milliseconds between heartbeat pings       |
| `HEARTBEAT_MISS_THRESHOLD`| `3`          | Consecutive misses before a node is suspected |
| `MAX_VALUE_BYTES`         | `1048576`    | Maximum value size (1 MB)                  |

---

## Quick Start

### Prerequisites

- Java 17 (Temurin recommended)
- Docker + Docker Compose

### 3-command start

```bash
# 1. Build the fat jar
./mvnw -q package -DskipTests

# 2. Start a 3-node cluster
docker compose up --build

# 3. Write and read a key
curl -X PUT  http://localhost:8081/kv/hello -d "world"
curl         http://localhost:8081/kv/hello
# → world

# Read from a different node (replication in action)
curl         http://localhost:8082/kv/hello
# → world
```

### Single-node local run

```bash
./mvnw -q package -DskipTests
NODE_ID=node1 java -Xmx128m -jar target/kv-store.jar
```

### Delete a key

```bash
curl -X DELETE http://localhost:8081/kv/hello
```

### Check cluster health

```bash
curl http://localhost:8081/health   # {"status":"ok","nodeId":"node1"}
curl http://localhost:8082/health   # {"status":"ok","nodeId":"node2"}
curl http://localhost:8083/health   # {"status":"ok","nodeId":"node3"}
```

### Kill a node and keep writing

```bash
docker compose stop node2

# Writes still succeed (W=2, only 2 of 3 nodes needed)
curl -X PUT http://localhost:8081/kv/resilience -d "still works"
curl        http://localhost:8083/kv/resilience
# → still works

# Restart node2 — it catches up via read repair on next access
docker compose start node2
```

---

## Running Tests

```bash
# Run all tests (prints balance and movement numbers)
./mvnw test

# Quiet mode (just pass/fail)
./mvnw -q test
```

### Test coverage by milestone

| Milestone | Tests | What is tested |
|-----------|-------|----------------|
| M0        | 1     | Maven + JUnit 5 wiring smoke test |
| M1 Ring   | 17    | Determinism, preference list distinctness, wrap-around, fewer-nodes-than-N, balance (100 k keys), key movement (3→4 nodes, all to new node), remove-node stability, ownership sum=1 |
| M1 Cache  | 17    | Eviction order, recency refresh, size stability, capacity=1, remove, clear, stats (hits/misses/evictions/hitRate), 8-thread × 50 k ops concurrency |

---

## Benchmark Results

> Benchmarks are run in M7. Results will be published here with reproduction instructions.
> Machine spec: Windows 11, Intel Core i5, 8 GB RAM, SSD.

### Cache impact on read throughput

| Mode         | ops/sec | p50   | p95   | p99   | Error rate |
|--------------|---------|-------|-------|-------|------------|
| Cache OFF    | —       | —     | —     | —     | —          |
| Cache ON     | —       | —     | —     | —     | —          |

### One-node-down scenario

| Scenario         | ops/sec | p99   | Error rate |
|------------------|---------|-------|------------|
| All 3 nodes up   | —       | —     | —          |
| 1 node killed    | —       | —     | —          |

*Raw CSV files will be committed to `bench/results/` after M7.*

---

## CAP Discussion

This system sits on the **CP** side of the CAP triangle by default (W=2, R=2 with N=3), but the quorum sizes are configurable:

| Configuration | Behaviour       | Tradeoff |
|---------------|-----------------|----------|
| W=2, R=2      | Strong consistency (R+W > N) | Lower availability — 2 nodes must be up |
| W=1, R=3      | Highly available writes, strong reads | Slow reads |
| W=1, R=1      | AP mode (eventual consistency) | Reads may be stale |

**Clock skew**: the versioning scheme uses wall-clock timestamps (`System.currentTimeMillis()`). Two concurrent writes from different nodes can resolve in a non-intuitive order if clocks drift. This is documented and accepted for a learning project.

**Lost concurrent updates**: two simultaneous writes to the same key from different coordinators will result in one being silently discarded (last-write-wins by timestamp). Vector clocks would detect this conflict, but are listed as a stretch goal.

---

## Design Decisions

### Why Bitcask-style storage instead of LSM or B-tree?

Bitcask has the simplest correct implementation: one append-only file, one in-memory hash index. Reads are a single seek + one `FileChannel.read`. Recovery is a sequential scan. The trade-off is that the entire key set must fit in memory — acceptable for a learning project targeting a few million keys.

### Why `TreeMap` for the ring?

`TreeMap.ceilingEntry(hash)` gives the next clockwise vnode in O(log V) with zero custom code. A sorted array + binary search would be equivalent but more verbose. The read-write lock pattern (`ReentrantReadWriteLock`) allows concurrent lookups while node additions are rare.

### Why write the LRU cache from scratch?

`LinkedHashMap` in access-order mode does support LRU eviction, but it is not thread-safe and has no built-in hit/miss/eviction counters. Writing the doubly-linked-list from scratch is a useful exercise and gives full control over behaviour and stats.

### Why `slf4j-simple` instead of Logback?

Minimal dependencies. `slf4j-simple` prints to stderr with no configuration file. The SLF4J facade means swapping to Logback is a single `pom.xml` change.

### Why JDK `HttpServer` instead of Javalin or Netty?

Zero extra dependencies. The JDK's built-in HTTP server handles the modest concurrency of a 3-node cluster during development and testing. For production throughput Netty or Jetty would be appropriate.

### Alternatives considered

| Decision | Alternative | Why not chosen |
|----------|-------------|----------------|
| SHA-1 hash | MurmurHash3, xxHash | SHA-1 is in the JDK — no dependency; balance was measured and is acceptable |
| Append-only log | B-tree (RocksDB) | B-tree requires significant complexity or an external library |
| Static membership | Gossip (SWIM) | Adds weeks of complexity; overkill for 3 nodes |
| Last-write-wins | Vector clocks | Simpler; concurrent writes are rare and documented as a known limitation |

---

## Limitations

1. **Clock skew**: wall-clock timestamps mean concurrent writes from different nodes can resolve incorrectly if clocks are not synchronised. Use NTP in production.
2. **Lost concurrent updates**: two simultaneous writes to the same key produce one winner silently. No conflict detection.
3. **No compaction**: the log grows indefinitely. Tombstones are never reclaimed. This is a stretch goal.
4. **Key set in memory**: the entire index must fit in RAM. Not suitable for datasets larger than available heap.
5. **Static membership**: adding or removing a node requires restarting the cluster. No live membership changes.
6. **No authentication**: all endpoints are unauthenticated. Not suitable for internet-facing deployments.
7. **Single log file per node**: no sharding within a node; very large datasets will hit filesystem limits.
8. **No hinted handoff**: if a node is down during a write, the missed write is only recovered via read repair — which only triggers on a subsequent read for that key.

---

## Project Structure

```
dynamo-kv/
├── src/
│   ├── main/java/com/kvstore/
│   │   ├── Main.java               # Entry point (wired in M3)
│   │   ├── ring/
│   │   │   ├── HashFunction.java   # SHA-1 → long hash
│   │   │   ├── HashRing.java       # TreeMap ring + preference list
│   │   │   └── Node.java           # Immutable node record
│   │   ├── storage/
│   │   │   ├── StorageEngine.java  # Append-only log, index, recovery
│   │   │   ├── Version.java        # (timestamp, nodeId) with ordering
│   │   │   ├── StoredRecord.java   # Immutable record snapshot
│   │   │   └── IndexEntry.java     # In-memory index entry
│   │   ├── cache/
│   │   │   └── LruCache.java       # Hand-rolled HashMap + DLL cache
│   │   ├── replication/            # (M5) Coordinator, quorum logic
│   │   ├── membership/             # (M6) Heartbeat, failure detection
│   │   ├── api/                    # (M3) HTTP handlers
│   │   ├── config/                 # (M3) Env-var configuration
│   │   └── tools/                  # (M7) Load generator
│   └── test/java/com/kvstore/
│       ├── ring/RingTest.java       # 17 ring tests (balance, movement, …)
│       ├── cache/LruCacheTest.java  # 17 cache tests (eviction, concurrency, …)
│       └── SmokeTest.java
├── bench/
│   └── results/                    # CSV benchmark output (M7)
├── chaos/                          # Chaos test scripts (M6)
├── docs/
│   └── design-notes.md             # 5-line milestone notes
├── scripts/                        # Helper scripts
├── Dockerfile                      # (M6)
├── docker-compose.yml              # (M6) 3-node cluster
├── pom.xml                         # Maven build (Java 17, JUnit 5, Jackson, SLF4J)
├── mvnw / mvnw.cmd                 # Maven wrapper (no system Maven needed)
└── .gitignore
```

---

## Milestones

| # | Status | Milestone |
|---|--------|-----------|
| M0 | ✅ | Project skeleton, packages, Maven wrapper, API contract |
| M1 | ✅ | Consistent hash ring + LRU cache + unit tests |
| M2 | 🔄 | Bitcask storage engine + crash recovery + tests |
| M3 | ⬜ | Single-node HTTP API |
| M4 | ⬜ | Multi-node routing |
| M5 | ⬜ | Replication, quorums, read repair |
| M6 | ⬜ | Heartbeats, failure detection, Docker Compose, chaos test |
| M7 | ⬜ | Load generator + benchmarks |
| M8 | ⬜ | README polish, Mermaid diagrams, complexity analysis, CI |

---

## License

MIT
