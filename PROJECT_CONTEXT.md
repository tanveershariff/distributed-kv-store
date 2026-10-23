# PROJECT_CONTEXT.md — Dynamo-style Distributed KV Store (Java 17)

Read this file fully before every task. It is the source of truth.

## 1. What we are building
A cluster of 3 peer nodes storing key-value data. Any node can receive any client request and acts as coordinator for it. Data is partitioned with consistent hashing and replicated to N=3 nodes (W=2, R=2). The cluster keeps serving reads and writes when ONE node dies, and no acknowledged write is lost. Inspired by the Amazon Dynamo paper. It is a learning project. NEVER claim it matches Dynamo or DynamoDB.

## 2. Owner context
Final-year CS student (India), strong in Node.js/TypeScript, working-level Java. Explain Java-specific concepts in short comments or notes. Code must COMPILE AND RUN. Owner runs every command.

## 3. Tech stack (keep minimal)
- Java 17, Maven, JUnit 5
- JDK built-in com.sun.net.httpserver.HttpServer (no Javalin, fewer dependencies)
- Jackson (JSON), SLF4J + slf4j-simple (logging)
- java.net.http.HttpClient for node-to-node calls
- Docker + Docker Compose (3 nodes), GitHub Actions CI
- Dev machine: Windows, 8 GB RAM. Use small JVM heaps (-Xmx128m to -Xmx192m per node).

## 4. Design (follow unless you show a better option and explain it)
### Consistent hashing
- TreeMap<Long, Node> ring, 128 vnodes per physical node (configurable).
- Hash: first 8 bytes of SHA-1 as a long. Use the SAME hash everywhere.
- Preference list = next N DISTINCT physical nodes clockwise (wrap around). Lookup O(log V).
### Replication and quorums
- N=3, W=2, R=2, all configurable.
- Write: coordinator sends to all N replicas in parallel (CompletableFuture + timeout), success when W acks.
- Read: ask replicas, wait for R responses, return newest version, repair stale replicas in the background (read repair).
### Versioning
- Version = (timestamp, nodeId). Last-write-wins. Tie-break by nodeId. Document limitations (clock skew, lost concurrent updates).
### Failure detection
- Static membership from env vars / config. Heartbeat to every peer each 1s. Suspect after 3 missed; healthy again when heartbeats resume.
- Coordinator skips down peers but still uses the rest of the preference list.
### Storage (Bitcask-style, per node)
- Append-only log. Record: key, value, version, tombstone flag, CRC32 checksum.
- In-memory HashMap<key, (offset, length, version)>. Reads use FileChannel, one record per read.
- Startup: rebuild index by scanning log. Safely handle a truncated last record (stop and truncate).
- Deletes are tombstones.
### LRU cache (write from scratch)
- HashMap + doubly linked list, O(1) get/put, fixed capacity, thread-safe. DO NOT use LinkedHashMap.
- Sits in front of storage disk reads; updated/invalidated on put and delete.
- Capacity and on/off switch configurable (env vars) for benchmarking.
### APIs
- Client: PUT /kv/{key} (body = value), GET /kv/{key}, DELETE /kv/{key}
- Internal: /internal/replicate, /internal/read, /internal/heartbeat
- Admin: GET /health, GET /admin/ring, GET /admin/stats (cache hit rate, key count, request counts)

## 5. Package layout
com.yourname.kv.{ring, storage, cache, replication, membership, api, config}
src/test/java mirrors main. Also: bench/, chaos/, docs/, Dockerfile, docker-compose.yml, README.md, .github/workflows/ci.yml

## 6. Milestones (ONE per task, never jump ahead)
M1 ring + LRU + tests | M2 storage engine + recovery + tests | M3 single-node HTTP API |
M4 multi-node routing | M5 replication + quorums + read repair |
M6 heartbeats + failure detection + Docker Compose + chaos test |
M7 load generator + benchmarks | M8 README, Mermaid diagram, docs/complexity.md, CI
Stretch (NOT now): hinted handoff, compaction, vector clocks.

## 7. Required tests
- Ring: lookup; balance (3 nodes, 128 vnodes, 100k random keys, each ~1/3, PRINT the spread); adding a node moves ~1/N keys.
- LRU: eviction order, capacity, concurrency.
- Storage: recovery after simulated crash, truncated last record.
- Integration: start cluster, write 10k keys, read all back.
- Chaos (script): write continuously, kill one node, keep writing/reading, verify every write that returned success is still readable; restart node and verify catch-up.
  State exactly: one node failure at a time, N=3, W=2, R=2.

## 8. Benchmarks (REAL NUMBERS ONLY)
Load generator: threads, duration, read/write ratio (90/10, 50/50), key distribution (uniform, Zipfian). Report ops/sec, p50/p95/p99, error rate. Runs: (a) cache off vs on, (b) one node killed mid-run. Save raw CSV in bench/results/. Record machine specs.
NEVER invent, estimate or fill in numbers. If a result is unimpressive, report it honestly. If a claim cannot be supported, say so.

## 9. Docs required
README (overview, Mermaid diagram, write/read flow, CAP discussion, limitations, design decisions + alternatives, 3-command quick start with curl, benchmark results + reproduction). docs/complexity.md (time/space for ring lookup, add node, LRU get/put, storage get/put/delete, index rebuild, replicated put/get incl. network round trips).

## 10. Rules for the agent
1. Provide COMPLETE files, no "..." fragments.
2. After each milestone: run `mvn -q test`, show the result, and give exact run commands.
3. Add short comments on non-obvious Java concepts (synchronized, ReentrantLock, CompletableFuture, FileChannel).
4. Proper error handling, timeouts, logging. No swallowed exceptions.
5. Do not add dependencies without saying why.
6. Do not claim anything is tested that has not actually been run.
7. After each milestone, write a 5-line note in docs/design-notes.md: what was built, why, trade-off, alternative, one thing that could go wrong.
8. Out of scope: auth, UI, multi-DC, SQL, transactions, Raft/Paxos.
