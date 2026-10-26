# Design Notes — Dynamo-style Distributed KV Store

> One 5-line entry per milestone (§10 rule 7): built · why · trade-off · alternative · risk.

---

## M0 — Project Skeleton

**Built:** Maven project (`com.kvstore/kv-store`, Java 17) with JUnit 5, Jackson, SLF4J-simple; eight packages (`ring`, `storage`, `cache`, `replication`, `membership`, `api`, `config`, `tools`) mirrored in test; `Main.java` stub; `.gitignore`; empty scaffolding dirs (`bench/`, `chaos/`, `docs/`, `scripts/`); CONFIG AND API CONTRACT appended to PROJECT_CONTEXT.md.
**Why:** A clean, reproducible build baseline with all names and contracts locked in prevents later rename churn; having the fat-jar wiring in place from day one means every milestone can be shipped and run immediately.
**Trade-off:** `slf4j-simple` logs to stderr with no per-package level control; acceptable for development but would be replaced with Logback in production (SLF4J facade makes this a one-line swap).
**Alternative:** Gradle instead of Maven — smaller boilerplate and better incremental compilation, but Maven's XML is more familiar for Java beginners and the ecosystem support is identical.
**Risk:** The Shade plugin's `META-INF/services` merge is easy to forget; without `ServicesResourceTransformer`, Jackson's module discovery breaks silently inside the fat-jar (already added to the config as a safeguard).

---

## M1 — Consistent Hash Ring + LRU Cache

**Built:** `HashRing` (TreeMap + ReentrantReadWriteLock, 128 vnodes/node, SHA-1 hash, collision resolution, ownership arcs, preference list); `HashFunction` (first 8 bytes of SHA-1 as big-endian long); `Node` (Java record); `LruCache` (HashMap + doubly-linked-list sentinel, ReentrantLock, hit/miss/eviction AtomicLong stats, package-private `validate()`); 35 passing JUnit 5 tests including a 8-thread × 50 k ops concurrency stress test.
**Why:** The ring and cache are the two data-structure cores that every later milestone depends on; building and testing them in isolation (with real numbers) catches correctness and balance issues before any networking code complicates debugging.
**Trade-off:** `ReentrantReadWriteLock` on `HashRing` allows concurrent reads but adds complexity; every `get()` on `LruCache` is a write (move-to-front), so the cache uses a plain `ReentrantLock` — a read-write lock there would give no concurrency benefit and would add overhead.
**Alternative:** MurmurHash3 or xxHash instead of SHA-1 — faster and has better avalanche properties, but SHA-1 is in the JDK with no extra dependency, and the balance test confirmed acceptable spread (node1 35.6%, node2 31.7%, node3 32.6% on 100 k keys; key movement 3→4 nodes: 23.4%, all moved to node4).
**Risk:** The vnode collision resolution (keep lex-smaller id) silently drops vnodes from the losing node, slightly reducing that node's hash-space share; with 128 vnodes across 2^64 positions the collision probability is negligible, but at very low vnode counts it could cause measurable imbalance.
