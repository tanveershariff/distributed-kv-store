/**
 * Replication coordinator: parallel writes via CompletableFuture, quorum reads, read-repair.
 * N=3, W=2, R=2 (all configurable). Timeout per replica = REPLICA_TIMEOUT_MS.
 * Read repair: after returning to client, asynchronously push newest version to stale replicas.
 * See PROJECT_CONTEXT §4 (Replication and quorums).
 */
package com.kvstore.replication;
