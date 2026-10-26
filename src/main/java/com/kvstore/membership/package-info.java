/**
 * Membership and heartbeat-based failure detection.
 * Static membership from env var PEERS. Heartbeat to every peer each HEARTBEAT_INTERVAL_MS.
 * Suspect node after HEARTBEAT_MISS_THRESHOLD consecutive misses; mark healthy when heartbeats resume.
 * Coordinator skips suspected nodes but still tries the rest of the preference list.
 * See PROJECT_CONTEXT §4 (Failure detection).
 */
package com.kvstore.membership;
