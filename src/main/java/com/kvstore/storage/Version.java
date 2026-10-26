package com.kvstore.storage;

/**
 * Immutable version stamp: (timestamp in ms, nodeId).
 *
 * <p>Ordering rule (last-write-wins):
 * <ol>
 *   <li>Higher timestamp wins.</li>
 *   <li>Tie-break: lexicographically higher nodeId wins.</li>
 * </ol>
 *
 * <p>Java beginner note: {@code implements Comparable<Version>} lets us use
 * {@code v1.compareTo(v2)} to order versions, just like Comparable&lt;String&gt;
 * lets you sort strings. Records auto-generate equals/hashCode from all fields.
 *
 * @param timestamp milliseconds since epoch (System.currentTimeMillis())
 * @param nodeId    unique node identifier string
 */
public record Version(long timestamp, String nodeId) implements Comparable<Version> {

    public Version {
        if (nodeId == null || nodeId.isBlank())
            throw new IllegalArgumentException("nodeId must not be blank");
    }

    /**
     * Compares by timestamp first (larger = newer); nodeId as lexicographic
     * tie-breaker (larger string = newer, consistent with last-write-wins).
     */
    @Override
    public int compareTo(Version other) {
        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) return cmp;
        return this.nodeId.compareTo(other.nodeId);
    }

    /**
     * Returns {@code true} if this version is strictly newer than {@code other}.
     */
    public boolean isNewerThan(Version other) {
        return this.compareTo(other) > 0;
    }
}
