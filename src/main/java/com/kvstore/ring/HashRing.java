package com.kvstore.ring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent-hash ring backed by a {@link TreeMap}{@code <Long, Node>}.
 *
 * <p><b>Java beginner notes:</b>
 * <ul>
 *   <li><b>TreeMap</b>: a sorted map where keys are kept in natural (ascending)
 *       order. Here keys are {@code long} vnode positions on the hash ring.
 *       Lookups are O(log V) where V = total vnodes.</li>
 *   <li><b>ceilingEntry(k)</b>: returns the smallest entry whose key ≥ k.
 *       This is how we find "the next vnode clockwise from a given hash".</li>
 *   <li><b>ReentrantReadWriteLock</b>: allows many concurrent readers OR one
 *       exclusive writer, but never both at once. This is better than
 *       {@code synchronized} when reads dominate (which they do in a KV store).</li>
 * </ul>
 *
 * <p>Vnode positions: {@code HashFunction.hash(nodeId + "#" + i)} for i in
 * [0, vnodesPerNode). On a hash collision between two vnodes, the node with
 * the lexicographically smaller id wins (the other is silently discarded with
 * a warning log).
 *
 * <p>Thread-safety: all public methods acquire the appropriate lock.
 */
public class HashRing {

    private static final Logger log = LoggerFactory.getLogger(HashRing.class);

    /** The ring: vnode position → physical Node. Sorted by long key. */
    private final TreeMap<Long, Node> ring = new TreeMap<>();

    /** Reverse index: nodeId → set of vnode positions (for removeNode). */
    private final Map<String, Set<Long>> nodePositions = new HashMap<>();

    /** How many virtual nodes to place on the ring for each physical node. */
    private final int vnodesPerNode;

    // ReentrantReadWriteLock: many threads can hold the readLock simultaneously,
    // but writeLock is exclusive. Using it (instead of synchronized) avoids
    // contention on concurrent reads, which dominate KV workloads.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock  = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    /**
     * @param vnodesPerNode number of virtual nodes per physical node (e.g. 128).
     *                      Must be ≥ 1.
     */
    public HashRing(int vnodesPerNode) {
        if (vnodesPerNode < 1) throw new IllegalArgumentException("vnodesPerNode must be >= 1");
        this.vnodesPerNode = vnodesPerNode;
    }

    // -------------------------------------------------------------------------
    // Mutations (acquire writeLock)
    // -------------------------------------------------------------------------

    /**
     * Adds a physical node by placing {@code vnodesPerNode} virtual nodes onto
     * the ring. Idempotent: adding the same node twice is a no-op after the
     * first call.
     */
    public void addNode(Node node) {
        writeLock.lock();
        try {
            if (nodePositions.containsKey(node.id())) {
                log.warn("Node {} already in ring; ignoring addNode", node.id());
                return;
            }
            Set<Long> positions = new HashSet<>();
            for (int i = 0; i < vnodesPerNode; i++) {
                long pos = HashFunction.hash(node.id() + "#" + i);

                // Collision handling: on a position clash keep the node with
                // the lexicographically smaller id.
                if (ring.containsKey(pos)) {
                    Node existing = ring.get(pos);
                    if (node.id().compareTo(existing.id()) < 0) {
                        // New node wins: evict the old vnode at this position.
                        log.warn("Vnode collision at pos {} between '{}' and '{}'; '{}' wins (lex-smaller id)",
                                pos, existing.id(), node.id(), node.id());
                        // Remove the old position from the old node's position set.
                        Set<Long> oldSet = nodePositions.get(existing.id());
                        if (oldSet != null) oldSet.remove(pos);
                        ring.put(pos, node);
                        positions.add(pos);
                    } else {
                        log.warn("Vnode collision at pos {} between '{}' and '{}'; '{}' wins (lex-smaller id)",
                                pos, existing.id(), node.id(), existing.id());
                        // Existing wins; skip this vnode for the new node.
                    }
                } else {
                    ring.put(pos, node);
                    positions.add(pos);
                }
            }
            nodePositions.put(node.id(), positions);
            log.info("Added node '{}' with {} vnodes ({} placed)", node.id(), vnodesPerNode, positions.size());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Removes a physical node and all its vnodes from the ring.
     * No-op if the node is not present.
     */
    public void removeNode(Node node) {
        writeLock.lock();
        try {
            Set<Long> positions = nodePositions.remove(node.id());
            if (positions == null) {
                log.warn("removeNode called for unknown node '{}'; ignoring", node.id());
                return;
            }
            for (long pos : positions) {
                ring.remove(pos);
            }
            log.info("Removed node '{}' ({} vnodes)", node.id(), positions.size());
        } finally {
            writeLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Queries (acquire readLock)
    // -------------------------------------------------------------------------

    /**
     * Returns the primary (first) node responsible for {@code key}.
     *
     * <p>Lookup: hash the key → find the first vnode at or clockwise of that
     * position → return its physical node. O(log V).
     *
     * @throws IllegalStateException if the ring is empty
     */
    public Node getPrimary(String key) {
        readLock.lock();
        try {
            return primaryEntry(HashFunction.hash(key)).getValue();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns up to {@code n} distinct physical nodes starting at the primary
     * for {@code key} and walking clockwise. Stops after one full lap.
     *
     * <p>This is the "preference list" used by the replication coordinator.
     */
    public List<Node> getPreferenceList(String key, int n) {
        readLock.lock();
        try {
            if (ring.isEmpty()) return List.of();
            long startPos = HashFunction.hash(key);
            return buildPreferenceList(startPos, n);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns an unmodifiable view of all physical nodes currently in the ring.
     */
    public Set<Node> getNodes() {
        readLock.lock();
        try {
            // nodePositions keyset gives us unique node ids; map to Node via ring.
            Set<Node> result = new HashSet<>();
            for (Set<Long> positions : nodePositions.values()) {
                for (long pos : positions) {
                    Node n = ring.get(pos);
                    if (n != null) { result.add(n); break; } // any position gives the node
                }
            }
            return Collections.unmodifiableSet(result);
        } finally {
            readLock.unlock();
        }
    }

    /** Total number of virtual nodes currently on the ring. */
    public int vnodeCount() {
        readLock.lock();
        try {
            return ring.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns a map of nodeId → fraction of the hash space owned by that node.
     *
     * <p>Each vnode "owns" the arc from the previous vnode (exclusive) up to
     * itself (inclusive). Arc lengths are summed per physical node, then divided
     * by the total space (2^64) to get a fraction.
     *
     * <p>We use {@code double} arithmetic; there is a tiny rounding error that
     * is acceptable for diagnostic/stats purposes.
     */
    public Map<String, Double> ownership() {
        readLock.lock();
        try {
            if (ring.isEmpty()) return Map.of();

            // Total space = 2^64 (unsigned), but Java's long is signed.
            // We compute arc lengths as unsigned differences using Long.compareUnsigned
            // is tricky; instead we accumulate in a double by computing
            // (current_pos - prev_pos) treating all positions as unsigned 64-bit.
            // Total = 2^64 = 1.8446744e19 (as double).
            final double totalSpace = Math.pow(2, 64);

            Map<String, Double> arcSum = new HashMap<>();
            for (String id : nodePositions.keySet()) arcSum.put(id, 0.0);

            Long[] sortedPositions = ring.keySet().toArray(Long[]::new);
            // sortedPositions is already in ascending signed-long order (TreeMap).
            int sz = sortedPositions.length;

            for (int i = 0; i < sz; i++) {
                long cur  = sortedPositions[i];
                long prev = sortedPositions[(i - 1 + sz) % sz]; // wrap to last for i==0
                double arc;
                if (i == 0) {
                    // Arc from last vnode to first vnode (wraps around Long.MIN_VALUE).
                    // Unsigned difference: (cur - prev) interpreted as unsigned.
                    // In Java: Long.toUnsignedString, or compute via:
                    //   arc = (cur - prev) if cur > prev, else 2^64 + cur - prev
                    arc = toUnsignedDouble(cur) - toUnsignedDouble(prev);
                    if (arc <= 0) arc += totalSpace;
                } else {
                    arc = (double) (cur - prev); // prev < cur guaranteed (sorted)
                }
                String nodeId = ring.get(cur).id();
                arcSum.merge(nodeId, arc, Double::sum);
            }

            Map<String, Double> result = new LinkedHashMap<>();
            for (Map.Entry<String, Double> e : arcSum.entrySet()) {
                result.put(e.getKey(), e.getValue() / totalSpace);
            }
            return Collections.unmodifiableMap(result);
        } finally {
            readLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers (called while holding readLock or writeLock)
    // -------------------------------------------------------------------------

    /**
     * Finds the primary map entry for a given hash position.
     * Must be called while holding at least the read lock.
     */
    private Map.Entry<Long, Node> primaryEntry(long hash) {
        if (ring.isEmpty()) throw new IllegalStateException("Hash ring is empty");
        // ceilingEntry(hash): smallest key >= hash → "next clockwise vnode".
        Map.Entry<Long, Node> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            // Wrapped around the end of the sorted map → go to the first vnode.
            entry = ring.firstEntry();
        }
        return entry;
    }

    /**
     * Builds a preference list of up to {@code n} distinct physical nodes
     * starting from {@code startPos}. Stops after one full lap of the ring.
     * Must be called while holding at least the read lock.
     */
    private List<Node> buildPreferenceList(long startPos, int n) {
        List<Node> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        // Begin at the primary vnode for startPos.
        Map.Entry<Long, Node> startEntry = primaryEntry(startPos);
        long startKey = startEntry.getKey(); // actual position where we began

        Map.Entry<Long, Node> cur = startEntry;
        do {
            Node node = cur.getValue();
            if (seen.add(node.id())) { // add() returns true if the id was new
                result.add(node);
                if (result.size() == n) break;
            }
            // Move to the next vnode clockwise.
            Map.Entry<Long, Node> next = ring.higherEntry(cur.getKey());
            if (next == null) next = ring.firstEntry(); // wrap around
            cur = next;
            // Stop after one full lap (we've returned to where we started).
        } while (!cur.getKey().equals(startKey));

        return Collections.unmodifiableList(result);
    }

    /**
     * Converts a signed long to its unsigned double equivalent.
     * Long.MIN_VALUE → 2^63, Long.MAX_VALUE → 2^63 - 1.
     */
    private static double toUnsignedDouble(long v) {
        // Shift right by 1 to avoid overflow, then scale back.
        // Equivalent to treating the 64 bits as an unsigned value.
        return (double) (v >>> 1) * 2.0 + (double) (v & 1L);
    }
}
