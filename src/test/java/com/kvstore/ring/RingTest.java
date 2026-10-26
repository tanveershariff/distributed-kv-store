package com.kvstore.ring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HashRing and HashFunction.
 *
 * The balance test uses 3 nodes × 128 vnodes and 100,000 random keys.
 * Expected share ≈ 33%. Bounds are [25%, 42%] — deliberately tight so any
 * severe imbalance in the hash function is caught. If the real numbers land
 * outside these bounds the test fails loudly rather than silently widening.
 */
class RingTest {

    private static final Node N1 = new Node("node1", "localhost", 8081);
    private static final Node N2 = new Node("node2", "localhost", 8082);
    private static final Node N3 = new Node("node3", "localhost", 8083);
    private static final Node N4 = new Node("node4", "localhost", 8084);

    private HashRing ring;

    @BeforeEach
    void setUp() {
        ring = new HashRing(128);
        ring.addNode(N1);
        ring.addNode(N2);
        ring.addNode(N3);
    }

    // -------------------------------------------------------------------------
    // Determinism
    // -------------------------------------------------------------------------

    @Test
    void hashIsDeterministic() {
        long h1 = HashFunction.hash("hello");
        long h2 = HashFunction.hash("hello");
        assertEquals(h1, h2, "HashFunction must be deterministic");
    }

    @Test
    void differentKeysHaveDifferentHashes() {
        assertNotEquals(HashFunction.hash("a"), HashFunction.hash("b"));
        assertNotEquals(HashFunction.hash("node1#0"), HashFunction.hash("node2#0"));
    }

    // -------------------------------------------------------------------------
    // Lookup correctness
    // -------------------------------------------------------------------------

    @Test
    void getPrimaryReturnsSomeNode() {
        Node primary = ring.getPrimary("testkey");
        assertNotNull(primary);
        assertTrue(ring.getNodes().contains(primary));
    }

    @Test
    void getPrimaryIsDeterministic() {
        String key = "deterministic-key-test";
        Node first  = ring.getPrimary(key);
        Node second = ring.getPrimary(key);
        assertEquals(first, second);
    }

    @Test
    void emptyRingThrows() {
        HashRing empty = new HashRing(128);
        assertThrows(IllegalStateException.class, () -> empty.getPrimary("k"));
    }

    // -------------------------------------------------------------------------
    // Preference list
    // -------------------------------------------------------------------------

    @Test
    void preferenceListHasDistinctNodes() {
        List<Node> pref = ring.getPreferenceList("mykey", 3);
        assertEquals(3, pref.size(), "Should return 3 distinct nodes");
        // All must be distinct by id.
        Set<String> ids = new HashSet<>();
        for (Node n : pref) assertTrue(ids.add(n.id()), "Duplicate node in preference list: " + n.id());
    }

    @Test
    void preferenceListWrapAround() {
        // Even if the key hashes to near Long.MAX_VALUE, we should still get N distinct nodes.
        // We'll test many keys to increase the probability of hitting a wrap-around.
        for (int i = 0; i < 500; i++) {
            String key = "wrapkey-" + i;
            List<Node> pref = ring.getPreferenceList(key, 3);
            assertEquals(3, pref.size(), "Wrap-around failed for key " + key);
            Set<String> ids = new HashSet<>();
            for (Node n : pref) assertTrue(ids.add(n.id()));
        }
    }

    @Test
    void preferenceListFewerNodesThanN() {
        // With only 2 nodes asking for n=5 should return 2.
        HashRing small = new HashRing(128);
        small.addNode(N1);
        small.addNode(N2);
        List<Node> pref = small.getPreferenceList("somekey", 5);
        assertEquals(2, pref.size(), "Should cap at the number of physical nodes");
        Set<String> ids = new HashSet<>();
        for (Node n : pref) assertTrue(ids.add(n.id()));
    }

    @Test
    void preferenceListSingleNode() {
        HashRing single = new HashRing(128);
        single.addNode(N1);
        List<Node> pref = single.getPreferenceList("k", 3);
        assertEquals(1, pref.size());
        assertEquals(N1.id(), pref.get(0).id());
    }

    // -------------------------------------------------------------------------
    // BALANCE TEST
    // -------------------------------------------------------------------------

    @Test
    void balanceTest_3nodes_128vnodes_100kKeys() {
        // Fixed seed for reproducibility.
        Random rng = new Random(42L);
        int totalKeys = 100_000;
        Map<String, Integer> counts = new HashMap<>();
        counts.put(N1.id(), 0);
        counts.put(N2.id(), 0);
        counts.put(N3.id(), 0);

        for (int i = 0; i < totalKeys; i++) {
            String key = Long.toHexString(rng.nextLong()) + "-" + i;
            Node primary = ring.getPrimary(key);
            counts.merge(primary.id(), 1, Integer::sum);
        }

        System.out.println("\n=== BALANCE TEST: 3 nodes, 128 vnodes, 100,000 keys ===");
        for (Map.Entry<String, Integer> e : new TreeMap<>(counts).entrySet()) {
            double pct = 100.0 * e.getValue() / totalKeys;
            System.out.printf("  %-8s : %6d keys  (%.2f%%)%n", e.getKey(), e.getValue(), pct);
        }
        System.out.printf("  Expected ≈ 33.33%% each; bounds [25%%, 42%%]%n");

        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            double share = (double) e.getValue() / totalKeys;
            assertTrue(share >= 0.25 && share <= 0.42,
                    String.format("Node %s has %.2f%% share — outside [25%%, 42%%]. " +
                            "Real count: %d / %d. The hash function may be unbalanced.",
                            e.getKey(), share * 100, e.getValue(), totalKeys));
        }
    }

    // -------------------------------------------------------------------------
    // KEY MOVEMENT TEST: 3 → 4 nodes
    // -------------------------------------------------------------------------

    @Test
    void keyMovementTest_3to4nodes() {
        int totalKeys = 100_000;
        Random rng = new Random(99L);
        String[] keys = new String[totalKeys];
        Map<String, Node> before = new HashMap<>();

        for (int i = 0; i < totalKeys; i++) {
            keys[i] = Long.toHexString(rng.nextLong()) + "-" + i;
            before.put(keys[i], ring.getPrimary(keys[i]));
        }

        // Add a fourth node.
        ring.addNode(N4);

        Map<String, Node> after = new HashMap<>();
        for (String key : keys) {
            after.put(key, ring.getPrimary(key));
        }

        int movedCount  = 0;
        int wrongTarget = 0;
        for (String key : keys) {
            Node fromNode = before.get(key);
            Node toNode   = after.get(key);
            if (!fromNode.id().equals(toNode.id())) {
                movedCount++;
                // Every moved key must have moved TO the new node N4.
                if (!toNode.id().equals(N4.id())) wrongTarget++;
            }
        }

        double movedFraction = (double) movedCount / totalKeys;
        System.out.printf("%n=== KEY MOVEMENT TEST: 3 → 4 nodes, 100,000 keys ===%n");
        System.out.printf("  Keys moved   : %d (%.2f%%)%n", movedCount, movedFraction * 100);
        System.out.printf("  Moved to N4  : %d%n", movedCount - wrongTarget);
        System.out.printf("  Moved to other (BUG if > 0): %d%n", wrongTarget);
        System.out.printf("  Expected ≈ 25%% moved; bounds [15%%, 35%%]%n");

        assertEquals(0, wrongTarget,
                "Some keys moved to a node other than N4 — this is a ring correctness bug.");

        assertTrue(movedFraction >= 0.15 && movedFraction <= 0.35,
                String.format("%.2f%% of keys moved — outside [15%%, 35%%]. " +
                        "Real count: %d / %d.", movedFraction * 100, movedCount, totalKeys));
    }

    // -------------------------------------------------------------------------
    // Remove node: non-owned keys must not move
    // -------------------------------------------------------------------------

    @Test
    void removeNode_nonOwnedKeysDontMove() {
        // Record assignments for 10,000 keys.
        int totalKeys = 10_000;
        Random rng = new Random(7L);
        String[] keys = new String[totalKeys];
        Map<String, Node> before = new HashMap<>();
        for (int i = 0; i < totalKeys; i++) {
            keys[i] = "rmkey-" + i + "-" + rng.nextInt(1_000_000);
            before.put(keys[i], ring.getPrimary(keys[i]));
        }

        // Remove node2.
        ring.removeNode(N2);

        // For every key that was NOT owned by node2, the primary must not change.
        int violations = 0;
        for (String key : keys) {
            Node wasOwner = before.get(key);
            if (wasOwner.id().equals(N2.id())) continue; // expected to move, skip
            Node nowOwner = ring.getPrimary(key);
            if (!nowOwner.id().equals(wasOwner.id())) {
                System.err.printf("Key '%s' moved from %s to %s unexpectedly%n",
                        key, wasOwner.id(), nowOwner.id());
                violations++;
            }
        }
        assertEquals(0, violations, violations + " keys moved unexpectedly after node removal.");
    }

    // -------------------------------------------------------------------------
    // vnodeCount and getNodes
    // -------------------------------------------------------------------------

    @Test
    void vnodeCountReflectsAddedNodes() {
        // 3 nodes × 128 vnodes = 384 (assuming no collisions, which is extremely
        // unlikely with SHA-1 and only 384 positions across 2^64 possible values).
        int count = ring.vnodeCount();
        assertTrue(count >= 380 && count <= 384,
                "Expected ~384 vnodes, got " + count);
    }

    @Test
    void getNodesReturnsAllAddedNodes() {
        Set<Node> nodes = ring.getNodes();
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(N1));
        assertTrue(nodes.contains(N2));
        assertTrue(nodes.contains(N3));
    }

    @Test
    void getNodesAfterRemove() {
        ring.removeNode(N2);
        Set<Node> nodes = ring.getNodes();
        assertEquals(2, nodes.size());
        assertFalse(nodes.contains(N2));
    }

    // -------------------------------------------------------------------------
    // Ownership
    // -------------------------------------------------------------------------

    @Test
    void ownershipSumsToOne() {
        Map<String, Double> own = ring.ownership();
        assertEquals(3, own.size());
        double total = own.values().stream().mapToDouble(Double::doubleValue).sum();
        // Floating point: allow 0.01% error.
        assertEquals(1.0, total, 0.0001,
                "Ownership fractions must sum to 1.0, got " + total);
    }

    @Test
    void ownershipRoughlyEqual() {
        Map<String, Double> own = ring.ownership();
        System.out.println("\n=== OWNERSHIP (arc-length fractions) ===");
        for (Map.Entry<String, Double> e : new TreeMap<>(own).entrySet()) {
            System.out.printf("  %-8s : %.4f (%.2f%%)%n",
                    e.getKey(), e.getValue(), e.getValue() * 100);
        }
        for (double f : own.values()) {
            assertTrue(f >= 0.20 && f <= 0.46,
                    "Ownership fraction " + f + " is outside [20%, 46%]");
        }
    }
}
