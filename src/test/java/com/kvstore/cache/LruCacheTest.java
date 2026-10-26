package com.kvstore.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LruCache}.
 * Covers eviction order, recency refresh, size stability, capacity=1 edge case,
 * remove, stats, and an 8-thread concurrency stress test.
 */
class LruCacheTest {

    // -------------------------------------------------------------------------
    // Basic put / get / eviction order
    // -------------------------------------------------------------------------

    @Test
    void putAndGet() {
        LruCache<String, Integer> cache = new LruCache<>(3);
        cache.put("a", 1);
        cache.put("b", 2);
        assertEquals(1, cache.get("a"));
        assertEquals(2, cache.get("b"));
        assertNull(cache.get("missing"));
    }

    @Test
    void evictionOrder_lruIsEvicted() {
        // capacity=3, insert a, b, c (order of LRU: c is MRU, a is LRU)
        // then insert d → a should be evicted.
        LruCache<String, String> cache = new LruCache<>(3);
        cache.put("a", "A");
        cache.put("b", "B");
        cache.put("c", "C");
        // Now evict: insert d; LRU is "a" (inserted first, never touched since).
        cache.put("d", "D");
        assertNull(cache.get("a"), "a should have been evicted");
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));
        assertEquals("D", cache.get("d"));
    }

    @Test
    void getRefreshesRecency() {
        LruCache<String, String> cache = new LruCache<>(3);
        cache.put("a", "A");
        cache.put("b", "B");
        cache.put("c", "C");
        // Touch "a" so it becomes MRU; now "b" is LRU.
        cache.get("a");
        cache.put("d", "D"); // evicts "b" (now LRU)
        assertNull(cache.get("b"), "b should be evicted after a was refreshed");
        assertNotNull(cache.get("a"), "a should still be present");
    }

    @Test
    void updatingExistingKeyDoesNotGrowSize() {
        LruCache<String, Integer> cache = new LruCache<>(3);
        cache.put("x", 1);
        cache.put("x", 2);
        cache.put("x", 3);
        assertEquals(1, cache.size(), "Size must stay 1 after repeated puts of the same key");
        assertEquals(3, cache.get("x"));
    }

    @Test
    void updateMovesToFront() {
        LruCache<String, String> cache = new LruCache<>(3);
        cache.put("a", "A");
        cache.put("b", "B");
        cache.put("c", "C");
        // Update "a" — it should become MRU; "b" is now LRU.
        cache.put("a", "A2");
        cache.put("d", "D"); // evicts "b"
        assertNull(cache.get("b"), "b should be evicted");
        assertEquals("A2", cache.get("a"), "a should still be present with updated value");
    }

    // -------------------------------------------------------------------------
    // Capacity = 1
    // -------------------------------------------------------------------------

    @Test
    void capacityOne() {
        LruCache<Integer, String> cache = new LruCache<>(1);
        cache.put(1, "one");
        assertEquals("one", cache.get(1));
        cache.put(2, "two"); // evicts 1
        assertNull(cache.get(1));
        assertEquals("two", cache.get(2));
        assertEquals(1, cache.size());
        assertEquals(1, cache.evictions());
    }

    // -------------------------------------------------------------------------
    // Remove
    // -------------------------------------------------------------------------

    @Test
    void remove_existingKey() {
        LruCache<String, String> cache = new LruCache<>(5);
        cache.put("k", "v");
        cache.remove("k");
        assertNull(cache.get("k"));
        assertEquals(0, cache.size());
        cache.validate(); // internal consistency
    }

    @Test
    void remove_missingKeyIsNoOp() {
        LruCache<String, String> cache = new LruCache<>(5);
        cache.put("a", "A");
        cache.remove("not-there"); // must not throw
        assertEquals(1, cache.size());
        cache.validate();
    }

    @Test
    void remove_makesRoomForNewEntry() {
        LruCache<String, String> cache = new LruCache<>(2);
        cache.put("a", "A");
        cache.put("b", "B");
        cache.remove("a");
        cache.put("c", "C"); // should not evict b
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));
        assertEquals(0, cache.evictions());
        cache.validate();
    }

    // -------------------------------------------------------------------------
    // Clear
    // -------------------------------------------------------------------------

    @Test
    void clear_emptiesCache() {
        LruCache<Integer, Integer> cache = new LruCache<>(5);
        for (int i = 0; i < 5; i++) cache.put(i, i);
        cache.clear();
        assertEquals(0, cache.size());
        for (int i = 0; i < 5; i++) assertNull(cache.get(i));
        cache.validate();
    }

    // -------------------------------------------------------------------------
    // Illegal capacity
    // -------------------------------------------------------------------------

    @Test
    void zeroCapacityThrows() {
        assertThrows(IllegalArgumentException.class, () -> new LruCache<>(0));
    }

    @Test
    void negativeCapacityThrows() {
        assertThrows(IllegalArgumentException.class, () -> new LruCache<>(-1));
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    @Test
    void stats_hitsAndMisses() {
        LruCache<String, String> cache = new LruCache<>(5);
        cache.put("a", "A");
        cache.get("a");     // hit
        cache.get("a");     // hit
        cache.get("nope");  // miss
        assertEquals(2, cache.hits());
        assertEquals(1, cache.misses());
        assertEquals(2.0 / 3.0, cache.hitRate(), 1e-9);
    }

    @Test
    void stats_evictionCount() {
        LruCache<Integer, Integer> cache = new LruCache<>(3);
        for (int i = 0; i < 6; i++) cache.put(i, i); // 3 evictions
        assertEquals(3, cache.evictions());
    }

    @Test
    void hitRate_noLookups_isZero() {
        LruCache<String, String> cache = new LruCache<>(5);
        assertEquals(0.0, cache.hitRate());
    }

    // -------------------------------------------------------------------------
    // validate() in simple cases
    // -------------------------------------------------------------------------

    @Test
    void validate_passesAfterNormalOperations() {
        LruCache<Integer, Integer> cache = new LruCache<>(4);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.get(1);
        cache.put(4, 40);
        cache.put(5, 50); // evicts LRU
        cache.remove(3);
        cache.validate();
    }

    // -------------------------------------------------------------------------
    // Concurrency stress test
    // -------------------------------------------------------------------------

    /**
     * 8 threads each perform 50,000 mixed get/put operations on a cache of
     * capacity 500. After all threads finish:
     * - No exceptions thrown.
     * - size() <= capacity.
     * - validate() passes (list and map are consistent).
     */
    @Test
    void concurrencyTest_8threads_50kOpsEach() throws InterruptedException {
        final int THREADS  = 8;
        final int OPS_EACH = 50_000;
        final int CAPACITY = 500;
        final int KEY_RANGE = 1_000; // keys 0..999

        LruCache<Integer, Integer> cache = new LruCache<>(CAPACITY);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        AtomicInteger completedOps = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(THREADS);

        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await(); // all threads start simultaneously
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    for (int i = 0; i < OPS_EACH; i++) {
                        int key = rng.nextInt(KEY_RANGE);
                        if (rng.nextBoolean()) {
                            cache.put(key, threadId * OPS_EACH + i);
                        } else {
                            cache.get(key);
                        }
                        completedOps.incrementAndGet();
                    }
                } catch (Throwable ex) {
                    errors.add(ex);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown(); // fire!
        boolean finished = done.await(30, TimeUnit.SECONDS);

        pool.shutdownNow();

        assertTrue(finished, "Concurrency test timed out after 30 seconds");
        assertTrue(errors.isEmpty(), "Exceptions during concurrency test: " + errors);
        assertTrue(cache.size() <= CAPACITY,
                "size() " + cache.size() + " exceeds capacity " + CAPACITY);
        cache.validate();

        System.out.printf("%n=== CONCURRENCY TEST ===%n");
        System.out.printf("  Threads: %d, ops each: %d, total ops: %d%n",
                THREADS, OPS_EACH, completedOps.get());
        System.out.printf("  Final cache size: %d / %d%n", cache.size(), CAPACITY);
        System.out.printf("  Hits: %d, Misses: %d, Evictions: %d, Hit rate: %.2f%%%n",
                cache.hits(), cache.misses(), cache.evictions(), cache.hitRate() * 100);
    }
}
