package com.kvstore.cache;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe Least-Recently-Used cache written from scratch.
 *
 * <p>Internally uses a {@link HashMap} for O(1) key lookup and a
 * doubly-linked list for O(1) move-to-front and eviction. The two structures
 * are kept in sync at all times.
 *
 * <p><b>Why not LinkedHashMap?</b> PROJECT_CONTEXT §4 forbids it.
 * LinkedHashMap supports access-order mode, but it is not thread-safe and
 * doesn't expose hit/miss/eviction counters natively.
 *
 * <p><b>Why a plain ReentrantLock instead of ReadWriteLock?</b>
 * Every {@code get()} mutates the list (moves the node to the front), so
 * reads are never truly read-only. A ReadWriteLock's read-lock disallows
 * concurrent writes, but since every operation here writes to the list,
 * all threads would always compete for the write lock — giving exactly the
 * same serialization as a plain lock but with more overhead.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LruCache<K, V> {

    // -------------------------------------------------------------------------
    // Internal doubly-linked list node
    // -------------------------------------------------------------------------

    /** A node in the doubly-linked list. Also stored in the HashMap. */
    private static class Entry<K, V> {
        K key;
        V value;
        Entry<K, V> prev;
        Entry<K, V> next;

        Entry(K key, V value) {
            this.key   = key;
            this.value = value;
        }
    }

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    private final int capacity;

    /** O(1) key → list-node lookup. */
    private final HashMap<K, Entry<K, V>> map;

    // Sentinel (dummy) nodes at both ends of the list.
    // head.next is the MOST recently used entry.
    // tail.prev is the LEAST recently used entry (next to be evicted).
    // Using sentinels eliminates null-pointer edge cases for insert/remove.
    private final Entry<K, V> head; // sentinel MRU end
    private final Entry<K, V> tail; // sentinel LRU end

    // ReentrantLock: a plain mutex. Unlike synchronized, it lets us use
    // try-finally which is the idiomatic way to ensure the lock is always
    // released even if an exception is thrown.
    private final ReentrantLock lock = new ReentrantLock();

    // AtomicLong: thread-safe counters without needing the main lock.
    // We update them inside the lock anyway, but AtomicLong is the standard
    // idiom for stats counters and also allows reading them lock-free.
    private final AtomicLong hits      = new AtomicLong();
    private final AtomicLong misses    = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * @param capacity maximum number of entries. Must be ≥ 1.
     * @throws IllegalArgumentException if capacity ≤ 0
     */
    public LruCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("LruCache capacity must be > 0, got: " + capacity);
        this.capacity = capacity;
        this.map      = new HashMap<>(capacity * 2); // pre-size to avoid rehashing
        // Initialise sentinel list: head ↔ tail (empty list)
        this.head = new Entry<>(null, null);
        this.tail = new Entry<>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the value for {@code key}, or {@code null} if absent.
     * Moves the entry to the most-recently-used position.
     */
    public V get(K key) {
        lock.lock();
        try {
            Entry<K, V> entry = map.get(key);
            if (entry == null) {
                misses.incrementAndGet();
                return null;
            }
            hits.incrementAndGet();
            moveToFront(entry);
            return entry.value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts or updates {@code key → value}.
     * The entry is placed at the most-recently-used position.
     * If the cache is at capacity, the least-recently-used entry is evicted first.
     */
    public void put(K key, V value) {
        lock.lock();
        try {
            Entry<K, V> entry = map.get(key);
            if (entry != null) {
                // Update existing entry in place; move to front.
                entry.value = value;
                moveToFront(entry);
                return;
            }
            // New entry: evict LRU if at capacity.
            if (map.size() >= capacity) {
                evictLru();
            }
            Entry<K, V> newEntry = new Entry<>(key, value);
            insertAtFront(newEntry);
            map.put(key, newEntry);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the entry for {@code key}. No-op if not present.
     */
    public void remove(K key) {
        lock.lock();
        try {
            Entry<K, V> entry = map.remove(key);
            if (entry != null) {
                unlink(entry);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Removes all entries. */
    public void clear() {
        lock.lock();
        try {
            map.clear();
            head.next = tail;
            tail.prev = head;
        } finally {
            lock.unlock();
        }
    }

    /** Current number of entries (not the capacity). */
    public int size() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }

    /** Number of successful lookups since creation. */
    public long hits() { return hits.get(); }

    /** Number of failed lookups since creation. */
    public long misses() { return misses.get(); }

    /** Number of entries evicted due to capacity being exceeded. */
    public long evictions() { return evictions.get(); }

    /**
     * Hit rate = hits / (hits + misses). Returns 0.0 if no lookups yet.
     */
    public double hitRate() {
        long h = hits.get(), m = misses.get();
        long total = h + m;
        return total == 0 ? 0.0 : (double) h / total;
    }

    // -------------------------------------------------------------------------
    // Package-private validation helper (used by tests)
    // -------------------------------------------------------------------------

    /**
     * Validates internal invariants. Throws {@link AssertionError} on violation.
     *
     * <ol>
     *   <li>map.size() == list length</li>
     *   <li>Every list node's prev/next links are mutually consistent</li>
     *   <li>Every list node is in the map</li>
     * </ol>
     */
    void validate() {
        lock.lock();
        try {
            int listLen = 0;
            Entry<K, V> cur = head.next;
            while (cur != tail) {
                listLen++;
                // Check forward link consistency: cur.next.prev must be cur.
                if (cur.next.prev != cur) {
                    throw new AssertionError("Forward/backward link inconsistency at key=" + cur.key);
                }
                // Check the entry is in the map.
                if (!map.containsKey(cur.key)) {
                    throw new AssertionError("List entry not in map: key=" + cur.key);
                }
                cur = cur.next;
            }
            // Check tail sentinel backward link.
            if (tail.prev.next != tail) {
                throw new AssertionError("Tail sentinel backward link is broken");
            }
            if (listLen != map.size()) {
                throw new AssertionError("List length (" + listLen + ") != map size (" + map.size() + ")");
            }
        } finally {
            lock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Private list manipulation helpers (called while holding lock)
    // -------------------------------------------------------------------------

    /** Inserts {@code entry} immediately after the head sentinel (MRU position). */
    private void insertAtFront(Entry<K, V> entry) {
        entry.next       = head.next;
        entry.prev       = head;
        head.next.prev   = entry;
        head.next        = entry;
    }

    /** Unlinks {@code entry} from the list without touching the map. */
    private void unlink(Entry<K, V> entry) {
        entry.prev.next = entry.next;
        entry.next.prev = entry.prev;
        entry.prev = null; // help GC
        entry.next = null;
    }

    /** Moves an existing {@code entry} to the MRU (front) position. */
    private void moveToFront(Entry<K, V> entry) {
        unlink(entry);
        insertAtFront(entry);
    }

    /** Evicts the least-recently-used entry (the one before the tail sentinel). */
    private void evictLru() {
        Entry<K, V> lru = tail.prev;
        if (lru == head) return; // list is empty (shouldn't happen when capacity > 0)
        unlink(lru);
        map.remove(lru.key);
        evictions.incrementAndGet();
    }
}
