package com.kvstore.storage;

import com.kvstore.cache.LruCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Bitcask-style single-file append-only storage engine.
 *
 * <h2>Binary record layout (big-endian)</h2>
 * <pre>
 *  [4 bytes] CRC32      — checksum over all bytes that follow in this record
 *  [8 bytes] timestamp  — long, milliseconds since epoch
 *  [2 bytes] nodeIdLen  — unsigned short
 *  [N bytes] nodeId     — UTF-8
 *  [1 byte]  flags      — bit 0 = tombstone
 *  [4 bytes] keyLen     — int
 *  [K bytes] key        — UTF-8
 *  [4 bytes] valueLen   — int (0 for tombstones)
 *  [V bytes] value bytes
 * </pre>
 *
 * <h2>Java beginner notes</h2>
 * <ul>
 *   <li><b>FileChannel</b>: Java NIO class for random-access file I/O.
 *       {@code channel.read(buffer, offset)} reads bytes at a specific byte
 *       position without changing a shared file pointer — so multiple threads
 *       can call it simultaneously without a lock.</li>
 *   <li><b>ByteBuffer</b>: fixed-size byte array with a cursor.
 *       {@code allocate(n)} creates one; {@code flip()} switches from
 *       write-mode to read-mode; {@code putInt/getLong} use big-endian order.</li>
 *   <li><b>ReentrantLock (writeLock)</b>: only one thread appends at a time.
 *       Reads are entirely lock-free because positional FileChannel reads are
 *       thread-safe.</li>
 *   <li><b>ConcurrentHashMap</b>: the index is read by many threads lock-free;
 *       the single writer updates it after the file write is durable.</li>
 * </ul>
 */
public class StorageEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    // ── Layout constants ───────────────────────────────────────────────────────
    private static final String LOG_FILE_NAME = "data.log";

    // Fixed header: crc(4) + ts(8) + nodeIdLen(2) + flags(1) + keyLen(4) + valueLen(4)
    private static final int FIXED_HEADER = 4 + 8 + 2 + 1 + 4 + 4; // 23 bytes

    // Recovery guard: reject records with any field larger than 10 MB.
    private static final int MAX_FIELD_BYTES = 10 * 1024 * 1024;

    // ── State ──────────────────────────────────────────────────────────────────

    private final FileChannel channel;
    private final Path        logPath;
    private final boolean     fsyncOnWrite;

    /**
     * In-memory index: key → (offset, length, version, tombstone).
     * ConcurrentHashMap allows lock-free reads from any thread.
     */
    private final ConcurrentHashMap<String, IndexEntry> index = new ConcurrentHashMap<>();

    /** Serialises all appends. Reads never acquire this lock. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /** Optional read cache. {@code null} when cacheEnabled=false. */
    private final LruCache<String, StoredRecord> cache;

    // ── Constructor ────────────────────────────────────────────────────────────

    /**
     * Opens (or creates) the storage engine and runs crash recovery.
     *
     * @param dataDir       directory that will hold data.log; created if absent
     * @param fsyncOnWrite  flush to disk after every write
     * @param cacheEnabled  wrap disk reads with an LRU cache
     * @param cacheCapacity max entries in the cache (ignored when disabled)
     */
    public StorageEngine(Path dataDir, boolean fsyncOnWrite,
                         boolean cacheEnabled, int cacheCapacity) throws IOException {
        this.fsyncOnWrite = fsyncOnWrite;
        this.cache        = cacheEnabled ? new LruCache<>(cacheCapacity) : null;

        Files.createDirectories(dataDir);
        logPath = dataDir.resolve(LOG_FILE_NAME);

        // Open READ + WRITE + CREATE. We do NOT use APPEND mode because we need
        // full positional control (both for writes at the tail and reads anywhere).
        channel = FileChannel.open(logPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

        recover();
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    /**
     * Writes key → value if {@code v} is newer than any existing version.
     *
     * @return {@code true} if applied; {@code false} if rejected (stale version)
     */
    public boolean put(String key, byte[] value, Version v) throws IOException {
        writeLock.lock();
        try {
            IndexEntry cur = index.get(key);
            if (cur != null && !v.isNewerThan(cur.version())) return false;

            long offset = appendRecord(key, value, v, false);
            int  len    = recordLen(key, v.nodeId(), value.length);
            index.put(key, new IndexEntry(offset, len, v, false));

            if (cache != null) cache.put(key, new StoredRecord(key, value, v, false));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Appends a tombstone if {@code v} is newer than any existing version.
     *
     * @return {@code true} if applied; {@code false} if stale
     */
    public boolean delete(String key, Version v) throws IOException {
        writeLock.lock();
        try {
            IndexEntry cur = index.get(key);
            if (cur != null && !v.isNewerThan(cur.version())) return false;

            long offset = appendRecord(key, new byte[0], v, true);
            int  len    = recordLen(key, v.nodeId(), 0);
            index.put(key, new IndexEntry(offset, len, v, true));

            if (cache != null) cache.remove(key);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the record for {@code key}, including tombstones.
     * {@link Optional#empty()} means the key was never written to this node.
     */
    public Optional<StoredRecord> get(String key) throws IOException {
        IndexEntry entry = index.get(key);
        if (entry == null) return Optional.empty();

        // Cache check — lock-free because LruCache is internally synchronized.
        if (cache != null) {
            StoredRecord cached = cache.get(key);
            if (cached != null) return Optional.of(cached);
        }

        // Disk read: allocate an exact-fit buffer and read the whole record.
        ByteBuffer buf = ByteBuffer.allocate(entry.length());
        readFully(buf, entry.offset());
        buf.flip();

        StoredRecord rec = parseAndVerify(buf, entry.offset());

        if (cache != null) cache.put(key, rec);
        return Optional.of(rec);
    }

    /** Number of live (non-tombstone) keys. */
    public long liveKeyCount() {
        return index.values().stream().filter(e -> !e.tombstone()).count();
    }

    /** Total distinct keys ever written (live + tombstones). */
    public long totalKeyCount() { return index.size(); }

    // ── Cache stat accessors ──────────────────────────────────────────────────

    public long   cacheHits()      { return cache != null ? cache.hits()      : 0; }
    public long   cacheMisses()    { return cache != null ? cache.misses()    : 0; }
    public long   cacheEvictions() { return cache != null ? cache.evictions() : 0; }
    public double cacheHitRate()   { return cache != null ? cache.hitRate()   : 0.0; }
    public int    cacheSize()      { return cache != null ? cache.size()      : 0; }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }

    // ── Recovery ───────────────────────────────────────────────────────────────

    /**
     * Scans data.log from offset 0, rebuilds the index, and truncates any
     * corrupt or incomplete tail record.
     *
     * <p>Safety rules applied on every record:
     * <ol>
     *   <li>Enough bytes remain for the fixed header.</li>
     *   <li>No field length exceeds MAX_FIELD_BYTES (avoids OOM on corrupt data).</li>
     *   <li>The complete record fits within the current file size.</li>
     *   <li>CRC32 of bytes [4, totalLen) matches the stored CRC at bytes [0, 4).</li>
     * </ol>
     */
    private void recover() throws IOException {
        long fileSize = channel.size();
        if (fileSize == 0) return;

        log.info("Recovering {} ({} bytes)", logPath, fileSize);
        long offset          = 0;
        long lastGoodOffset  = 0;
        int  recordsIndexed  = 0;

        while (offset < fileSize) {
            long remaining = fileSize - offset;

            // ── 1. Read fixed header ───────────────────────────────────────────
            if (remaining < FIXED_HEADER) {
                log.warn("Incomplete header at offset {} ({} bytes left, need {}); truncating",
                        offset, remaining, FIXED_HEADER);
                break;
            }
            ByteBuffer hdr = ByteBuffer.allocate(FIXED_HEADER);
            readFully(hdr, offset);
            hdr.flip();

            int  storedCrc = hdr.getInt();
            long timestamp = hdr.getLong();
            int  nodeIdLen = hdr.getShort() & 0xFFFF; // unsigned
            byte flags     = hdr.get();
            int  keyLen    = hdr.getInt();
            int  valueLen  = hdr.getInt();

            // ── 2. Sanity-check lengths ────────────────────────────────────────
            if (nodeIdLen <= 0 || nodeIdLen > MAX_FIELD_BYTES
                    || keyLen    <= 0 || keyLen    > MAX_FIELD_BYTES
                    || valueLen  <  0 || valueLen  > MAX_FIELD_BYTES) {
                log.warn("Absurd lengths at offset {} (nodeIdLen={} keyLen={} valueLen={}); truncating",
                        offset, nodeIdLen, keyLen, valueLen);
                break;
            }

            int totalLen = FIXED_HEADER + nodeIdLen + keyLen + valueLen;

            // ── 3. Full record must fit in file ────────────────────────────────
            if (offset + totalLen > fileSize) {
                log.warn("Incomplete record at offset {} (need {} bytes, {} available); truncating",
                        offset, totalLen, remaining);
                break;
            }

            // ── 4. Read full record and verify CRC ────────────────────────────
            ByteBuffer rec = ByteBuffer.allocate(totalLen);
            readFully(rec, offset);
            rec.flip();

            int computedCrc = crc32(rec, 4, totalLen - 4);
            if (computedCrc != storedCrc) {
                log.warn("CRC mismatch at offset {} (stored={} computed={}); truncating",
                        offset, storedCrc, computedCrc);
                break;
            }

            // ── 5. Parse fields ───────────────────────────────────────────────
            rec.position(FIXED_HEADER);
            byte[] nodeIdBytes = new byte[nodeIdLen];  rec.get(nodeIdBytes);
            byte[] keyBytes    = new byte[keyLen];     rec.get(keyBytes);
            // (valueLen bytes follow but we only need key + version for the index)

            String  nodeId    = new String(nodeIdBytes, StandardCharsets.UTF_8);
            String  key       = new String(keyBytes,    StandardCharsets.UTF_8);
            boolean tombstone = (flags & 0x01) != 0;
            Version v         = new Version(timestamp, nodeId);

            // ── 6. Apply newer-version rule ───────────────────────────────────
            IndexEntry existing = index.get(key);
            if (existing == null || v.isNewerThan(existing.version())) {
                index.put(key, new IndexEntry(offset, totalLen, v, tombstone));
            }

            lastGoodOffset = offset + totalLen;
            recordsIndexed++;
            offset += totalLen;
        }

        // Truncate any corrupt/incomplete tail.
        if (lastGoodOffset < fileSize) {
            log.warn("Truncating {} corrupt bytes (offset {} → {})",
                    fileSize - lastGoodOffset, lastGoodOffset, fileSize);
            channel.truncate(lastGoodOffset);
        }

        log.info("Recovery complete: {} records, file now {} bytes",
                recordsIndexed, channel.size());
    }

    // ── Write helper ───────────────────────────────────────────────────────────

    /**
     * Serialises and appends one record at the current end of the log file.
     *
     * @return the byte offset at which the record starts
     */
    private long appendRecord(String key, byte[] value, Version v, boolean tombstone)
            throws IOException {
        byte[] nodeIdBytes = v.nodeId().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes    = key.getBytes(StandardCharsets.UTF_8);
        byte   flags       = tombstone ? (byte) 0x01 : (byte) 0x00;
        int    totalLen    = FIXED_HEADER + nodeIdBytes.length + keyBytes.length + value.length;

        // Build the record in a single ByteBuffer (all big-endian by Java default).
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putInt(0);                                    // CRC placeholder
        buf.putLong(v.timestamp());
        buf.putShort((short) nodeIdBytes.length);
        buf.put(flags);
        buf.putInt(keyBytes.length);
        buf.putInt(value.length);
        buf.put(nodeIdBytes);
        buf.put(keyBytes);
        buf.put(value);

        // Compute CRC over bytes [4, totalLen) and patch it in at position 0.
        int crc = crc32(buf, 4, totalLen - 4);
        buf.putInt(0, crc);
        buf.flip(); // switch to read mode for writing to channel

        // Append at the current end of file (we hold writeLock so size() is stable).
        long writeOffset = channel.size();
        long pos         = writeOffset;
        while (buf.hasRemaining()) {
            pos += channel.write(buf, pos);
        }

        if (fsyncOnWrite) channel.force(true);

        return writeOffset;
    }

    // ── Read helper ────────────────────────────────────────────────────────────

    /**
     * Reads exactly {@code buf.remaining()} bytes starting at {@code offset}.
     * Safe to call without the write lock (FileChannel positional reads are
     * thread-safe in Java).
     */
    private void readFully(ByteBuffer buf, long offset) throws IOException {
        long pos = offset;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) throw new IOException("Unexpected EOF at file offset " + pos);
            pos += n;
        }
    }

    /**
     * Parses a full record from {@code buf} (positioned at 0) and verifies CRC.
     *
     * @throws IOException if CRC does not match
     */
    private StoredRecord parseAndVerify(ByteBuffer buf, long offset) throws IOException {
        int totalLen = buf.limit();
        buf.position(0);
        int  storedCrc = buf.getInt();
        long timestamp = buf.getLong();
        int  nodeIdLen = buf.getShort() & 0xFFFF;
        byte flags     = buf.get();
        int  keyLen    = buf.getInt();
        int  valueLen  = buf.getInt();

        int computedCrc = crc32(buf, 4, totalLen - 4);
        if (computedCrc != storedCrc) {
            throw new IOException(String.format(
                    "CRC mismatch at offset %d (stored=%d computed=%d)",
                    offset, storedCrc, computedCrc));
        }

        buf.position(FIXED_HEADER);
        byte[] nodeIdBytes = new byte[nodeIdLen]; buf.get(nodeIdBytes);
        byte[] keyBytes    = new byte[keyLen];    buf.get(keyBytes);
        byte[] valueBytes  = new byte[valueLen];  buf.get(valueBytes);

        return new StoredRecord(
                new String(keyBytes,    StandardCharsets.UTF_8),
                valueBytes,
                new Version(timestamp, new String(nodeIdBytes, StandardCharsets.UTF_8)),
                (flags & 0x01) != 0);
    }

    // ── CRC utility ───────────────────────────────────────────────────────────

    /**
     * Returns CRC32 of bytes [{@code start}, {@code start+length}) in {@code buf}
     * without altering buf's position.
     *
     * <p>CRC32 detects accidental corruption (torn writes, bit-rot). It is fast
     * and available in the JDK — no extra dependency needed.
     */
    private static int crc32(ByteBuffer buf, int start, int length) {
        CRC32 crc = new CRC32();
        // duplicate() shares the backing data but has an independent position/limit.
        ByteBuffer slice = buf.duplicate();
        slice.position(start).limit(start + length);
        crc.update(slice); // CRC32.update(ByteBuffer) available since Java 9
        return (int) crc.getValue();
    }

    /** Computes the total byte length of a serialised record. */
    private static int recordLen(String key, String nodeId, int valueLen) {
        return FIXED_HEADER
                + nodeId.getBytes(StandardCharsets.UTF_8).length
                + key.getBytes(StandardCharsets.UTF_8).length
                + valueLen;
    }
}
