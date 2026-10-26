package com.kvstore.ring;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Deterministic hash function used everywhere in the ring.
 *
 * <p>Java beginner note: SHA-1 produces a 20-byte digest. We take the first 8
 * bytes and read them as a single 64-bit signed long (big-endian). Using a
 * signed long is fine because the TreeMap uses natural ordering — negative
 * values simply sort before positive ones, and the ring "wraps around" in the
 * lookup logic (ceilingEntry → firstEntry).
 */
public final class HashFunction {

    private HashFunction() {} // utility class — no instances

    /**
     * Returns the first 8 bytes of SHA-1(key as UTF-8) interpreted as a
     * big-endian signed long.
     *
     * @param key any non-null string
     * @return a deterministic long in [Long.MIN_VALUE, Long.MAX_VALUE]
     */
    public static long hash(String key) {
        try {
            // MessageDigest is not thread-safe, so we create a new one each call.
            // SHA-1 is fast enough that the allocation cost is negligible here.
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            // ByteBuffer.wrap reads bytes in big-endian order by default.
            // getLong() reads 8 bytes starting at position 0.
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is guaranteed by the Java spec to be present on every JVM.
            throw new AssertionError("SHA-1 not available", e);
        }
    }
}
