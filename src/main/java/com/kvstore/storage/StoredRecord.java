package com.kvstore.storage;

import java.util.Arrays;

/**
 * An immutable snapshot of one key's data as read from the storage log.
 *
 * <p>A tombstone record has {@code tombstone=true} and an empty {@code value}
 * array. The storage engine returns tombstones from {@code get()} so callers
 * can distinguish "key was explicitly deleted" from "key was never written".
 *
 * @param key       the key string
 * @param value     raw value bytes (length 0 for a tombstone)
 * @param version   the version at which this record was written
 * @param tombstone {@code true} if this record represents a deletion
 */
public record StoredRecord(String key, byte[] value, Version version, boolean tombstone) {

    /** Defensive copy of the byte array to preserve immutability. */
    public StoredRecord {
        value = (value == null) ? new byte[0] : Arrays.copyOf(value, value.length);
    }

    /** Returns a defensive copy of the value bytes. */
    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    /** Convenience: is this a live (non-tombstone) record? */
    public boolean isLive() {
        return !tombstone;
    }
}
