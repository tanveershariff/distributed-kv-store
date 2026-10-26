package com.kvstore.storage;

/**
 * In-memory index entry for a single key.
 *
 * <p>The index maps {@code key → IndexEntry} so reads can jump directly to
 * the right position in the log file without scanning.
 *
 * @param offset    byte offset in data.log where this record starts (inclusive)
 * @param length    total byte length of the record (used for a single positional read)
 * @param version   the version of this record (used to reject stale writes)
 * @param tombstone whether the record is a deletion marker
 */
record IndexEntry(long offset, int length, Version version, boolean tombstone) {}
