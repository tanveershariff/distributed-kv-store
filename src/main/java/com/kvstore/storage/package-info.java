/**
 * Bitcask-style append-only storage engine.
 * Append-only log file; in-memory HashMap&lt;key, (offset, length, version)&gt; index.
 * Reads use FileChannel (zero-copy, one record per read). Deletes are tombstones.
 * On startup: rebuild index by scanning log; handle truncated last record safely.
 * See PROJECT_CONTEXT §4 (Storage).
 */
package com.kvstore.storage;
