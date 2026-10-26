/**
 * Thread-safe LRU cache (HashMap + doubly-linked list, O(1) get/put).
 * DO NOT use LinkedHashMap — written from scratch per PROJECT_CONTEXT §4.
 * Sits in front of storage disk reads; invalidated on put and delete.
 * Capacity and on/off switch configurable via env vars (CACHE_ENABLED, CACHE_CAPACITY).
 */
package com.kvstore.cache;
