/**
 * Consistent-hashing ring.
 * TreeMap&lt;Long, Node&gt; ring with 128 virtual nodes per physical node.
 * Hash function: first 8 bytes of SHA-1 cast to long (same function everywhere).
 * Preference list = next N distinct physical nodes clockwise. Lookup O(log V).
 * See PROJECT_CONTEXT §4 (Consistent hashing).
 */
package com.kvstore.ring;
