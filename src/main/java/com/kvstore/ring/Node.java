package com.kvstore.ring;

/**
 * Represents a physical node in the cluster.
 *
 * <p>Java beginner note: {@code record} is a Java 16+ feature. It is like a
 * data class in Kotlin or a TypeScript interface with all readonly fields.
 * The compiler automatically generates a constructor, getters (id(), host(),
 * port()), equals(), hashCode(), and toString(). Records are immutable.
 *
 * @param id   unique name for this node (e.g. "node1"); used as a tie-breaker
 *             and to build vnode positions: HashFunction.hash(id + "#" + i)
 * @param host hostname or IP to reach this node
 * @param port TCP port the node's HTTP server listens on
 */
public record Node(String id, String host, int port) {

    /** Compact constructor — validate inputs on creation. */
    public Node {
        if (id == null || id.isBlank())   throw new IllegalArgumentException("Node id must not be blank");
        if (host == null || host.isBlank()) throw new IllegalArgumentException("Node host must not be blank");
        if (port < 1 || port > 65535)     throw new IllegalArgumentException("Node port out of range: " + port);
    }

    /** Convenience: base URL used for HTTP calls (e.g. "http://host:port"). */
    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
