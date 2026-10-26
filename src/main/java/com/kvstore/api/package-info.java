/**
 * HTTP API layer using JDK's built-in com.sun.net.httpserver.HttpServer (no Javalin).
 * Client API:   PUT /kv/{key}, GET /kv/{key}, DELETE /kv/{key}
 * Internal API: POST /internal/replicate, GET /internal/read, POST /internal/heartbeat
 * Admin API:    GET /health, GET /admin/ring, GET /admin/stats
 * See PROJECT_CONTEXT §4 (APIs) and CONFIG AND API CONTRACT section.
 */
package com.kvstore.api;
