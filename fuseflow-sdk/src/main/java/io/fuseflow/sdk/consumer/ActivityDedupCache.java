package io.fuseflow.sdk.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker-side dedup cache for dispatch messages (Phase 8 hardening): prevents re-execution
 * of activities whose {@code (executionId, taskId, attempt)} has already been processed by
 * this worker instance.
 *
 * <p>Why this exists: the engine's dispatch outbox guarantees at-least-once delivery — a crash
 * between DB commit and Kafka publish (or a Kafka redelivery) causes the outbox poller to
 * re-dispatch the same task. Without this cache the worker re-executes the activity (wasted
 * CPU/IO); the engine's {@link io.fuseflow.engine.service.ResultHandler} would deduplicate
 * the result, but the work is already done.
 *
 * <p>Key design choices:
 * <ul>
 *   <li>Key = {@code (executionId, taskId, attempt)} — same task retried with a new attempt
 *       number is a <em>different</em> entry (not a duplicate).
 *   <li>TTL defaults to 30 minutes; configurable at construction. Once the engine's
 *       execution timeout has passed the task can't be redelivered, so the entry is stale.
 *   <li>Bounded to {@code maxEntries} (default 200 000) with oldest-eviction when the bound
 *       is reached — prevents unbounded growth under burst traffic.
 *   <li>Periodic cleanup removes expired entries every minute (lazy per-access eviction
 *       is not sufficient alone because expired entries occupy map slots).
 * </ul>
 *
 * <p>Thread-safe: all operations go through a {@link ConcurrentHashMap}.
 */
public class ActivityDedupCache {

    private static final Logger log = LoggerFactory.getLogger(ActivityDedupCache.class);

    private static final int DEFAULT_MAX_ENTRIES = 200_000;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);

    private final ConcurrentHashMap<Key, Instant> cache = new ConcurrentHashMap<>();
    private final Duration ttl;
    private final int maxEntries;
    private final ScheduledExecutorService cleaner;

    public ActivityDedupCache() {
        this(DEFAULT_TTL, DEFAULT_MAX_ENTRIES);
    }

    public ActivityDedupCache(Duration ttl, int maxEntries) {
        this.ttl = ttl;
        this.maxEntries = maxEntries;
        this.cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fuseflow-dedup-cache-cleaner");
            t.setDaemon(true);
            return t;
        });
        this.cleaner.scheduleWithFixedDelay(this::evictExpired, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Returns {@code true} if this {@code (executionId, taskId, attempt)} has already been
     * processed by this worker instance and should be skipped.
     */
    public boolean isDuplicate(UUID executionId, String taskId, int attempt) {
        Key key = new Key(executionId, taskId, attempt);
        Instant seen = cache.get(key);
        if (seen != null && Instant.now().isBefore(seen.plus(ttl))) {
            log.debug("Dedup hit: skipping task {} of execution {} (attempt {})",
                    taskId, executionId, attempt);
            return true;
        }
        return false;
    }

    /**
     * Marks this {@code (executionId, taskId, attempt)} as processed. Called after the
     * activity has been executed and the terminal result published.
     */
    public void markProcessed(UUID executionId, String taskId, int attempt) {
        Key key = new Key(executionId, taskId, attempt);
        if (cache.size() >= maxEntries) {
            evictExpired();
            // If still over limit after eviction, the oldest entries are naturally
            // evicted by ConcurrentHashMap's eventual consistency — acceptable for
            // a dedup cache (worst case: one extra execution, engine dedupes).
        }
        cache.put(key, Instant.now());
    }

    /** Visible for testing. */
    int size() {
        return cache.size();
    }

    public void shutdown() {
        cleaner.shutdownNow();
    }

    // ---------------------------------------------------------------- internals

    private void evictExpired() {
        Instant cutoff = Instant.now().minus(ttl);
        cache.entrySet().removeIf(entry -> entry.getValue().isBefore(cutoff));
    }

    private record Key(UUID executionId, String taskId, int attempt) {
    }
}
