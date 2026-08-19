package io.fuseflow.sdk.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-side memory of engine control signals (Option B), learned from the
 * {@code workflow-events} topic by the {@link ControlEventConsumer}: executions that are
 * paused or cancelled (their queued tasks must not execute) and task attempts superseded by a
 * retry (the old attempt's queued message must not execute). The dispatch path
 * ({@link PoolActivityListener}) consults this before executing.
 *
 * <p>Best-effort by design — the engine's DB guards (attempt/status checks on results) remain
 * the source of truth, and a task consumed a moment before the control signal arrives still
 * executes with its result dropped. Memory is bounded: paused entries hold until released
 * (resume/cancel/completion — a pause has no natural expiry), while cancelled and superseded
 * entries are TTL-bounded and lazily evicted on lookup, so terminal executions cannot
 * accumulate.
 */
public class WorkflowControlCache {

    private final Duration ttl;
    private final Map<UUID, Entry> entries = new ConcurrentHashMap<>();

    public WorkflowControlCache(Duration ttl) {
        this.ttl = ttl;
    }

    /** Pauses an execution: skip its queued tasks until {@link #resume} (or cancel/completion). */
    public void pause(UUID executionId) {
        entry(executionId).paused = true;
    }

    /** Resumes an execution: queued tasks may execute again. */
    public void resume(UUID executionId) {
        Entry entry = entries.get(executionId);
        if (entry != null) {
            entry.paused = false;
            entry.cancelledUntil = null;
        }
    }

    /** Cancels an execution (terminal): skip its queued tasks, TTL-bounded. */
    public void cancel(UUID executionId) {
        Entry entry = entry(executionId);
        entry.cancelledUntil = Instant.now().plus(ttl);
    }

    /**
     * Marks {@code attempt} of the given task as superseded by a retry: any queued message for
     * this task with {@code attempt <= supersededAttempt} is skipped. TTL-bounded.
     */
    public void supersede(UUID executionId, String taskId, int supersededAttempt) {
        Entry entry = entry(executionId);
        entry.supersededUntil = Instant.now().plus(ttl);
        entry.superseded.merge(taskId, supersededAttempt, Math::max);
    }

    /** Drops all state for an execution (terminal completion/failure — nothing else can arrive). */
    public void clear(UUID executionId) {
        entries.remove(executionId);
    }

    /** True when the execution is paused, or cancelled within the retention window. */
    public boolean isBlocked(UUID executionId) {
        Entry entry = entries.get(executionId);
        if (entry == null) {
            return false;
        }
        if (entry.paused) {
            return true;
        }
        boolean cancelled = entry.cancelledUntil != null && Instant.now().isBefore(entry.cancelledUntil);
        if (!cancelled && expired(entry)) {
            entries.remove(executionId, entry);
        }
        return cancelled;
    }

    /**
     * True when a queued message for {@code taskId} at {@code attempt} was superseded — the
     * engine has already moved past it (retry scheduled or dispatched), so executing it would
     * be wasted work whose result is dropped by the attempt guard.
     */
    public boolean isSuperseded(UUID executionId, String taskId, int attempt) {
        Entry entry = entries.get(executionId);
        if (entry == null) {
            return false;
        }
        if (expired(entry)) {
            if (!entry.paused) {
                entries.remove(executionId, entry);
            }
            return false;
        }
        Integer supersededAttempt = entry.superseded.get(taskId);
        return supersededAttempt != null && attempt <= supersededAttempt;
    }

    // ---------------------------------------------------------------- internals

    private Entry entry(UUID executionId) {
        return entries.computeIfAbsent(executionId, id -> new Entry());
    }

    /** True when every volatile concern of the entry is past its TTL (lazy eviction trigger). */
    private boolean expired(Entry entry) {
        Instant now = Instant.now();
        return !entry.paused
                && (entry.cancelledUntil == null || !now.isBefore(entry.cancelledUntil))
                && (entry.supersededUntil == null || !now.isBefore(entry.supersededUntil));
    }

    static final class Entry {
        volatile boolean paused;
        volatile Instant cancelledUntil;
        volatile Instant supersededUntil;
        final Map<String, Integer> superseded = new ConcurrentHashMap<>();
    }
}
