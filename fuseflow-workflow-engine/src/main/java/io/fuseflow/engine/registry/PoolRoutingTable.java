package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The engine's capability → pool routing table (Phase 5): which pools advertise which
 * activities, which of those pools currently have ≥ 1 ONLINE worker, and each pool's declared
 * concurrency. Seeded from the registry (boot) and refreshed on {@code worker-events} — the
 * dispatch hot path never touches the registry.
 *
 * <p>Selection policy: candidates are the pools advertising the activity with ≥ 1 ONLINE
 * worker; the target is chosen by a deterministic hash of the task id, so overlapping pools
 * (two pools both advertising an activity) never double-execute — each task goes to exactly one
 * pool topic, and the choice is stable across engine instances.
 *
 * <p>Thread safety: {@code seed} swaps an immutable snapshot, so concurrent dispatches always
 * read a consistent view.
 */
@Component
public class PoolRoutingTable {

    /** A routable target for an activity. */
    public record PoolTarget(String poolName, String topic, boolean online, int concurrency) {
    }

    private final String topicPrefix;

    private volatile Map<String, List<PoolTarget>> byActivity = Map.of();
    private volatile Map<String, Integer> concurrencyByPool = Map.of();

    public PoolRoutingTable(@Value("${fuseflow.kafka.topic.pool-prefix:fuseflow-pool}") String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    /** Replaces the routing snapshot from the current registry view. */
    public synchronized void seed(List<WorkerResponse> workers) {
        Map<String, PoolLiveness> livenessByPool = new HashMap<>();
        Map<String, Set<String>> activitiesByPool = new LinkedHashMap<>();
        for (WorkerResponse worker : workers) {
            if (worker.poolName() == null || worker.poolName().isBlank()) {
                continue;
            }
            PoolLiveness liveness = livenessByPool.computeIfAbsent(worker.poolName(), PoolLiveness::new);
            liveness.online |= "ONLINE".equalsIgnoreCase(worker.status());
            if (worker.concurrency() != null) {
                liveness.concurrency = Math.max(liveness.concurrency, worker.concurrency());
            }
            activitiesByPool.computeIfAbsent(worker.poolName(), p -> new java.util.LinkedHashSet<>())
                    .addAll(worker.activities());
        }

        Map<String, List<PoolTarget>> newByActivity = new HashMap<>();
        Map<String, Integer> newConcurrency = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : activitiesByPool.entrySet()) {
            String pool = entry.getKey();
            PoolLiveness liveness = livenessByPool.get(pool);
            newConcurrency.put(pool, liveness.concurrency);
            for (String activity : entry.getValue()) {
                newByActivity.computeIfAbsent(activity, a -> new ArrayList<>())
                        .add(new PoolTarget(pool, topic(pool), liveness.online, liveness.concurrency));
            }
        }
        // Deterministic candidate order — selection must be stable across instances.
        newByActivity.replaceAll((activity, targets) -> targets.stream()
                .sorted(Comparator.comparing(PoolTarget::poolName))
                .toList());

        this.byActivity = Map.copyOf(newByActivity);
        this.concurrencyByPool = Map.copyOf(newConcurrency);
    }

    /**
     * Resolves the pool topic a task should be dispatched to. Empty when no ONLINE pool
     * advertises the activity — the caller leaves the task SCHEDULED with a diagnostic event.
     */
    public Optional<String> resolveTopic(String activityName, String taskId) {
        List<PoolTarget> candidates = byActivity.getOrDefault(activityName, List.of()).stream()
                .filter(PoolTarget::online)
                .toList();
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        int index = Math.floorMod(taskId.hashCode(), candidates.size());
        return Optional.of(candidates.get(index).topic());
    }

    /** All pools currently in the table (for topic provisioning after a seed). */
    public Set<String> poolNames() {
        return concurrencyByPool.keySet();
    }

    /**
     * The current capability set — every activity advertised by at least one pool, regardless
     * of liveness (used for diagnostics).
     */
    public Set<String> activities() {
        return byActivity.keySet();
    }

    /**
     * The routable set — activities with at least one <b>ONLINE</b> pool. The rejoin sweep
     * gates on THIS growing (post-Phase 7 hardening): an activity already in the table via an
     * OFFLINE pool must still trigger the sweep the moment a pool for it comes ONLINE.
     */
    public Set<String> routableActivities() {
        return byActivity.entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(PoolTarget::online))
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }

    /** The declared concurrency of a pool (defaults to 1) — drives its topic's partitions. */
    public int poolConcurrency(String poolName) {
        return concurrencyByPool.getOrDefault(poolName, 1);
    }

    /** Whether any pool advertises the activity (regardless of liveness). */
    public boolean hasCapablePool(String activityName) {
        return byActivity.containsKey(activityName);
    }

    /** Number of distinct activities in the table (diagnostics). */
    public int size() {
        return byActivity.size();
    }

    private String topic(String poolName) {
        return topicPrefix + "." + poolName;
    }

    private static final class PoolLiveness {
        final String poolName;
        boolean online;
        int concurrency = 1;

        PoolLiveness(String poolName) {
            this.poolName = poolName;
        }
    }
}
