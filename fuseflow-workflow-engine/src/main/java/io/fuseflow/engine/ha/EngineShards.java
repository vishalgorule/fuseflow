package io.fuseflow.engine.ha;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Engine high-availability sharding (Phase 5): a fixed shard count with a consistent
 * {@code executionId → shard} mapping shared by every engine instance. Each instance owns a
 * configured subset of shards ({@code fuseflow.engine.owned-shards}, default {@code all}) and
 * only boot-recovers executions on those shards, so N instances recovering the same RUNNING
 * executions can never double-dispatch.
 *
 * <p>The count is fixed at deploy time (like Temporal's history-shard count): changing it
 * re-maps every execution, which is safe (shard is stored per row) but defeats the split —
 * decide {@code fuseflow.engine.shards} once, ahead of peak load.
 */
@Component
public class EngineShards {

    private final int shardCount;
    private final Set<Integer> ownedShards;
    private final boolean ownsAll;

    public EngineShards(@Value("${fuseflow.engine.shards:8}") int shardCount,
                        @Value("${fuseflow.engine.owned-shards:all}") String ownedShards) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("fuseflow.engine.shards must be at least 1");
        }
        this.shardCount = shardCount;
        this.ownedShards = parse(shardCount, ownedShards);
        this.ownsAll = this.ownedShards.size() == shardCount;
    }

    /** The total shard count (must be identical on every engine instance). */
    public int shardCount() {
        return shardCount;
    }

    /** The deterministic shard of an execution — identical on every instance. */
    public int shardOf(UUID executionId) {
        return Math.floorMod(executionId.hashCode(), shardCount);
    }

    /** Whether this instance owns the given shard. */
    public boolean owns(int shard) {
        return ownedShards.contains(shard);
    }

    /** The shards this instance recovers at boot. */
    public Set<Integer> ownedShards() {
        return ownedShards;
    }

    /** True when this instance is configured to recover every shard (single-engine deployment). */
    public boolean ownsAll() {
        return ownsAll;
    }

    /** Parses {@code all} | {@code 0-3} | {@code 0,1,4} (ranges and lists may mix). */
    private static Set<Integer> parse(int shardCount, String spec) {
        Set<Integer> result = new LinkedHashSet<>();
        if (spec == null || spec.isBlank() || spec.equalsIgnoreCase("all")) {
            for (int i = 0; i < shardCount; i++) {
                result.add(i);
            }
            return result;
        }
        for (String part : spec.split(",")) {
            String token = part.trim();
            int dash = token.indexOf('-');
            if (dash > 0) {
                int from = Integer.parseInt(token.substring(0, dash));
                int to = Integer.parseInt(token.substring(dash + 1));
                for (int i = from; i <= to; i++) {
                    result.add(i);
                }
            } else {
                result.add(Integer.parseInt(token));
            }
        }
        for (int shard : result) {
            if (shard < 0 || shard >= shardCount) {
                throw new IllegalArgumentException(
                        "fuseflow.engine.owned-shards references shard " + shard
                                + " outside [0, " + shardCount + ")");
            }
        }
        return result;
    }
}
