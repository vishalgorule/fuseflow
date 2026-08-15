package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PoolRoutingTableTest {

    private static WorkerResponse worker(UUID id, String pool, String status, int concurrency,
                                         String... activities) {
        Instant now = Instant.now();
        return new WorkerResponse(id, "host", status, List.of(activities), pool, concurrency,
                now, 0, now, now);
    }

    @Test
    void resolvesTopicToThePoolsOnlineWorker() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(
                worker(UUID.randomUUID(), "media", "ONLINE", 8, "resizeImage", "watermarkImage"),
                worker(UUID.randomUUID(), "io", "ONLINE", 4, "downloadImage")));

        assertThat(table.resolveTopic("resizeImage", "t1")).contains("fuseflow-pool.media");
        assertThat(table.resolveTopic("downloadImage", "t2")).contains("fuseflow-pool.io");
        assertThat(table.poolConcurrency("media")).isEqualTo(8);
    }

    @Test
    void offlinePoolsAreNotRoutableButRemainKnown() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(worker(UUID.randomUUID(), "media", "OFFLINE", 8, "resizeImage")));

        assertThat(table.resolveTopic("resizeImage", "t1")).isEmpty();
        assertThat(table.hasCapablePool("resizeImage")).isTrue();
    }

    @Test
    void overlappingPoolsResolveDeterministicallyToExactlyOneTopic() {
        // Two pools both advertise downloadImage — Phase 5's cross-group duplicate guard:
        // each task must route to exactly one pool, and the choice must be stable.
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(
                worker(UUID.randomUUID(), "a", "ONLINE", 4, "downloadImage"),
                worker(UUID.randomUUID(), "b", "ONLINE", 4, "downloadImage")));

        Optional<String> first = table.resolveTopic("downloadImage", "task-1");
        Optional<String> second = table.resolveTopic("downloadImage", "task-1");

        assertThat(first).isPresent();
        assertThat(second).isEqualTo(first);
        assertThat(first.get()).isIn("fuseflow-pool.a", "fuseflow-pool.b");
    }

    @Test
    void overlappingSelectionSpreadsAcrossPools() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(
                worker(UUID.randomUUID(), "a", "ONLINE", 4, "downloadImage"),
                worker(UUID.randomUUID(), "b", "ONLINE", 4, "downloadImage")));

        // With two pools and enough distinct task ids, both should be selected at least once
        // (hash spread), proving the pool is not hard-coded.
        var seenA = new java.util.concurrent.atomic.AtomicBoolean(false);
        var seenB = new java.util.concurrent.atomic.AtomicBoolean(false);
        for (int i = 0; i < 200; i++) {
            String topic = table.resolveTopic("downloadImage", "task-" + i).orElseThrow();
            if (topic.equals("fuseflow-pool.a")) {
                seenA.set(true);
            } else if (topic.equals("fuseflow-pool.b")) {
                seenB.set(true);
            }
        }
        assertThat(seenA.get()).isTrue();
        assertThat(seenB.get()).isTrue();
    }

    @Test
    void unknownActivityIsUnroutable() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(worker(UUID.randomUUID(), "a", "ONLINE", 4, "downloadImage")));

        assertThat(table.resolveTopic("noSuchActivity", "t")).isEmpty();
        assertThat(table.hasCapablePool("noSuchActivity")).isFalse();
    }

    @Test
    void reseedReplacesTheSnapshot() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(worker(UUID.randomUUID(), "a", "ONLINE", 4, "x")));

        table.seed(List.of()); // every worker deregistered

        assertThat(table.resolveTopic("x", "t")).isEmpty();
        assertThat(table.poolNames()).isEmpty();
    }

    @Test
    void aPoolIsOnlineWhenAnyOfItsWorkersIsOnline() {
        PoolRoutingTable table = new PoolRoutingTable("fuseflow-pool");
        table.seed(List.of(
                worker(UUID.randomUUID(), "media", "OFFLINE", 8, "resizeImage"),
                worker(UUID.randomUUID(), "media", "ONLINE", 8, "resizeImage")));

        assertThat(table.resolveTopic("resizeImage", "t1")).contains("fuseflow-pool.media");
    }
}
