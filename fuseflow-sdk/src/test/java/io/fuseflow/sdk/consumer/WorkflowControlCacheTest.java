package io.fuseflow.sdk.consumer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowControlCacheTest {

    private static final Duration SHORT_TTL = Duration.ofMillis(40);

    @Test
    void pauseBlocksUntilResume() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
        UUID id = UUID.randomUUID();

        cache.pause(id);
        assertThat(cache.isBlocked(id)).isTrue();

        cache.resume(id);
        assertThat(cache.isBlocked(id)).isFalse();
    }

    @Test
    void cancelBlocksWithinTtlThenExpires() throws InterruptedException {
        WorkflowControlCache cache = new WorkflowControlCache(SHORT_TTL);
        UUID id = UUID.randomUUID();

        cache.cancel(id);
        assertThat(cache.isBlocked(id)).isTrue();

        Thread.sleep(SHORT_TTL.toMillis() + 80);
        assertThat(cache.isBlocked(id)).isFalse();
    }

    @Test
    void resumeClearsCancel() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
        UUID id = UUID.randomUUID();

        cache.cancel(id);
        cache.resume(id);

        assertThat(cache.isBlocked(id)).isFalse();
    }

    @Test
    void supersedeSkipsOldAttemptsOnly() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
        UUID id = UUID.randomUUID();
        cache.supersede(id, "a", 2);

        assertThat(cache.isSuperseded(id, "a", 1)).isTrue();
        assertThat(cache.isSuperseded(id, "a", 2)).isTrue();
        assertThat(cache.isSuperseded(id, "a", 3)).isFalse();
        // Other tasks of the same execution are unaffected.
        assertThat(cache.isSuperseded(id, "b", 1)).isFalse();
    }

    @Test
    void supersedeKeepsTheHighestAttempt() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
        UUID id = UUID.randomUUID();

        cache.supersede(id, "a", 2);
        cache.supersede(id, "a", 1);

        assertThat(cache.isSuperseded(id, "a", 2)).isTrue();
        assertThat(cache.isSuperseded(id, "a", 3)).isFalse();
    }

    @Test
    void supersedeExpiresAfterTtl() throws InterruptedException {
        WorkflowControlCache cache = new WorkflowControlCache(SHORT_TTL);
        UUID id = UUID.randomUUID();
        cache.supersede(id, "a", 1);

        Thread.sleep(SHORT_TTL.toMillis() + 80);

        assertThat(cache.isSuperseded(id, "a", 1)).isFalse();
    }

    @Test
    void clearDropsEverything() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
        UUID id = UUID.randomUUID();
        cache.pause(id);
        cache.supersede(id, "a", 1);

        cache.clear(id);

        assertThat(cache.isBlocked(id)).isFalse();
        assertThat(cache.isSuperseded(id, "a", 1)).isFalse();
    }

    @Test
    void unknownExecutionIsNeverBlocked() {
        WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));

        assertThat(cache.isBlocked(UUID.randomUUID())).isFalse();
        assertThat(cache.isSuperseded(UUID.randomUUID(), "a", 1)).isFalse();
    }
}
