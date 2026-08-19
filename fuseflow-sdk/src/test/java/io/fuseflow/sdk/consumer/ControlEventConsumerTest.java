package io.fuseflow.sdk.consumer;

import io.fuseflow.common.messaging.WorkflowEventMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ControlEventConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkflowControlCache cache = new WorkflowControlCache(Duration.ofMinutes(10));
    private final ControlEventConsumer consumer = new ControlEventConsumer(objectMapper, cache);

    private void feed(WorkflowEventMessage event) throws Exception {
        consumer.onEvent(new ConsumerRecord<>("workflow-events", 0, 0L, event.executionId().toString(),
                objectMapper.writeValueAsString(event)));
    }

    @Test
    void pauseBlocksAndResumeUnblocks() throws Exception {
        UUID id = UUID.randomUUID();

        feed(new WorkflowEventMessage(id, "WorkflowPaused", Map.of(), Instant.now()));
        assertThat(cache.isBlocked(id)).isTrue();

        feed(new WorkflowEventMessage(id, "WorkflowResumed", Map.of(), Instant.now()));
        assertThat(cache.isBlocked(id)).isFalse();
    }

    @Test
    void cancelBlocksAndCompletionClears() throws Exception {
        UUID id = UUID.randomUUID();

        feed(new WorkflowEventMessage(id, "WorkflowCancelled", Map.of(), Instant.now()));
        assertThat(cache.isBlocked(id)).isTrue();

        feed(new WorkflowEventMessage(id, "WorkflowCompleted", Map.of(), Instant.now()));
        assertThat(cache.isBlocked(id)).isFalse();
    }

    @Test
    void supersededAttemptMarksTaskStale() throws Exception {
        UUID id = UUID.randomUUID();
        Map<String, Object> payload = Map.of(
                "taskId", "resize",
                "activityName", "resizeImage",
                "supersededAttempt", 1,
                "newAttempt", 2);

        feed(new WorkflowEventMessage(id, "ActivitySuperseded", payload, Instant.now()));

        assertThat(cache.isSuperseded(id, "resize", 1)).isTrue();
        // Attempt 2 is the NEW attempt — it must execute.
        assertThat(cache.isSuperseded(id, "resize", 2)).isFalse();
        assertThat(cache.isSuperseded(id, "resize", 3)).isFalse();
    }

    @Test
    void unrelatedEventsAreIgnored() throws Exception {
        UUID id = UUID.randomUUID();

        feed(new WorkflowEventMessage(id, "WorkflowStarted", Map.of(), Instant.now()));

        assertThat(cache.isBlocked(id)).isFalse();
        assertThat(cache.isSuperseded(id, "a", 1)).isFalse();
    }

    @Test
    void toleratesMalformedEvent() {
        consumer.onEvent(new ConsumerRecord<>("workflow-events", 0, 0L, "x", "{not json"));

        // No exception escapes; cache stays empty.
        assertThat(cache.isBlocked(UUID.randomUUID())).isFalse();
    }
}
