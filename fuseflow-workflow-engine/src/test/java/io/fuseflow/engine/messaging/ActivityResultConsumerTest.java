package io.fuseflow.engine.messaging;

import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityResultType;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.service.ActivityStateService;
import io.fuseflow.engine.service.ResultHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ActivityResultConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ActivityStateService activityState = mock(ActivityStateService.class);
    private final ResultHandler resultHandler = mock(ResultHandler.class);
    private final ActivityResultConsumer consumer =
            new ActivityResultConsumer(objectMapper, activityState, resultHandler);

    private static ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>("activity-results", 0, 0L, "b", json);
    }

    @Test
    void startedSignalMarksActivityStarted() throws Exception {
        ActivityResultMessage message = new ActivityResultMessage(UUID.randomUUID(), "b", 1,
                ActivityResultType.STARTED, null, null, null);
        when(activityState.startActivity(message.executionId(), "b", 1)).thenReturn(true);

        consumer.onResult(record(objectMapper.writeValueAsString(message)));

        verify(activityState).startActivity(message.executionId(), "b", 1);
        verify(resultHandler, never()).handleResult(any());
    }

    @Test
    void completedResultIsFedToResultHandler() throws Exception {
        ActivityResultMessage message = new ActivityResultMessage(UUID.randomUUID(), "b", 1,
                ActivityResultType.COMPLETED, "{\"x\":1}", null, null);

        consumer.onResult(record(objectMapper.writeValueAsString(message)));

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(resultHandler).handleResult(captor.capture());
        assertThat(captor.getValue().success()).isTrue();
        assertThat(captor.getValue().output()).isEqualTo("{\"x\":1}");
        assertThat(captor.getValue().attempt()).isEqualTo(1);
        verify(activityState, never()).startActivity(any(), any(), anyInt());
    }

    @Test
    void failedResultIsFedToResultHandlerAsFailure() throws Exception {
        ActivityResultMessage message = new ActivityResultMessage(UUID.randomUUID(), "b", 2,
                ActivityResultType.FAILED, null, "boom", "java.lang.IllegalStateException");

        consumer.onResult(record(objectMapper.writeValueAsString(message)));

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(resultHandler).handleResult(captor.capture());
        assertThat(captor.getValue().success()).isFalse();
        assertThat(captor.getValue().error()).isEqualTo("boom");
        assertThat(captor.getValue().errorType()).isEqualTo("java.lang.IllegalStateException");
    }

    @Test
    void malformedMessageIsLoggedAndAcknowledged() {
        consumer.onResult(record("{not json"));

        verifyNoInteractions(activityState, resultHandler);
    }

    @Test
    void rethrowsProcessingFailuresSoKafkaRedelivers() throws Exception {
        // A transient failure (e.g. DB outage) must NOT be acked — redelivery preserves
        // at-least-once.
        ActivityResultMessage message = new ActivityResultMessage(UUID.randomUUID(), "b", 1,
                ActivityResultType.COMPLETED, "{\"x\":1}", null, null);
        doThrow(new RuntimeException("db down")).when(resultHandler).handleResult(any());

        assertThatThrownBy(() -> consumer.onResult(record(objectMapper.writeValueAsString(message))))
                .isInstanceOf(RuntimeException.class);
    }
}
