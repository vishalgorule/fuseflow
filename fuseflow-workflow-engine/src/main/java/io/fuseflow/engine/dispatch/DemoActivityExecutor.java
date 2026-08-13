package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Demo-mode {@link ActivityExecutor}: simulates a healthy worker by sleeping a configurable
 * delay and reporting success with a small JSON payload. Used by the locally running engine
 * (`make services`) so a workflow can be started and watched complete via curl without any
 * real worker. Tests inject their own deterministic executors instead.
 */
public class DemoActivityExecutor implements ActivityExecutor {

    private final ObjectMapper objectMapper;
    private final long delayMillis;

    public DemoActivityExecutor(ObjectMapper objectMapper, long delayMillis) {
        this.objectMapper = objectMapper;
        this.delayMillis = delayMillis;
    }

    @Override
    public ActivityResult execute(ActivityTask task) throws Exception {
        Thread.sleep(delayMillis);
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("taskId", task.taskId());
        output.put("activityName", task.activityName());
        output.put("message", "ok");
        return ActivityResult.success(task, objectMapper.writeValueAsString(output));
    }
}
