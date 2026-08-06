package io.fuseflow.engine.config;

import io.fuseflow.engine.dispatch.ActivityExecutor;
import io.fuseflow.engine.dispatch.DemoActivityExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import tools.jackson.databind.ObjectMapper;

/**
 * Engine wiring: the activity worker thread pool and the demo {@link ActivityExecutor}.
 * The demo executor is the default; tests replace it via {@code @ConditionalOnMissingBean}
 * with deterministic fakes.
 */
@Configuration
public class EngineConfig {

    @Bean
    public ThreadPoolTaskExecutor activityTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(256);
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("activity-exec-");
        threadFactory.setDaemon(true);
        executor.setThreadFactory(threadFactory);
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean(ActivityExecutor.class)
    public ActivityExecutor demoActivityExecutor(ObjectMapper objectMapper,
                                                 @Value("${fuseflow.demo.activity-delay-ms:500}") long delayMillis) {
        return new DemoActivityExecutor(objectMapper, delayMillis);
    }
}
