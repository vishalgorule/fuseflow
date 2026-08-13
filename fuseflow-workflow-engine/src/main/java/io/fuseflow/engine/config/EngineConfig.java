package io.fuseflow.engine.config;

import io.fuseflow.engine.dispatch.ActivityExecutor;
import io.fuseflow.engine.dispatch.DemoActivityExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import tools.jackson.databind.ObjectMapper;

/**
 * Engine wiring for the in-memory dispatch mode: the activity worker thread pool and the demo
 * {@link ActivityExecutor}. Both only exist in {@code fuseflow.engine.dispatch-mode=in-memory}
 * (Phase 2 tests + demo auto-complete); the default Kafka mode dispatches over Kafka and needs
 * no in-process pool. The demo executor is the default in-memory executor; tests replace it via
 * {@code @ConditionalOnMissingBean} with deterministic fakes.
 */
@Configuration
public class EngineConfig {

    @Bean
    @ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "in-memory")
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
    @ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "in-memory")
    @ConditionalOnMissingBean(ActivityExecutor.class)
    public ActivityExecutor demoActivityExecutor(ObjectMapper objectMapper,
                                                 @Value("${fuseflow.demo.activity-delay-ms:500}") long delayMillis) {
        return new DemoActivityExecutor(objectMapper, delayMillis);
    }
}
