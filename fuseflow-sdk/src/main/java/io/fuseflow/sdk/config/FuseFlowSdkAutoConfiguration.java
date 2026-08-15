package io.fuseflow.sdk.config;

import io.fuseflow.sdk.client.RegistryClient;
import io.fuseflow.sdk.consumer.PoolActivityListener;
import io.fuseflow.sdk.pub.ActivityResultPublisher;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.ActivityScanner;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

/**
 * Wires the worker runtime into any application that has the SDK on its classpath and enables
 * it ({@code fuseflow.worker.enabled=true}). Registered via
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "fuseflow.worker.enabled", havingValue = "true")
@ConditionalOnClass({KafkaTemplate.class, RestClient.class})
@EnableConfigurationProperties(WorkerProperties.class)
public class FuseFlowSdkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ActivityRegistry activityRegistry() {
        return new ActivityRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public ActivityScanner activityScanner(ApplicationContext applicationContext,
                                           ActivityRegistry activityRegistry) {
        return new ActivityScanner(applicationContext, activityRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public RegistryClient registryClient(WorkerProperties properties,
                                         @Value("${fuseflow.registry.base-url:http://localhost:8083}") String baseUrl) {
        return new RegistryClient(baseUrl);
    }

    @Bean
    @ConditionalOnMissingBean
    public ActivityResultPublisher activityResultPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${fuseflow.queue.activity-results:activity-results}") String queue) {
        return new ActivityResultPublisher(kafkaTemplate, objectMapper, queue);
    }

    @Bean
    @ConditionalOnMissingBean
    public FuseFlowWorker fuseFlowWorker(WorkerProperties properties,
                                         ActivityRegistry activityRegistry,
                                         RegistryClient registryClient,
                                         ActivityResultPublisher resultPublisher,
                                         ObjectMapper objectMapper) {
        return new FuseFlowWorker(properties, activityRegistry, registryClient, resultPublisher, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public PoolActivityListener poolActivityListener(ObjectMapper objectMapper,
                                                     ActivityRegistry activityRegistry,
                                                     FuseFlowWorker fuseFlowWorker) {
        return new PoolActivityListener(objectMapper, activityRegistry, fuseFlowWorker);
    }

    /**
     * Declares the pool's dispatch queue (Phase 5) so the worker creates it on boot with the
     * pool's declared concurrency as its partition count. This wins the creation race against
     * the broker's auto-create (which would use 1 partition) — the engine's provisioner
     * self-heals any queue it finds under-sized either way.
     */
    @Bean
    @ConditionalOnMissingBean(name = "fuseflowPoolQueue")
    public NewTopic fuseflowPoolQueue(
            @Value("${fuseflow.queue.pool-prefix:fuseflow-pool}.${fuseflow.worker.pool:default}") String queue,
            @Value("${fuseflow.worker.concurrency:1}") int concurrency) {
        return TopicBuilder.name(queue).partitions(concurrency).replicas((short) 1).build();
    }
}
