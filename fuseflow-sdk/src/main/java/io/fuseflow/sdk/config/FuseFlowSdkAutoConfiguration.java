package io.fuseflow.sdk.config;

import io.fuseflow.sdk.client.RegistryClient;
import io.fuseflow.sdk.consumer.ActivityDedupCache;
import io.fuseflow.sdk.consumer.ControlEventConsumer;
import io.fuseflow.sdk.consumer.PoolActivityListener;
import io.fuseflow.sdk.consumer.WorkflowControlCache;
import io.fuseflow.sdk.pub.ActivityResultPublisher;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.ActivityScanner;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

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
            KafkaProperties properties,
            ObjectMapper objectMapper,
            @Value("${fuseflow.queue.activity-results:activity-results}") String queue) {
        // Post-Phase 7 hardening: two templates. The injected kafkaTemplate (Boot's) is
        // transactional — terminal results (COMPLETED/FAILED) join the pool listener's
        // container transaction and commit atomically with the offset. The eager STARTED signal
        // must NOT join that transaction (it would only reach the engine at commit time, and
        // long activities would false-trigger the engine's start timeout), so it goes through a
        // dedicated non-transactional template.
        Map<String, Object> producerProps = properties.buildProducerProperties();
        producerProps.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        KafkaTemplate<String, String> startedTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        return new ActivityResultPublisher(kafkaTemplate, startedTemplate, objectMapper, queue);
    }

    /**
     * Transactional listener container for the pool dispatch queue (post-Phase 7 hardening): the
     * container starts a Kafka transaction around each execution and commits the offset atomically
     * with the COMPLETED/FAILED result — a worker crash mid-execution leaves the offset
     * uncommitted, so the task is redelivered instead of stalling until the execution timeout.
     * The eager STARTED signal is exempt (published via the non-transactional template).
     */
    @Bean
    @ConditionalOnMissingBean(name = "fuseflowPoolListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> fuseflowPoolListenerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            ProducerFactory<Object, Object> producerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(new KafkaTransactionManager<>(producerFactory));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
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

    /**
     * Option B stale-task guard: worker-side memory of pause/cancel/supersede signals, fed by
     * the control consumer and consulted by the pool listener before every execution. Retention
     * bounds cancelled/superseded entries; paused entries hold until resume/cancel/completion.
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkflowControlCache workflowControlCache(
            @Value("${fuseflow.worker.control-ttl:10m}") Duration controlTtl) {
        return new WorkflowControlCache(controlTtl);
    }

    /**
     * Consumes the engine's workflow-events topic (own group per instance, so every worker sees
     * every control signal) and updates the control cache.
     */
    @Bean
    @ConditionalOnMissingBean
    public ControlEventConsumer controlEventConsumer(ObjectMapper objectMapper,
                                                     WorkflowControlCache controlCache) {
        return new ControlEventConsumer(objectMapper, controlCache);
    }

    @Bean
    @ConditionalOnMissingBean
    public ActivityDedupCache activityDedupCache() {
        return new ActivityDedupCache();
    }

    @Bean
    @ConditionalOnMissingBean
    public PoolActivityListener poolActivityListener(ObjectMapper objectMapper,
                                                     ActivityRegistry activityRegistry,
                                                     FuseFlowWorker fuseFlowWorker,
                                                     WorkflowControlCache controlCache,
                                                     ActivityDedupCache dedupCache) {
        return new PoolActivityListener(objectMapper, activityRegistry, fuseFlowWorker, controlCache, dedupCache);
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
