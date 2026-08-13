package io.fuseflow.sdk.config;

import io.fuseflow.sdk.client.RegistryClient;
import io.fuseflow.sdk.consumer.ActivityDispatchListener;
import io.fuseflow.sdk.pub.ActivityResultPublisher;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.ActivityScanner;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
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
            @Value("${fuseflow.kafka.topic.activity-results:activity-results}") String topic) {
        return new ActivityResultPublisher(kafkaTemplate, objectMapper, topic);
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
    public ActivityDispatchListener activityDispatchListener(ObjectMapper objectMapper,
                                                             ActivityRegistry activityRegistry,
                                                             FuseFlowWorker fuseFlowWorker) {
        return new ActivityDispatchListener(objectMapper, activityRegistry, fuseFlowWorker);
    }
}
