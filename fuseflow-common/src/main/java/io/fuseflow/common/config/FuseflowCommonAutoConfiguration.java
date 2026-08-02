package io.fuseflow.common.config;

import io.fuseflow.common.correlation.CorrelationIdFilter;
import io.fuseflow.common.exception.GlobalExceptionHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

/**
 * Auto-configuration that applies FuseFlow common concerns to any service that
 * has this module on its classpath:
 * <ul>
 *   <li>{@link CorrelationIdFilter} — correlation-ID propagation on every request</li>
 *   <li>{@link GlobalExceptionHandler} — uniform {@code ApiError} responses</li>
 * </ul>
 * (JSON conventions — ISO-8601 timestamps, non-null inclusion — are applied per
 * service via {@code spring.jackson.*} properties in {@code application.yml}.)
 * Registered via
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class FuseflowCommonAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public FilterRegistrationBean<CorrelationIdFilter> correlationIdFilterRegistration() {
        FilterRegistrationBean<CorrelationIdFilter> registration =
                new FilterRegistrationBean<>(new CorrelationIdFilter());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        registration.addUrlPatterns("/*");
        return registration;
    }

    @Bean
    @ConditionalOnMissingBean
    public GlobalExceptionHandler globalExceptionHandler() {
        return new GlobalExceptionHandler();
    }
}

