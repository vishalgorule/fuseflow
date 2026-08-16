package io.fuseflow.definition.config;

import io.fuseflow.common.validation.DagValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares beans for the shared types that moved to {@code fuseflow-common} (Phase 6): the
 * {@link DagValidator} is now a plain class there (usable from the SDK's runtime scanner and
 * compile-time processor), so the definition service exposes it as a bean here.
 */
@Configuration
public class DefinitionServiceConfig {

    @Bean
    public DagValidator dagValidator() {
        return new DagValidator();
    }
}
