package io.clubone.billing.batch.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Enterprise-grade configuration for billing job.
 * Configures circuit breakers, retries, and other resilience patterns.
 */
@Configuration
public class EnterpriseConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
