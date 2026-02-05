package io.clubone.billing.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Default security configuration when OAuth2 is not enabled.
 * Permits all requests - no authentication required.
 * This ensures the API is accessible when OAuth2 is not configured.
 * Only activates when OAuth2SecurityConfig is not present.
 */
@Configuration
@EnableWebSecurity
public class DefaultSecurityConfig {

    @Bean("defaultSecurityFilterChain")
    @ConditionalOnMissingBean(name = "oauth2SecurityFilterChain")
    public SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf.disable())
            .authorizeHttpRequests(auth -> auth
                .anyRequest().permitAll()
            );

        return http.build();
    }
}
