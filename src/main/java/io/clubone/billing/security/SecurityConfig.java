package io.clubone.billing.security;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Configuration
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {

  private final ActorOnlyContextFilter actorOnlyContextFilter;
  private final ObjectMapper objectMapper;

  public SecurityConfig(ActorOnlyContextFilter actorOnlyContextFilter, ObjectMapper objectMapper) {
    this.actorOnlyContextFilter = actorOnlyContextFilter;
    this.objectMapper = objectMapper;
  }

  @Bean
  @Order(1)
  SecurityFilterChain publicEndpointsFilterChain(HttpSecurity http) throws Exception {
    return http
        .securityMatcher(PublicApiPaths::isPublic)
        .csrf(AbstractHttpConfigurer::disable)
        .cors(cors -> cors.configurationSource(corsConfigurationSource()))
        .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
        .build();
  }

  @Bean
  @Order(2)
  SecurityFilterChain apiFilterChain(HttpSecurity http) throws Exception {
    return http
        .csrf(AbstractHttpConfigurer::disable)
        .anonymous(AbstractHttpConfigurer::disable)
        .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .cors(cors -> cors.configurationSource(corsConfigurationSource()))
        .exceptionHandling(ex -> ex
            .authenticationEntryPoint(this::unauthorized)
            .accessDeniedHandler(this::forbidden))
        .authorizeHttpRequests(auth -> auth
            .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
            .anyRequest().authenticated())
        .addFilterBefore(actorOnlyContextFilter, UsernamePasswordAuthenticationFilter.class)
        .build();
  }

  private void unauthorized(HttpServletRequest request, HttpServletResponse response,
      org.springframework.security.core.AuthenticationException ex) throws IOException {
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
    objectMapper.writeValue(response.getWriter(), Map.of(
        "error", "unauthorized",
        "message", "Authentication required — provide X-Actor-Id and X-Location-Id headers",
        "path", request.getRequestURI(),
        "timestamp", Instant.now().toString()));
  }

  private void forbidden(HttpServletRequest request, HttpServletResponse response,
      org.springframework.security.access.AccessDeniedException ex) throws IOException {
    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
    objectMapper.writeValue(response.getWriter(), Map.of(
        "error", "forbidden",
        "message", ex.getMessage() != null ? ex.getMessage() : "Access denied",
        "path", request.getRequestURI(),
        "timestamp", Instant.now().toString()));
  }

  @Bean
  public CorsConfigurationSource corsConfigurationSource() {
    CorsConfiguration cfg = new CorsConfiguration();
    cfg.setAllowedOriginPatterns(List.of(
        "http://localhost:*",
        "http://127.0.0.1:*",
        "http://[::1]:*",
        "https://ops.clubone.io",
        "https://*.clubone.io"));
    cfg.setAllowCredentials(true);
    cfg.setAllowedMethods(List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"));
    cfg.setAllowedHeaders(List.of("*"));
    cfg.setExposedHeaders(List.of("Content-Type", "X-Actor-Id", "X-Location-Id", "application-id"));
    cfg.setMaxAge(3600L);

    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", cfg);
    return source;
  }
}
