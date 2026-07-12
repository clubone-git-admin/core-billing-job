package io.clubone.billing.config;

/**
 * @deprecated Replaced by {@link io.clubone.billing.security.SecurityConfig}
 * (enterprise hard-cutover tenant security). Kept as an empty stub so existing
 * imports do not break; no SecurityFilterChain is registered here.
 */
@Deprecated
public class DefaultSecurityConfig {
  // Intentionally empty — permitAll chain removed for hard cutover.
}
