package io.clubone.billing.config;

/**
 * @deprecated OAuth2 resource-server chain disabled in favor of
 * {@link io.clubone.billing.security.SecurityConfig} (actor/tenant hard cutover).
 * Kept as an empty stub so existing imports do not break.
 */
@Deprecated
public class SecurityConfig {
  // Intentionally empty — OAuth2 permit/authenticated chain removed for hard cutover.
}
