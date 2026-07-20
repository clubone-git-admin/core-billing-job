package io.clubone.billing.security;

import java.util.regex.Pattern;

/**
 * Endpoints used by both authenticated CRM staff and the public Join funnel.
 *
 * <p>When all tenant headers are present and the actor is valid, normal tenant
 * validation still runs. Otherwise the filter installs a limited optional principal.
 */
public final class TenantOptionalApiPaths {

  private static final Pattern JOIN_LEAD_CONVERT = Pattern.compile(
      "^/api/crm/leads/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/status/converted$");

  private TenantOptionalApiPaths() {
  }

  public static boolean isOptional(String path, String method) {
    if (path == null || method == null) {
      return false;
    }
    // Public Join: convert lead → client after details, before payment.
    return "PATCH".equalsIgnoreCase(method) && JOIN_LEAD_CONVERT.matcher(path).matches();
  }
}
