package io.clubone.billing.security;

import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

/**
 * Builds outbound tenant headers from the current {@link TenantContext}.
 */
public final class TenantHttpHeaders {

  private TenantHttpHeaders() {
  }

  public static HttpHeaders fromContext() {
    TenantContext ctx = TenantContext.get();
    if (ctx == null) {
      throw new UnauthorizedException(
          "Tenant context is required for outbound service calls");
    }
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Actor-Id", ctx.applicationUserId().toString());
    headers.set("X-Location-Id", ctx.workingLocation().toString());
    headers.set("application-id", ctx.applicationId().toString());
    return headers;
  }

  public static HttpHeaders from(UUID actorId, UUID locationId, UUID applicationId) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Actor-Id", actorId.toString());
    headers.set("X-Location-Id", locationId.toString());
    headers.set("application-id", applicationId.toString());
    return headers;
  }
}
