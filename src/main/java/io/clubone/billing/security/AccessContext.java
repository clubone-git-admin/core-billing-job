package io.clubone.billing.security;

import java.util.UUID;

public final class AccessContext {

  private AccessContext() {
  }

  public static TenantContext require() {
    TenantContext ctx = TenantContext.get();
    if (ctx == null) {
      throw new UnauthorizedException("Actor context is required");
    }
    return ctx;
  }

  public static UUID requireApplicationId(String applicationIdHeader) {
    TenantContext ctx = require();
    if (applicationIdHeader == null || applicationIdHeader.isBlank()) {
      throw new UnauthorizedException("application-id header is required");
    }
    UUID headerAppId;
    try {
      headerAppId = UUID.fromString(applicationIdHeader.trim());
    } catch (IllegalArgumentException ex) {
      throw new UnauthorizedException("Invalid application-id header");
    }
    if (!ctx.applicationId().equals(headerAppId)) {
      throw new ForbiddenException("application-id does not match authenticated actor");
    }
    return headerAppId;
  }

  public static UUID actorUserId() {
    return require().userId();
  }

  public static UUID actorApplicationUserId() {
    return require().applicationUserId();
  }

  public static UUID workingLocationId() {
    UUID loc = require().workingLocation();
    if (loc == null) {
      throw new ForbiddenException("X-Location-Id is required for this operation");
    }
    return loc;
  }

  public static UUID applicationId() {
    return require().applicationId();
  }
}
