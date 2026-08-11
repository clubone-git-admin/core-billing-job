package io.clubone.billing.security;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Helpers for installing {@link TenantContext} on worker threads (async jobs, batch).
 * HTTP filters clear ThreadLocal; async executors do not inherit it.
 */
public final class TenantContexts {

  private TenantContexts() {}

  /**
   * Minimal tenant for background work scoped by {@code applicationId} + location.
   * Uses a synthetic actor unless {@link #forBackgroundJob(UUID, UUID, UUID)} provides a real one.
   */
  public static TenantContext forBackgroundJob(UUID applicationId, UUID workingLocation) {
    return forBackgroundJob(applicationId, workingLocation, ExternalAuth.SYNTHETIC_NIL);
  }

  /**
   * Background tenant with an explicit actor (e.g. first active app user) for outbound service headers.
   */
  public static TenantContext forBackgroundJob(UUID applicationId, UUID workingLocation, UUID applicationUserId) {
    UUID app = require(applicationId, "applicationId");
    UUID loc = workingLocation != null ? workingLocation : ExternalAuth.SYNTHETIC_NIL;
    UUID actor = applicationUserId != null ? applicationUserId : ExternalAuth.SYNTHETIC_NIL;
    return new TenantContext(
        actor,
        actor,
        app,
        actor,
        true,
        true,
        List.of("SYSTEM"),
        List.of(),
        Set.of(),
        loc,
        "system",
        "system@local",
        "UTC");
  }

  public static void run(TenantContext ctx, Runnable action) {
    TenantContext previous = TenantContext.get();
    try {
      if (ctx != null) {
        TenantContext.set(ctx);
      }
      action.run();
    } finally {
      if (previous != null) {
        TenantContext.set(previous);
      } else {
        TenantContext.clear();
      }
    }
  }

  public static <T> T call(TenantContext ctx, Supplier<T> action) {
    TenantContext previous = TenantContext.get();
    try {
      if (ctx != null) {
        TenantContext.set(ctx);
      }
      return action.get();
    } finally {
      if (previous != null) {
        TenantContext.set(previous);
      } else {
        TenantContext.clear();
      }
    }
  }

  private static UUID require(UUID id, String name) {
    if (id == null) {
      throw new IllegalArgumentException(name + " is required");
    }
    return id;
  }
}
