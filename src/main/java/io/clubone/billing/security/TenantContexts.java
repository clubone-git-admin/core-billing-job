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
   * Does not represent a real actor — sufficient for {@link AccessContext#applicationId()}.
   */
  public static TenantContext forBackgroundJob(UUID applicationId, UUID workingLocation) {
    UUID app = require(applicationId, "applicationId");
    UUID loc = workingLocation != null ? workingLocation : ExternalAuth.SYNTHETIC_NIL;
    UUID system = ExternalAuth.SYNTHETIC_NIL;
    return new TenantContext(
        system,
        system,
        app,
        system,
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
