package io.clubone.billing.security;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Level-scoped access for pricing / redemption locations.
 * Empty allowed set ⇒ no rows (hard cutover, never all levels).
 */
@Component
@Slf4j
public class AccessGuard {

  public TenantContext requireCtx() {
    return AccessContext.require();
  }

  public void requireLevelAccess(UUID levelId) {
    if (levelId == null) {
      throw new ForbiddenException("levelId is required");
    }
    var allowed = allowedLevels();
    if (!allowed.contains(levelId)) {
      log.warn("Denied level access levelId={} actor={}", levelId, AccessContext.actorApplicationUserId());
      throw new ForbiddenException("No access to level " + levelId);
    }
  }

  /** Immutable; empty means no accessible levels. */
  public Set<UUID> allowedLevels() {
    var ctx = requireCtx();
    var s = ctx.accessibleLevelIds();
    Set<UUID> out = s == null ? Set.of() : Set.copyOf(s);
    log.debug("allowedLevels actor={} count={}", ctx.applicationUserId(), out.size());
    return out;
  }

  public UUID applicationId() {
    return AccessContext.applicationId();
  }

  /** Builds `column IN (?, ?, ?)` (or `1=0` if empty) and args in same order. */
  public static SqlInClause buildInClause(String column, Collection<?> values) {
    if (values == null || values.isEmpty()) {
      return new SqlInClause("1=0", List.of()); // nothing allowed → no rows
    }
    var qs = values.stream().map(v -> "?").collect(Collectors.joining(","));
    return new SqlInClause(column + " IN (" + qs + ")", new ArrayList<>(values));
  }

  public static final class SqlInClause {
    public final String sql;
    public final List<Object> args;
    public SqlInClause(String sql, List<Object> args) { this.sql = sql; this.args = args; }
  }
}
