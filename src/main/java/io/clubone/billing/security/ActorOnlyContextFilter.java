package io.clubone.billing.security;

import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * Hard-cutover tenant filter. Required headers:
 * X-Actor-Id, X-Location-Id, application-id (must match actor application).
 * No optional / fallback tenant paths.
 */
@Component
@Slf4j
public class ActorOnlyContextFilter extends OncePerRequestFilter {

  private final JdbcTemplate jdbc;
  private final ActorCache cache;
  private final LocationAccessValidator locationAccessValidator;

  public ActorOnlyContextFilter(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ActorCache cache,
      LocationAccessValidator locationAccessValidator) {
    this.jdbc = jdbc;
    this.cache = cache;
    this.locationAccessValidator = locationAccessValidator;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    if ("OPTIONS".equalsIgnoreCase(req.getMethod())) {
      return true;
    }
    return PublicApiPaths.isPublic(req);
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {

    final String path = PublicApiPaths.resolvePath(req);
    final String method = req.getMethod();
    log.debug("Tenant filter: {} {}", method, path);

    String actorHeader = req.getHeader("X-Actor-Id");
    String locationHeader = req.getHeader("X-Location-Id");
    String appHeader = firstNonBlank(
        req.getHeader("application-id"),
        firstNonBlank(req.getHeader("X-Application-Id"), req.getHeader("X-App-Id")));

    if (actorHeader == null || actorHeader.isBlank()) {
      FilterErrorWriter.write(res, 400, "bad_request", "X-Actor-Id header is required");
      return;
    }

    if (locationHeader == null || locationHeader.isBlank()) {
      FilterErrorWriter.write(res, 400, "bad_request", "X-Location-Id header is required");
      return;
    }

    if (appHeader == null || appHeader.isBlank()) {
      FilterErrorWriter.write(res, 400, "bad_request", "application-id header is required");
      return;
    }

    final UUID actorId;
    final UUID workingLoc;
    try {
      actorId = UUID.fromString(actorHeader.trim());
    } catch (IllegalArgumentException e) {
      FilterErrorWriter.write(res, 400, "bad_request", "Invalid X-Actor-Id header");
      return;
    }
    try {
      workingLoc = UUID.fromString(locationHeader.trim());
    } catch (IllegalArgumentException e) {
      FilterErrorWriter.write(res, 400, "bad_request", "Invalid X-Location-Id header");
      return;
    }

    ActorCtx ctx = cache.get(actorId, workingLoc, this::loadActorCtx);
    if (ctx == null || !ctx.isUserActive() || !ctx.isAppActive()) {
      FilterErrorWriter.write(res, 403, "forbidden", "Actor inactive or not found");
      return;
    }

    if (!locationAccessValidator.canAccessLocation(actorId, workingLoc, ctx.accessibleLevelIds())) {
      FilterErrorWriter.write(res, 403, "forbidden", "X-Location-Id is not accessible to this actor");
      return;
    }

    if (!ctx.applicationId().toString().equalsIgnoreCase(appHeader.trim())) {
      FilterErrorWriter.write(res, 403, "forbidden", "application-id mismatch");
      return;
    }

    var authorities = new ArrayList<SimpleGrantedAuthority>();
    for (String r : ctx.roles()) {
      if (r != null && !r.isBlank()) {
        authorities.add(new SimpleGrantedAuthority(r.startsWith("ROLE_") ? r : "ROLE_" + r));
      }
    }

    var auth = new UsernamePasswordAuthenticationToken(ctx.applicationUserId().toString(), null, authorities);
    SecurityContextHolder.getContext().setAuthentication(auth);

    TenantContext.set(new TenantContext(
        ctx.applicationUserId(),
        ctx.userId(),
        ctx.applicationId(),
        ctx.orgClientId(),
        ctx.isUserActive(),
        ctx.isAppActive(),
        List.of(ctx.roles()),
        List.of(ctx.scopes()),
        Set.copyOf(Arrays.asList(ctx.accessibleLevelIds())),
        workingLoc,
        ctx.userName(),
        ctx.userEmail(),
        ctx.loggedInTimezone()));

    log.debug("Tenant context set actor={} location={} app={}", actorId, workingLoc, ctx.applicationId());

    try {
      chain.doFilter(req, res);
    } finally {
      TenantContext.clear();
      SecurityContextHolder.clearContext();
    }
  }

  private ActorCtx loadActorCtx(UUID applicationUserId, UUID workingLocation) {
    try {
      return jdbc.queryForObject("""
          SELECT application_user_id, user_id, application_id, org_client_id,
                 is_user_active, is_app_active, roles, scopes, accessible_level_ids,
                 user_name, user_email, logged_in_timezone
          FROM access.get_actor_context(?, ?)
          """,
          (rs, i) -> new ActorCtx(
              UUID.fromString(rs.getString("application_user_id")),
              UUID.fromString(rs.getString("user_id")),
              UUID.fromString(rs.getString("application_id")),
              UUID.fromString(rs.getString("org_client_id")),
              rs.getBoolean("is_user_active"),
              rs.getBoolean("is_app_active"),
              toStringArray(rs.getArray("roles")),
              toStringArray(rs.getArray("scopes")),
              toUuidArray(rs.getArray("accessible_level_ids")),
              rs.getString("user_name"),
              rs.getString("user_email"),
              rs.getString("logged_in_timezone")),
          applicationUserId, workingLocation);
    } catch (EmptyResultDataAccessException e) {
      log.warn("Actor context not found for applicationUserId={}", applicationUserId);
      return null;
    }
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a;
    }
    if (b != null && !b.isBlank()) {
      return b;
    }
    return null;
  }

  private static String[] toStringArray(Array a) throws java.sql.SQLException {
    return a == null ? new String[0] : (String[]) a.getArray();
  }

  private static UUID[] toUuidArray(Array a) throws java.sql.SQLException {
    if (a == null) {
      return new UUID[0];
    }
    Object[] raw = (Object[]) a.getArray();
    UUID[] out = new UUID[raw.length];
    for (int i = 0; i < raw.length; i++) {
      Object v = raw[i];
      out[i] = (v instanceof UUID u) ? u : UUID.fromString(String.valueOf(v));
    }
    return out;
  }

  private record ActorCtx(
      UUID applicationUserId,
      UUID userId,
      UUID applicationId,
      UUID orgClientId,
      boolean isUserActive,
      boolean isAppActive,
      String[] roles,
      String[] scopes,
      UUID[] accessibleLevelIds,
      String userName,
      String userEmail,
      String loggedInTimezone) {
  }
}
