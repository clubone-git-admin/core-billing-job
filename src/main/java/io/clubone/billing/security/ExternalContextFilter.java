package io.clubone.billing.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Qualifier;
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
 * External API Gateway principal for billing / CRM / reconciliation APIs hosted here.
 */
@Component
@Slf4j
public class ExternalContextFilter extends OncePerRequestFilter {

  private final JdbcTemplate jdbc;

  public ExternalContextFilter(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest req) {
    if ("OPTIONS".equalsIgnoreCase(req.getMethod())) {
      return true;
    }
    if (PublicApiPaths.isPublic(req)) {
      return true;
    }
    String clientId = req.getHeader(ExternalAuth.HEADER_CLIENT_ID);
    return clientId == null || clientId.isBlank();
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {

    String clientId = req.getHeader(ExternalAuth.HEADER_CLIENT_ID).trim();
    Set<String> scopes = parseScopes(req.getHeader(ExternalAuth.HEADER_SCOPES));
    if (scopes.isEmpty()) {
      FilterErrorWriter.write(res, HttpServletResponse.SC_FORBIDDEN, "forbidden",
          "X-External-Scopes is required for external clients");
      return;
    }

    String appHeader = firstNonBlank(
        req.getHeader(ExternalAuth.HEADER_APPLICATION_ID),
        req.getHeader(ExternalAuth.HEADER_APPLICATION_ID_ALT),
        req.getHeader("X-Application-Id"));
    if (appHeader == null) {
      FilterErrorWriter.write(res, HttpServletResponse.SC_BAD_REQUEST, "bad_request",
          "X-External-Application-Id (or application-id) is required for external clients");
      return;
    }

    final UUID applicationId;
    try {
      applicationId = UUID.fromString(appHeader.trim());
    } catch (IllegalArgumentException e) {
      FilterErrorWriter.write(res, HttpServletResponse.SC_BAD_REQUEST, "bad_request",
          "Invalid X-External-Application-Id / application-id");
      return;
    }

    String path = PublicApiPaths.resolvePath(req);
    String method = req.getMethod() == null ? "GET" : req.getMethod().toUpperCase();
    String[] required = resolveRequiredScopes(path, method);
    if (required == null) {
      FilterErrorWriter.write(res, HttpServletResponse.SC_FORBIDDEN, "forbidden",
          "No partner scope mapped for path " + path);
      return;
    }
    boolean ok = false;
    for (String s : required) {
      if (scopes.contains(s)) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      FilterErrorWriter.write(res, HttpServletResponse.SC_FORBIDDEN, "forbidden",
          "insufficient_scope: one of " + Arrays.toString(required) + " required");
      return;
    }

    Set<UUID> accessibleLevels = loadAllLevelIds(applicationId);
    UUID synthetic = ExternalAuth.SYNTHETIC_NIL;
    TenantContext ctx = new TenantContext(
        synthetic, synthetic, applicationId, applicationId,
        true, true, List.of("EXTERNAL"), scopes, accessibleLevels, synthetic,
        "external:" + clientId, "external@" + clientId, "UTC", true, clientId);

    List<SimpleGrantedAuthority> authorities = scopes.stream()
        .map(s -> new SimpleGrantedAuthority("SCOPE_" + s))
        .collect(Collectors.toList());
    authorities.add(new SimpleGrantedAuthority("ROLE_EXTERNAL"));

    SecurityContextHolder.getContext().setAuthentication(
        new UsernamePasswordAuthenticationToken(clientId, "external", authorities));
    TenantContext.set(ctx);
    req.setAttribute(ExternalAuth.REQUEST_ATTR, Boolean.TRUE);

    log.info("External context clientId={} appId={} scopes={} levels={} path={}",
        clientId, applicationId, scopes, accessibleLevels.size(), path);

    try {
      chain.doFilter(req, res);
    } finally {
      TenantContext.clear();
      SecurityContextHolder.clearContext();
    }
  }

  /**
   * Returns acceptable scopes for the path+method, or null if not a partner surface.
   */
  static String[] resolveRequiredScopes(String path, String method) {
    boolean mutating = "POST".equals(method) || "PUT".equals(method)
        || "PATCH".equals(method) || "DELETE".equals(method);
    if (path.startsWith("/api/crm") || path.startsWith("/crm")) {
      return mutating
          ? new String[] {"crm:write"}
          : new String[] {"crm:read", "crm:write"};
    }
    if (path.startsWith("/reconciliation") || path.contains("/reconciliation")) {
      return mutating
          ? new String[] {"reconciliation:write", "billing:write"}
          : new String[] {"reconciliation:read", "reconciliation:write", "billing:read", "billing:write"};
    }
    if (path.startsWith("/api/v1/billing") || path.startsWith("/api/billing")
        || path.startsWith("/billing")) {
      return mutating
          ? new String[] {"billing:write"}
          : new String[] {"billing:read", "billing:write"};
    }
    // Other authenticated billing-job paths: accept any of the hosted domain scopes
    return mutating
        ? new String[] {"billing:write", "crm:write", "reconciliation:write"}
        : new String[] {
            "billing:read", "billing:write",
            "crm:read", "crm:write",
            "reconciliation:read", "reconciliation:write"
        };
  }

  private Set<UUID> loadAllLevelIds(UUID applicationId) {
    List<UUID> ids = jdbc.query("""
        SELECT l.level_id
        FROM locations.levels l
        WHERE l.application_id = ?
        """,
        (rs, i) -> (UUID) rs.getObject("level_id"),
        applicationId);
    return Set.copyOf(ids);
  }

  private static Set<String> parseScopes(String header) {
    if (header == null || header.isBlank()) {
      return Set.of();
    }
    return Arrays.stream(header.trim().split("[\\s,]+"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(HashSet::new));
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }
}
