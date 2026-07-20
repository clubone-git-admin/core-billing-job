package io.clubone.billing.security;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Paths that do not require tenant headers:
 * health, actuator (including Spring Batch actuator), swagger/OpenAPI docs only.
 * Join lead-convert uses {@link TenantOptionalApiPaths} (optional actor), not this list.
 */
public final class PublicApiPaths {

  private PublicApiPaths() {
  }

  public static boolean isPublic(HttpServletRequest request) {
    return isPublicPath(resolvePath(request));
  }

  public static boolean isPublicPath(String path) {
    if (path == null || path.isEmpty()) {
      return false;
    }
    // Gateway exposes /crm/health; local/actuator still use /health.
    return path.equals("/health")
        || path.equals("/crm/health")
        || path.startsWith("/health/")
        || path.equals("/actuator")
        || path.startsWith("/actuator/")
        || path.equals("/swagger-ui.html")
        || path.startsWith("/swagger-ui/")
        || path.startsWith("/docs")
        || path.equals("/v3/api-docs")
        || path.startsWith("/v3/api-docs/");
  }

  public static String resolvePath(HttpServletRequest request) {
    String servletPath = request.getServletPath();
    if (servletPath != null && !servletPath.isEmpty()) {
      return servletPath;
    }
    String uri = request.getRequestURI();
    String context = request.getContextPath();
    if (context != null && !context.isEmpty() && uri.startsWith(context)) {
      return uri.substring(context.length());
    }
    return uri;
  }
}
