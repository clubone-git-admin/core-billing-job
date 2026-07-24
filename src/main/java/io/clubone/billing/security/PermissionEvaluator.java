package io.clubone.billing.security;

import java.util.Locale;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * Method-security helpers for {@code @PreAuthorize("@perm....")}.
 *
 * <p>{@code access.get_actor_context} returns role <em>names</em> (e.g. {@code Platform Admin}),
 * not short codes — matching must account for that.
 */
@Component("perm")
public class PermissionEvaluator {

  /** Normalized role names treated as full admins (spaces/underscores/ROLE_ stripped). */
  private static final Set<String> ADMIN_ROLE_KEYS = Set.of(
      "ADMIN",
      "PLATFORM_ADMIN",
      "ORG_ADMIN");

  public boolean hasAnyRole(String... roles) {
    var ctx = TenantContext.get();
    if (ctx == null || roles == null) {
      return false;
    }
    for (String role : roles) {
      if (role != null && (ctx.hasRole(role) || ctx.hasRole("ROLE_" + role))) {
        return true;
      }
    }
    return false;
  }

  public boolean isAdmin() {
    if (hasAnyRole(
        "Admin", "ADMIN", "ROLE_Admin", "ROLE_ADMIN",
        "Platform Admin", "Org Admin",
        "PLATFORM_ADMIN", "ORG_ADMIN")) {
      return true;
    }
    var ctx = TenantContext.get();
    if (ctx == null) {
      return false;
    }
    for (String role : ctx.roles()) {
      if (isAdminRoleName(role)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Admin, known billing/finance admin codes, Finance role, or any actor role whose name
   * contains BILLING / FINANCE / RECON (from {@code access.get_actor_context} role names).
   */
  public boolean canManageBilling() {
    if (isAdmin() || hasAnyRole("BILLING_ADMIN", "FINANCE_ADMIN", "RECON_ADMIN", "Finance")) {
      return true;
    }
    var ctx = TenantContext.get();
    if (ctx == null) {
      return false;
    }
    if (ctx.isExternal() && (ctx.hasScope("billing:write") || ctx.hasScope("billing:read")
        || ctx.hasScope("reconciliation:write") || ctx.hasScope("reconciliation:read"))) {
      return true;
    }
    for (String role : ctx.roles()) {
      if (role == null) {
        continue;
      }
      String u = role.toUpperCase(Locale.ROOT);
      if (u.contains("BILLING") || u.contains("FINANCE") || u.contains("RECON")) {
        return true;
      }
    }
    return false;
  }

  /** POS checkout + acquisition writes — any authenticated actor with a real role (not anonymous). */
  public boolean canOperatePos() {
    var ctx = TenantContext.get();
    if (ctx == null || !ctx.isUserActive()) return false;
    if (ctx.isExternal() && (ctx.hasScope("billing:write") || ctx.hasScope("crm:write"))) {
      return true;
    }
    // Any non-empty role set from get_actor_context means staff/POS user
    return ctx.roles() != null && !ctx.roles().isEmpty()
        || isAdmin()
        || hasAnyRole("POS","POS_USER","FRONT_DESK","SALES","STAFF","MEMBERSHIP");
  }

  public boolean canManageRefunds() {
    return canManageBilling() || hasAnyRole("REFUND","REFUND_ADMIN","SUPPORT");
  }

  static boolean isAdminRoleName(String role) {
    if (role == null || role.isBlank()) {
      return false;
    }
    String n = role.trim().toUpperCase(Locale.ROOT)
        .replace(' ', '_')
        .replace('-', '_');
    if (n.startsWith("ROLE_")) {
      n = n.substring(5);
    }
    return ADMIN_ROLE_KEYS.contains(n);
  }
}
