package io.clubone.billing.security;

import java.util.Locale;

import org.springframework.stereotype.Component;

/**
 * Method-security helpers for {@code @PreAuthorize("@perm....")}.
 */
@Component("perm")
public class PermissionEvaluator {

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
    return hasAnyRole("Admin", "ADMIN", "ROLE_Admin", "ROLE_ADMIN");
  }

  /**
   * Admin, known billing/finance admin codes, or any actor role whose code contains
   * BILLING / FINANCE / RECON (from {@code access.get_actor_context} role codes).
   */
  public boolean canManageBilling() {
    if (isAdmin() || hasAnyRole("BILLING_ADMIN", "FINANCE_ADMIN", "RECON_ADMIN")) {
      return true;
    }
    var ctx = TenantContext.get();
    if (ctx == null) {
      return false;
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
    // Any non-empty role set from get_actor_context means staff/POS user
    return ctx.roles() != null && !ctx.roles().isEmpty()
        || isAdmin()
        || hasAnyRole("POS","POS_USER","FRONT_DESK","SALES","STAFF","MEMBERSHIP");
  }

  public boolean canManageRefunds() {
    return canManageBilling() || hasAnyRole("REFUND","REFUND_ADMIN","SUPPORT");
  }
}
