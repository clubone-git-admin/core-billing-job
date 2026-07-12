package io.clubone.billing.security;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Request-scoped tenant/actor context populated by {@link ActorOnlyContextFilter}.
 * No identity fallbacks — blank name/email/timezone are rejected at construction.
 */
public class TenantContext {

  private static final ThreadLocal<TenantContext> TL = new ThreadLocal<>();

  public static void set(TenantContext ctx) {
    TL.set(ctx);
  }

  public static TenantContext get() {
    return TL.get();
  }

  public static void clear() {
    TL.remove();
  }

  private final UUID applicationUserId;
  private final UUID userId;
  private final UUID applicationId;
  private final UUID orgClientId;
  private final boolean userActive;
  private final boolean appActive;
  private final Set<String> roles;
  private final Set<String> scopes;
  private final Set<UUID> accessibleLevelIds;
  private final UUID workingLocation;
  private final String userName;
  private final String userEmail;
  private final String loggedInTimezone;

  public TenantContext(
      UUID applicationUserId,
      UUID userId,
      UUID applicationId,
      UUID orgClientId,
      boolean userActive,
      boolean appActive,
      Collection<String> roles,
      Collection<String> scopes,
      Collection<UUID> accessibleLevelIds,
      UUID workingLocation,
      String userName,
      String userEmail,
      String loggedInTimezone) {
    this.applicationUserId = Objects.requireNonNull(applicationUserId, "applicationUserId");
    this.userId = Objects.requireNonNull(userId, "userId");
    this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
    this.orgClientId = Objects.requireNonNull(orgClientId, "orgClientId");
    this.userActive = userActive;
    this.appActive = appActive;
    this.roles = roles == null ? Set.of() : Set.copyOf(roles);
    this.scopes = scopes == null ? Set.of() : Set.copyOf(scopes);
    this.accessibleLevelIds = accessibleLevelIds == null ? Set.of() : Set.copyOf(accessibleLevelIds);
    this.workingLocation = Objects.requireNonNull(workingLocation, "workingLocation");
    this.userName = userName;
    this.userEmail = userEmail;
    this.loggedInTimezone = loggedInTimezone;
  }

  public UUID applicationUserId() {
    return applicationUserId;
  }

  public UUID userId() {
    return userId;
  }

  public UUID applicationId() {
    return applicationId;
  }

  public UUID orgClientId() {
    return orgClientId;
  }

  public boolean isUserActive() {
    return userActive;
  }

  public boolean isAppActive() {
    return appActive;
  }

  public Set<String> roles() {
    return roles;
  }

  public Set<String> scopes() {
    return scopes;
  }

  public Set<UUID> accessibleLevelIds() {
    return accessibleLevelIds;
  }

  public boolean hasRole(String role) {
    return roles.contains(role);
  }

  public boolean hasScope(String scope) {
    return scopes.contains(scope);
  }

  public boolean canAccessLevel(UUID levelId) {
    return accessibleLevelIds.contains(levelId);
  }

  public UUID workingLocation() {
    return workingLocation;
  }

  public String userName() {
    return userName;
  }

  public String userEmail() {
    return userEmail;
  }

  public String loggedInTimezone() {
    return loggedInTimezone;
  }
}
