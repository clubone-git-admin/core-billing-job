package io.clubone.billing.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LocationAccessValidator {

  private final JdbcTemplate jdbc;

  public LocationAccessValidator(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public boolean canAccessLocation(UUID applicationUserId, UUID locationId, UUID[] accessibleLevelIds) {
    int directCount = countDirectRolesAtLocation(applicationUserId, locationId);
    if (directCount > 0) {
      log.debug("Actor {} allowed at location {} ({} direct role assignment(s))", applicationUserId,
          locationId, directCount);
      return true;
    }

    if (accessibleLevelIds != null && accessibleLevelIds.length > 0
        && locationMapsToAccessibleLevel(locationId, accessibleLevelIds)) {
      log.debug("Actor {} allowed at location {} via hierarchy", applicationUserId, locationId);
      return true;
    }

    log.warn("Actor {} denied at location {}", applicationUserId, locationId);
    return false;
  }

  public int countDirectRolesAtLocation(UUID applicationUserId, UUID locationId) {
    Integer count = jdbc.queryForObject("""
        SELECT COUNT(*)::int
        FROM access.access_user_role_location aurl
        JOIN access.access_application_user aau
          ON aau.application_user_id = aurl.application_user_id
         AND aau.is_active = true
        JOIN access.access_role ar ON ar.role_id = aurl.role_id
        JOIN access.lu_role_status rs ON rs.role_status_id = ar.role_status_id AND rs.code = 'ACTIVE'
        WHERE aurl.application_user_id = ?
          AND aurl.location_id = ?
          AND COALESCE(aurl.is_active, true) = true
        """, Integer.class, applicationUserId, locationId);
    return count != null ? count : 0;
  }

  private boolean locationMapsToAccessibleLevel(UUID locationId, UUID[] accessibleLevelIds) {
    String placeholders = String.join(",",
        java.util.Collections.nCopies(accessibleLevelIds.length, "?"));
    String sql = """
        SELECT EXISTS (
          SELECT 1
          FROM locations.levels l
          WHERE (l.reference_entity_id = ? OR l.level_id = ?)
            AND l.level_id IN (%s)
        )
        """.formatted(placeholders);

    List<Object> params = new ArrayList<>();
    params.add(locationId);
    params.add(locationId);
    params.addAll(Arrays.asList(accessibleLevelIds));

    Boolean ok = jdbc.queryForObject(sql, Boolean.class, params.toArray());
    return Boolean.TRUE.equals(ok);
  }
}
