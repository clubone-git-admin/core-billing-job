package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Resolves {@link locations.levels} branches to {@link locations.location} rows.
 */
@Repository
public class LocationLevelRepository {

    private final JdbcTemplate jdbc;

    public LocationLevelRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<UUID> findApplicationIdForLevel(UUID levelId) {
        if (levelId == null) {
            return Optional.empty();
        }
        try {
            UUID app = jdbc.queryForObject(
                    "SELECT application_id FROM locations.levels WHERE level_id = ?::uuid",
                    UUID.class,
                    levelId.toString());
            return Optional.ofNullable(app);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * All location_ids under the org that are valid {@code locations.location} rows.
     */
    public List<UUID> listAllLocationIdsForApplication(UUID applicationId) {
        if (applicationId == null) {
            return List.of();
        }
        return jdbc.query(
                "SELECT location_id FROM locations.location WHERE application_id = ?::uuid",
                (rs, rn) -> (UUID) rs.getObject("location_id"),
                applicationId.toString());
    }

    /**
     * Resolves a level to concrete locations: for each level row in the branch (optionally
     * including descendants), if {@code reference_entity_id} is a known location, it is included.
     */
    public List<LocationRow> resolveLocationsForLevel(UUID levelId, boolean includeChildLevels) {
        if (levelId == null) {
            return List.of();
        }
        if (includeChildLevels) {
            return jdbc.query(
                    """
                    WITH RECURSIVE sub AS (
                        SELECT level_id, parent_level_id, reference_entity_id, application_id
                        FROM locations.levels
                        WHERE level_id = ?::uuid
                        UNION ALL
                        SELECT l.level_id, l.parent_level_id, l.reference_entity_id, l.application_id
                        FROM locations.levels l
                        INNER JOIN sub s ON l.parent_level_id = s.level_id
                    )
                    SELECT DISTINCT loc.location_id, loc.display_name
                    FROM sub
                    INNER JOIN locations.location loc
                        ON loc.location_id = sub.reference_entity_id
                        AND loc.application_id = sub.application_id
                    ORDER BY loc.display_name
                    """,
                    (rs, rn) -> new LocationRow(
                            (UUID) rs.getObject("location_id"),
                            rs.getString("display_name")),
                    levelId.toString());
        }
        return jdbc.query(
                """
                SELECT loc.location_id, loc.display_name
                FROM locations.levels lv
                INNER JOIN locations.location loc
                    ON loc.location_id = lv.reference_entity_id
                    AND loc.application_id = lv.application_id
                WHERE lv.level_id = ?::uuid
                ORDER BY loc.display_name
                """,
                (rs, rn) -> new LocationRow(
                        (UUID) rs.getObject("location_id"),
                        rs.getString("display_name")),
                levelId.toString());
    }

    public Optional<UUID> findLevelIdByReferenceLocation(UUID locationId, UUID applicationId) {
        if (locationId == null || applicationId == null) {
            return Optional.empty();
        }
        try {
            UUID id = jdbc.queryForObject(
                    "SELECT level_id FROM locations.levels "
                            + "WHERE reference_entity_id = ?::uuid AND application_id = ?::uuid "
                            + "ORDER BY level_id LIMIT 1",
                    UUID.class,
                    locationId.toString(),
                    applicationId.toString());
            return Optional.ofNullable(id);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<String> findLevelName(UUID levelId) {
        if (levelId == null) {
            return Optional.empty();
        }
        try {
            String n = jdbc.queryForObject(
                    "SELECT l.\"name\"::text FROM locations.levels l WHERE l.level_id = ?::uuid",
                    String.class,
                    levelId.toString());
            return Optional.ofNullable(n);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Flat level rows for building a hierarchy tree (same app). Used by billing location filter UI.
     */
    public List<Map<String, Object>> listLevelsByApplicationId(UUID applicationId) {
        if (applicationId == null) {
            return List.of();
        }
        return jdbc.queryForList(
                """
                SELECT level_id, parent_level_id, "name"::text AS name
                FROM locations.levels
                WHERE application_id = ?::uuid
                """,
                applicationId.toString());
    }

    public List<LocationRow> findLocationsByIds(Collection<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(locationIds.size(), "?::uuid"));
        List<Object> params = new ArrayList<>();
        for (UUID id : locationIds) {
            params.add(id.toString());
        }
        return jdbc.query(
                "SELECT location_id, display_name FROM locations.location WHERE location_id IN ("
                        + placeholders
                        + ")",
                (rs, rn) -> new LocationRow(
                        (UUID) rs.getObject("location_id"),
                        rs.getString("display_name")),
                params.toArray());
    }

    public record LocationRow(UUID locationId, String displayName) {
    }
}
