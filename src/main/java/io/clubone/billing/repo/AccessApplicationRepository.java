package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Resolves org_client_id from application_id using access.access_application.
 * Used by CRM request context (X-Application-Id header).
 */
@Repository
public class AccessApplicationRepository {

    private final JdbcTemplate jdbc;

    public AccessApplicationRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Returns org_client_id for the given application_id when the application is active, else null.
     */
    public UUID findOrgClientIdByApplicationId(UUID applicationId) {
        if (applicationId == null) return null;
        try {
            return jdbc.queryForObject("""
                SELECT org_client_id
                FROM access.access_application
                WHERE application_id = ? AND is_active = true
                LIMIT 1
                """, UUID.class, applicationId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    /** Stable audit actor for public Join when X-Actor-Id is absent or inactive. */
    public UUID findSystemApplicationUserId(UUID applicationId) {
        ApplicationUserRef ref = findSystemApplicationUser(applicationId);
        return ref == null ? null : ref.applicationUserId();
    }

    public ApplicationUserRef findSystemApplicationUser(UUID applicationId) {
        if (applicationId == null) return null;
        try {
            return jdbc.queryForObject("""
                SELECT application_user_id, user_id
                FROM access.access_application_user
                WHERE application_id = ?
                  AND COALESCE(is_active, true) = true
                ORDER BY application_user_id
                LIMIT 1
                """, (rs, i) -> new ApplicationUserRef(
                    (UUID) rs.getObject("application_user_id"),
                    (UUID) rs.getObject("user_id")), applicationId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public UUID findUserIdByApplicationUserId(UUID applicationUserId) {
        if (applicationUserId == null) return null;
        try {
            return jdbc.queryForObject("""
                SELECT user_id
                FROM access.access_application_user
                WHERE application_user_id = ?
                LIMIT 1
                """, UUID.class, applicationUserId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public record ApplicationUserRef(UUID applicationUserId, UUID userId) {
    }
}
