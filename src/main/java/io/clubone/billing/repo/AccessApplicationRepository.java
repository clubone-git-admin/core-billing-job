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
}
