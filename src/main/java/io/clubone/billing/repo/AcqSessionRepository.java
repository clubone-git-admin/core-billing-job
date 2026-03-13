package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for acquisition.acq_session. Scoped by application_id (org/tenant).
 */
@Repository
public class AcqSessionRepository {

    private final JdbcTemplate jdbc;

    public AcqSessionRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Find all sessions for the given client_id within the application (org) scope.
     * application_id is used for tenant scope (from X-Application-Id → org_client_id).
     */
    public List<Map<String, Object>> findSessionsByClientId(UUID applicationId, UUID clientId) {
    	System.out.println("applicationId ==> " +  applicationId );

    	System.out.println("clientId ==> " +  clientId );
    	return jdbc.queryForList("""
            SELECT
                session_id,
                application_id,
                client_id,
                status_code,
                created_on,
                created_by,
                modified_on,
                modified_by,
                expires_at,
                opened_at,
                submitted_at,
                last_completed_step,
                first_name,
                last_name,
                email,
                phone,
                timezone,
                step_payload_json,
                quote_json,
                utm_json
            FROM acquisition.acq_session
            WHERE application_id = ? AND client_id = ?
            ORDER BY created_on DESC NULLS LAST
            """, applicationId, clientId);
    }

    /** Format timestamp as stored in DB (no conversion). */
    public static String toTimestampStringAsStored(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp ts) return ts.toLocalDateTime().toString();
        if (value instanceof OffsetDateTime odt) return odt.toLocalDateTime().toString();
        return value.toString();
    }
}
