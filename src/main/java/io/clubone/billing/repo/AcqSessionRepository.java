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
        return jdbc.queryForList("""
            SELECT
                s.session_id,
                s.application_id,
                s.client_id,
                s.status_code,
                s.created_on,
                s.created_by,
                s.modified_on,
                s.modified_by,
                s.expires_at,
                s.opened_at,
                s.submitted_at,
                s.last_completed_step,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
                s.timezone,
                s.step_payload_json,
                s.quote_json,
                s.utm_json,
                COALESCE(
                    (s.quote_json->>'totalDueToday')::numeric,
                    (s.quote_json->>'amount')::numeric,
                    (s.quote_json->>'totalAmount')::numeric,
                    (SELECT SUM(COALESCE(
                        (i.quote_item_json->>'totalDueToday')::numeric,
                        (i.quote_item_json->'summary'->>'totalDueToday')::numeric
                    )) FROM acquisition.acq_session_item i WHERE i.session_id = s.session_id)
                ) AS amount,
                snap.has_recurring AS has_recurring,
                snap.recurring_total_amount AS recurring_amount,
                snap.recurring_annual_amount AS recurring_annual_amount
            FROM acquisition.acq_session s
            LEFT JOIN LATERAL (
                SELECT snap_inner.has_recurring,
                       snap_inner.recurring_total_amount,
                       (SELECT SUM(
                            r.amount * CASE UPPER(COALESCE(TRIM(r.frequency_code), 'MONTHLY'))
                                WHEN 'YEARLY'   THEN 1.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'MONTHLY'  THEN 12.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'QUARTERLY' THEN 4.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'QUARTER'  THEN 4.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'WEEKLY'   THEN 52.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'BIWEEKLY' THEN 26.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                WHEN 'FORTNIGHTLY' THEN 26.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                                ELSE 12.0 / GREATEST(COALESCE(r.interval_count, 1), 1)
                            END
                        ) FROM acquisition.acq_step4_recurring_item r
                        WHERE r.snapshot_id = snap_inner.snapshot_id AND r.is_active = true
                       ) AS recurring_annual_amount
                FROM acquisition.acq_step4_snapshot snap_inner
                WHERE snap_inner.session_id = s.session_id AND snap_inner.is_active = true
                ORDER BY snap_inner.generated_at DESC NULLS LAST
                LIMIT 1
            ) snap ON true
            WHERE s.application_id = ? AND s.client_id = ?
            ORDER BY s.created_on DESC NULLS LAST
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
