package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for audit log operations.
 */
@Repository
public class AuditLogRepository {

    private final JdbcTemplate jdbc;

    public AuditLogRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert an audit log entry.
     */
    public void insertAuditLog(String eventType, String entityType, UUID entityId,
                               String action, String userId, Map<String, Object> details) {
        String detailsJson = null;
        if (details != null && !details.isEmpty()) {
            try {
                detailsJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(details);
            } catch (Exception e) {
                // ignore
            }
        }
        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_audit_log
            (event_type, entity_type, entity_id, action, user_id, details, created_on)
            VALUES (?, ?, ?::uuid, ?, ?, ?::jsonb, now())
            """,
                eventType,
                entityType,
                entityId != null ? entityId.toString() : null,
                action,
                userId,
                detailsJson);
    }

    /**
     * Find audit log entries with filtering.
     *
     * <p>{@code billingRunId} matches BILLING_RUN entity rows, STAGE_RUN rows for that billing run,
     * and any row whose details JSON carries {@code billing_run_id} (covers legacy / cross-entity traces).
     */
    public List<Map<String, Object>> findAuditLogs(
            String entityType,
            UUID entityId,
            UUID billingRunId,
            String eventType,
            List<UUID> locationIds,
            OffsetDateTime fromTs,
            OffsetDateTime toTs,
            Integer limit,
            Integer offset) {

        StringBuilder sql = new StringBuilder("""
            SELECT
                bal.audit_log_id,
                bal.event_type,
                bal.entity_type,
                bal.entity_id,
                bal.action,
                bal.user_id,
                bal.user_email,
                bal.details,
                bal.created_on,
                bal.ip_address,
                bal.user_agent,
                br.billing_run_code AS billing_run_code,
                bsr.stage_run_code AS stage_run_code,
                bsc.stage_code AS stage_code,
                COALESCE(
                    NULLIF(TRIM(bal.user_email), ''),
                    NULLIF(TRIM(CONCAT_WS(' ', actor_u.first_name, actor_u.last_name)), ''),
                    NULLIF(TRIM(CONCAT_WS(' ', actor_app_u.first_name, actor_app_u.last_name)), ''),
                    NULLIF(TRIM(actor_u.email), ''),
                    NULLIF(TRIM(actor_app_u.email), '')
                ) AS resolved_user_label
            FROM client_subscription_billing.billing_audit_log bal
            """);
        sql.append(resolvedBillingRunJoin());
        sql.append(resolvedActorJoin());
        sql.append(" WHERE 1=1 ");

        List<Object> params = new ArrayList<>();
        appendFilters(sql, params, entityType, entityId, billingRunId, eventType, locationIds, fromTs, toTs);

        sql.append(" ORDER BY bal.created_on DESC LIMIT ? OFFSET ?");
        params.add(limit != null ? limit : 100);
        params.add(offset != null ? offset : 0);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count audit log entries.
     */
    public Integer countAuditLogs(
            String entityType,
            UUID entityId,
            UUID billingRunId,
            String eventType,
            List<UUID> locationIds,
            OffsetDateTime fromTs,
            OffsetDateTime toTs) {

        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_audit_log bal
            """);
        sql.append(resolvedBillingRunJoin());
        sql.append(" WHERE 1=1 ");

        List<Object> params = new ArrayList<>();
        appendFilters(sql, params, entityType, entityId, billingRunId, eventType, locationIds, fromTs, toTs);

        Integer count = jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
        return count != null ? count : 0;
    }

    /**
     * Export audit logs to CSV format.
     */
    public String exportAuditLogsCSV(
            String entityType,
            UUID billingRunId,
            String eventType,
            List<UUID> locationIds,
            OffsetDateTime fromTs,
            OffsetDateTime toTs) {

        StringBuilder sql = new StringBuilder("""
            SELECT
                bal.audit_log_id,
                bal.event_type,
                bal.entity_type,
                bal.entity_id,
                bal.action,
                bal.user_id,
                bal.user_email,
                bal.created_on,
                bal.ip_address,
                br.billing_run_code AS billing_run_code,
                COALESCE(
                    NULLIF(TRIM(bal.user_email), ''),
                    NULLIF(TRIM(CONCAT_WS(' ', actor_u.first_name, actor_u.last_name)), ''),
                    NULLIF(TRIM(CONCAT_WS(' ', actor_app_u.first_name, actor_app_u.last_name)), ''),
                    NULLIF(TRIM(actor_u.email), ''),
                    NULLIF(TRIM(actor_app_u.email), '')
                ) AS resolved_user_label
            FROM client_subscription_billing.billing_audit_log bal
            """);
        sql.append(resolvedBillingRunJoin());
        sql.append(resolvedActorJoin());
        sql.append(" WHERE 1=1 ");

        List<Object> params = new ArrayList<>();
        appendFilters(sql, params, entityType, null, billingRunId, eventType, locationIds, fromTs, toTs);

        sql.append(" ORDER BY bal.created_on DESC");

        List<Map<String, Object>> logs = jdbc.queryForList(sql.toString(), params.toArray());

        StringBuilder csv = new StringBuilder();
        csv.append("audit_log_id,event_type,entity_type,entity_id,action,user_id,user_label,billing_run_code,created_on,ip_address\n");

        for (Map<String, Object> log : logs) {
            String userLabel = firstNonBlank(
                    stringOrNull(log.get("resolved_user_label")),
                    stringOrNull(log.get("user_email")),
                    "system".equalsIgnoreCase(stringOrNull(log.get("user_id"))) ? "System" : null,
                    stringOrNull(log.get("user_id")));
            csv.append(formatCSVValue(log.get("audit_log_id"))).append(",");
            csv.append(formatCSVValue(log.get("event_type"))).append(",");
            csv.append(formatCSVValue(log.get("entity_type"))).append(",");
            csv.append(formatCSVValue(log.get("entity_id"))).append(",");
            csv.append(formatCSVValue(log.get("action"))).append(",");
            csv.append(formatCSVValue(log.get("user_id"))).append(",");
            csv.append(formatCSVValue(userLabel)).append(",");
            csv.append(formatCSVValue(log.get("billing_run_code"))).append(",");
            csv.append(formatCSVValue(log.get("created_on"))).append(",");
            csv.append(formatCSVValue(log.get("ip_address"))).append("\n");
        }

        return csv.toString();
    }

    /**
     * Resolve the owning billing_run for both BILLING_RUN and STAGE_RUN audit rows so location
     * filters do not drop stage-scoped events (entity_id is a stage_run_id in that case).
     */
    private static String resolvedBillingRunJoin() {
        return """
            LEFT JOIN client_subscription_billing.billing_stage_run bsr
              ON UPPER(COALESCE(bal.entity_type, '')) = 'STAGE_RUN'
             AND bal.entity_id IS NOT NULL
             AND bal.entity_id::text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             AND bsr.stage_run_id = bal.entity_id::uuid
            LEFT JOIN billing_config.billing_stage_code bsc
              ON bsc.billing_stage_code_id = bsr.stage_code_id
            LEFT JOIN client_subscription_billing.billing_run br
              ON br.billing_run_id = COALESCE(
                    bsr.billing_run_id,
                    CASE
                      WHEN UPPER(COALESCE(bal.entity_type, '')) = 'BILLING_RUN'
                       AND bal.entity_id IS NOT NULL
                       AND bal.entity_id::text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN bal.entity_id::uuid
                      WHEN bal.details->>'billing_run_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN (bal.details->>'billing_run_id')::uuid
                      WHEN bal.details->>'billingRunId' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        THEN (bal.details->>'billingRunId')::uuid
                      ELSE NULL
                    END
                 )
            """;
    }

    /**
     * Resolve actor display label when {@code user_id} stores an access user / application-user UUID.
     * Compare via text — never cast {@code bal.user_id} to uuid (job rows store {@code "system"}).
     * Postgres does not guarantee AND short-circuit, so {@code ... ~* uuid AND col = user_id::uuid}
     * still throws on non-UUID values like {@code "system"}.
     */
    private static String resolvedActorJoin() {
        return """
            LEFT JOIN access.access_user actor_u
              ON bal.user_id IS NOT NULL
             AND bal.user_id <> ''
             AND actor_u.user_id::text = bal.user_id
            LEFT JOIN access.access_application_user actor_aau
              ON bal.user_id IS NOT NULL
             AND bal.user_id <> ''
             AND actor_aau.application_user_id::text = bal.user_id
            LEFT JOIN access.access_user actor_app_u
              ON actor_app_u.user_id = actor_aau.user_id
            """;
    }

    private static String stringOrNull(Object v) {
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v.trim();
            }
        }
        return null;
    }

    private void appendFilters(
            StringBuilder sql,
            List<Object> params,
            String entityType,
            UUID entityId,
            UUID billingRunId,
            String eventType,
            List<UUID> locationIds,
            OffsetDateTime fromTs,
            OffsetDateTime toTs) {

        if (entityType != null && !entityType.isBlank()) {
            sql.append(" AND UPPER(bal.entity_type) = UPPER(?)");
            params.add(entityType.trim());
        }

        if (entityId != null) {
            sql.append(" AND bal.entity_id = ?::uuid");
            params.add(entityId.toString());
        }

        if (billingRunId != null) {
            // Trace everything for a bill run: direct BILLING_RUN rows, all STAGE_RUN rows for the
            // run (including regenerate history), and any row that stamped billing_run_id in details.
            sql.append("""
                 AND (
                      br.billing_run_id = ?::uuid
                   OR (
                        UPPER(COALESCE(bal.entity_type, '')) = 'BILLING_RUN'
                    AND bal.entity_id = ?::uuid
                   )
                   OR (
                        UPPER(COALESCE(bal.entity_type, '')) = 'STAGE_RUN'
                    AND bal.entity_id IS NOT NULL
                    AND bal.entity_id::text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    AND EXISTS (
                          SELECT 1
                            FROM client_subscription_billing.billing_stage_run s
                           WHERE s.stage_run_id = bal.entity_id::uuid
                             AND s.billing_run_id = ?::uuid
                        )
                   )
                   OR (
                        bal.details->>'billing_run_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    AND (bal.details->>'billing_run_id')::uuid = ?::uuid
                   )
                   OR (
                        bal.details->>'billingRunId' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    AND (bal.details->>'billingRunId')::uuid = ?::uuid
                   )
                 )
                """);
            String id = billingRunId.toString();
            params.add(id);
            params.add(id);
            params.add(id);
            params.add(id);
            params.add(id);
        }

        if (eventType != null && !eventType.isBlank()) {
            sql.append(" AND UPPER(bal.event_type) = UPPER(?)");
            params.add(eventType.trim());
        }

        if (fromTs != null) {
            sql.append(" AND bal.created_on >= ?");
            params.add(fromTs);
        }

        if (toTs != null) {
            sql.append(" AND bal.created_on <= ?");
            params.add(toTs);
        }

        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql.append(" AND (")
                    .append("br.location_id IN (").append(in).append(") ")
                    .append("OR EXISTS (SELECT 1 FROM client_subscription_billing.billing_run_location j ")
                    .append("WHERE j.billing_run_id = br.billing_run_id ")
                    .append("AND j.location_id IN (").append(in).append("))) ");
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    params.add(u.toString());
                }
            }
        }
    }

    private String formatCSVValue(Object value) {
        if (value == null) {
            return "";
        }
        String str = value.toString();
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    private String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }
}
