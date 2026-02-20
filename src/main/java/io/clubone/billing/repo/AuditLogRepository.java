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
     */
    public List<Map<String, Object>> findAuditLogs(
            String entityType, UUID entityId, OffsetDateTime fromTs,
            OffsetDateTime toTs, Integer limit, Integer offset) {
        
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
                bal.user_agent
            FROM client_subscription_billing.billing_audit_log bal
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (entityType != null) {
            sql.append(" AND bal.entity_type = ?");
            params.add(entityType);
        }

        if (entityId != null) {
            sql.append(" AND bal.entity_id = ?::uuid");
            params.add(entityId.toString());
        }

        if (fromTs != null) {
            sql.append(" AND bal.created_on >= ?");
            params.add(fromTs);
        }

        if (toTs != null) {
            sql.append(" AND bal.created_on <= ?");
            params.add(toTs);
        }

        sql.append(" ORDER BY bal.created_on DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count audit log entries.
     */
    public Integer countAuditLogs(
            String entityType, UUID entityId, OffsetDateTime fromTs, OffsetDateTime toTs) {
        
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_audit_log bal
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (entityType != null) {
            sql.append(" AND bal.entity_type = ?");
            params.add(entityType);
        }

        if (entityId != null) {
            sql.append(" AND bal.entity_id = ?::uuid");
            params.add(entityId.toString());
        }

        if (fromTs != null) {
            sql.append(" AND bal.created_on >= ?");
            params.add(fromTs);
        }

        if (toTs != null) {
            sql.append(" AND bal.created_on <= ?");
            params.add(toTs);
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Export audit logs to CSV format.
     */
    public String exportAuditLogsCSV(
            String entityType, OffsetDateTime fromTs, OffsetDateTime toTs) {
        
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
                bal.ip_address
            FROM client_subscription_billing.billing_audit_log bal
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (entityType != null) {
            sql.append(" AND bal.entity_type = ?");
            params.add(entityType);
        }

        if (fromTs != null) {
            sql.append(" AND bal.created_on >= ?");
            params.add(fromTs);
        }

        if (toTs != null) {
            sql.append(" AND bal.created_on <= ?");
            params.add(toTs);
        }

        sql.append(" ORDER BY bal.created_on DESC");

        List<Map<String, Object>> logs = jdbc.queryForList(sql.toString(), params.toArray());

        // Build CSV
        StringBuilder csv = new StringBuilder();
        csv.append("audit_log_id,event_type,entity_type,entity_id,action,user_id,user_email,created_on,ip_address\n");

        for (Map<String, Object> log : logs) {
            csv.append(formatCSVValue(log.get("audit_log_id"))).append(",");
            csv.append(formatCSVValue(log.get("event_type"))).append(",");
            csv.append(formatCSVValue(log.get("entity_type"))).append(",");
            csv.append(formatCSVValue(log.get("entity_id"))).append(",");
            csv.append(formatCSVValue(log.get("action"))).append(",");
            csv.append(formatCSVValue(log.get("user_id"))).append(",");
            csv.append(formatCSVValue(log.get("user_email"))).append(",");
            csv.append(formatCSVValue(log.get("created_on"))).append(",");
            csv.append(formatCSVValue(log.get("ip_address"))).append("\n");
        }

        return csv.toString();
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
}
