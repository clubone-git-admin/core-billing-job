package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Enterprise reconciliation schedules ({@code reconciliation.reco_schedule}).
 */
@Repository
public class ReconciliationEnterpriseScheduleRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationEnterpriseScheduleRepository(
            @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper
    ) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    private static UUID parseOptionalUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String str(Map<?, ?> m, String key) {
        Object v = m.get(key);
        return v == null ? null : String.valueOf(v).trim();
    }

    private static boolean bool(Map<?, ?> m, String key, boolean def) {
        Object v = m.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Boolean b) {
            return b;
        }
        return Boolean.parseBoolean(String.valueOf(v));
    }

    private static Integer intVal(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(v).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize JSON", e);
        }
    }

    private void writeAudit(UUID tenantId, UUID actorId, String action, String entityRef, Object details) {
        try {
            Map<String, Object> clean = new HashMap<>();
            if (details instanceof Map<?, ?> dm) {
                for (Map.Entry<?, ?> e : dm.entrySet()) {
                    if (e.getValue() != null) {
                        clean.put(String.valueOf(e.getKey()), e.getValue());
                    }
                }
            }
            jdbc.update("""
                    INSERT INTO reconciliation.audit_log (application_id, actor_user_id, action_code, entity_type, entity_reference_code, details_json)
                    VALUES (?, ?, ?, 'reco_schedule', ?, ?::jsonb)
                    """, tenantId, actorId, action, entityRef, toJson(clean));
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private UUID queryUuidOrNull(String sql, Object... args) {
        try {
            return jdbc.queryForObject(sql, UUID.class, args);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private UUID domainIdByCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull("SELECT reco_domain_id FROM reconciliation.lu_reco_domain WHERE code = ? LIMIT 1", code.trim());
    }

    private UUID stageIdByCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull("SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = ? LIMIT 1", code.trim());
    }

    private UUID scheduleFrequencyIdByCode(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("frequency is required");
        }
        UUID id = queryUuidOrNull("SELECT schedule_frequency_id FROM reconciliation.lu_schedule_frequency WHERE UPPER(code) = UPPER(?) LIMIT 1", code.trim());
        if (id == null) {
            throw new IllegalArgumentException("Unknown frequency: " + code);
        }
        return id;
    }

    private static String normalizeView(String view) {
        if (view == null || view.isBlank()) {
            return "active";
        }
        return switch (view.trim().toLowerCase()) {
            case "active", "calendar", "queue", "failed", "retry-policies", "audit" -> view.trim().toLowerCase();
            default -> "active";
        };
    }

    private String scheduleBaseFrom() {
        return """
                FROM reconciliation.reco_schedule s
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = s.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = s.reconciliation_stage_id
                JOIN reconciliation.lu_schedule_frequency sf ON sf.schedule_frequency_id = s.schedule_frequency_id
                LEFT JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = s.payment_gateway_id
                LEFT JOIN LATERAL (
                    SELECT rr.created_on AS last_run_at, rs.code AS last_run_status
                    FROM reconciliation.reconciliation_run rr
                    JOIN reconciliation.lu_reconciliation_run_status rs ON rs.reconciliation_run_status_id = rr.reconciliation_run_status_id
                    WHERE rr.application_id IS NOT DISTINCT FROM s.application_id
                      AND rr.filters_json->>'scheduleId' = CAST(s.reco_schedule_id AS text)
                    ORDER BY rr.created_on DESC
                    LIMIT 1
                ) lr ON true
                """;
    }

    private void appendScheduleFilters(
            StringBuilder where,
            List<Object> args,
            Boolean activeOnly,
            String search,
            String recoDomain,
            String stage,
            String level,
            String gatewayCode,
            String frequency,
            String status
    ) {
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND s.is_active = true ");
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append("""
                     AND (
                        s.schedule_name ILIKE ? ESCAPE '\\' OR s.schedule_code ILIKE ? ESCAPE '\\'
                        OR COALESCE(d.code,'') ILIKE ? ESCAPE '\\' OR COALESCE(stg.code,'') ILIKE ? ESCAPE '\\'
                        OR COALESCE(pg.name,'') ILIKE ? ESCAPE '\\' OR COALESCE(s.execution_json->>'gatewayCode','') ILIKE ? ESCAPE '\\'
                     )
                    """);
            for (int i = 0; i < 6; i++) {
                args.add(q);
            }
        }
        if (recoDomain != null && !recoDomain.isBlank()) {
            where.append(" AND UPPER(COALESCE(d.code,'')) = UPPER(?) ");
            args.add(recoDomain.trim());
        }
        if (stage != null && !stage.isBlank()) {
            where.append(" AND UPPER(COALESCE(stg.code,'')) = UPPER(?) ");
            args.add(stage.trim());
        }
        if (frequency != null && !frequency.isBlank()) {
            where.append(" AND UPPER(sf.code) = UPPER(?) ");
            args.add(frequency.trim());
        }
        if (level != null && !level.isBlank()) {
            UUID lvl = parseOptionalUuid(level);
            if (lvl != null) {
                where.append(" AND (s.level_id = ? OR s.execution_json->>'locationId' = ? OR s.execution_json->>'locationLevelId' = ?) ");
                args.add(lvl);
                args.add(lvl.toString());
                args.add(lvl.toString());
            }
        }
        if (gatewayCode != null && !gatewayCode.isBlank()) {
            where.append("""
                     AND (
                        UPPER(COALESCE(pg.name,'')) = UPPER(?)
                        OR UPPER(COALESCE(s.execution_json->>'gatewayCode','')) = UPPER(?)
                     )
                    """);
            args.add(gatewayCode.trim());
            args.add(gatewayCode.trim());
        }
        if (status != null && !status.isBlank()) {
            String s = status.trim().toUpperCase();
            if ("ACTIVE".equals(s)) {
                where.append(" AND s.is_active = true ");
            } else if ("PAUSED".equals(s) || "INACTIVE".equals(s) || "DRAFT".equals(s)) {
                where.append(" AND s.is_active = false ");
            }
        }
    }

    private Map<String, Object> scheduleKpis(UUID tenantUuid) {
        Map<String, Object> k = new HashMap<>();
        if (tenantUuid == null) {
            k.put("activeSchedules", 0L);
            k.put("pausedSchedules", 0L);
            k.put("runsToday", 0L);
            k.put("failedRuns", 0L);
            k.put("queuedRuns", 0L);
            k.put("averageRuntimeMinutes", 0L);
            return k;
        }
        k.put("activeSchedules", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_schedule s
                WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? AND s.is_active = true
                """, tenantUuid));
        k.put("pausedSchedules", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_schedule s
                WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? AND s.is_active = false
                """, tenantUuid));
        k.put("runsToday", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reconciliation_run rr
                WHERE rr.application_id IS NOT DISTINCT FROM ? AND rr.created_on::date = CURRENT_DATE
                """, tenantUuid));
        k.put("failedRuns", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                WHERE rr.application_id IS NOT DISTINCT FROM ? AND st.code = 'FAILED'
                """, tenantUuid));
        k.put("queuedRuns", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                WHERE rr.application_id IS NOT DISTINCT FROM ? AND st.code IN ('QUEUED','RUNNING')
                """, tenantUuid));
        Double avg = jdbc.queryForObject("""
                SELECT AVG(EXTRACT(EPOCH FROM (COALESCE(rr.ended_on, now()) - rr.started_on)) / 60.0)
                FROM reconciliation.reconciliation_run rr
                WHERE rr.application_id IS NOT DISTINCT FROM ? AND rr.started_on IS NOT NULL AND rr.created_on::date = CURRENT_DATE
                """, Double.class, tenantUuid);
        k.put("averageRuntimeMinutes", avg == null ? 0L : Math.round(avg));
        return k;
    }

    private long countOrZero(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0L : n;
    }

    private List<Map<String, Object>> scheduleConflictWarnings(UUID tenantUuid) {
        if (tenantUuid == null) {
            return List.of();
        }
        List<Map<String, Object>> dupes = jdbc.queryForList("""
                SELECT d.code AS "recoDomain", stg.code AS "stage",
                       COALESCE(pg.name, s.execution_json->>'gatewayCode') AS "gatewayCode",
                       string_agg(CAST(s.reco_schedule_id AS text), ',' ORDER BY s.schedule_code) AS "ids"
                FROM reconciliation.reco_schedule s
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = s.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = s.reconciliation_stage_id
                LEFT JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = s.payment_gateway_id
                WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? AND s.is_active = true
                GROUP BY s.reco_domain_id, s.reconciliation_stage_id, d.code, stg.code, pg.name, s.execution_json->>'gatewayCode'
                HAVING COUNT(*) > 1
                """, tenantUuid);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> row : dupes) {
            String ids = Objects.toString(row.get("ids"), "");
            String[] parts = ids.split(",", 5);
            Map<String, Object> w = new HashMap<>();
            w.put("message", "Multiple active schedules overlap for "
                    + row.get("recoDomain") + " / " + row.get("stage") + " / " + row.get("gatewayCode") + ".");
            w.put("scheduleIds", List.of(parts));
            w.put("winnerScheduleId", parts.length > 0 ? parts[0] : null);
            w.put("reason", "FIRST_BY_CODE_ORDER");
            out.add(w);
        }
        return out;
    }

    private Map<String, Object> wrapList(List<Map<String, Object>> items, long total, int page, int pageSize, UUID tenantUuid) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("total", total);
        meta.put("totalRecords", total);
        meta.put("page", page);
        meta.put("pageSize", pageSize);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) Math.max(1, pageSize))));
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        out.put("kpis", scheduleKpis(tenantUuid));
        out.put("conflictWarnings", scheduleConflictWarnings(tenantUuid));
        return out;
    }

    public Map<String, Object> listEnterpriseSchedules(
            String tenantId,
            String viewRaw,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String recoDomain,
            String stage,
            String level,
            String gatewayCode,
            String currencyCode,
            String paymentMethodCode,
            String sourceSystem,
            String frequency,
            String status,
            String from,
            String to,
            String locationId
    ) {
        String view = normalizeView(viewRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;

        return switch (view) {
            case "calendar" -> listScheduleCalendar(tenantUuid, activeOnly, search, p, ps, offset, recoDomain, stage, level, gatewayCode, frequency, status, from, to);
            case "queue" -> listScheduleQueue(tenantUuid, p, ps, offset);
            case "failed" -> listScheduleFailed(tenantUuid, activeOnly, search, p, ps, offset, recoDomain, stage, level, gatewayCode, frequency, status);
            case "retry-policies" -> listScheduleRetryPolicies(tenantUuid, activeOnly, search, p, ps, offset, recoDomain, stage, level, gatewayCode, frequency, status);
            case "audit" -> listScheduleAudit(tenantUuid, p, ps, offset, search);
            default -> listScheduleActive(tenantUuid, activeOnly, search, p, ps, offset, recoDomain, stage, level, gatewayCode, frequency, status, locationId);
        };
    }

    private Map<String, Object> listScheduleActive(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String recoDomain, String stage, String level, String gatewayCode, String frequency, String status, String locationId
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        appendScheduleFilters(where, args, activeOnly, search, recoDomain, stage, level, gatewayCode, frequency, status);
        UUID loc = parseOptionalUuid(locationId);
        if (loc != null) {
            where.append("""
                     AND (
                        s.level_id = ? OR s.execution_json->>'locationId' = ? OR s.execution_json->>'locationLevelId' = ?
                     )
                    """);
            args.add(loc);
            args.add(loc.toString());
            args.add(loc.toString());
        }
        String fromSql = scheduleBaseFrom() + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    CAST(s.reco_schedule_id AS text) AS "scheduleId",
                    CAST(s.reco_schedule_id AS text) AS "id",
                    s.schedule_name AS "scheduleName",
                    s.schedule_code AS "scheduleCode",
                    d.code AS "recoDomain",
                    stg.code AS "stage",
                    sf.code AS "frequency",
                    CAST(s.level_id AS text) AS "locationLevelId",
                    COALESCE(pg.name, s.execution_json->>'gatewayCode') AS "gatewayCode",
                    CAST(NULL AS timestamptz) AS "nextRunAt",
                    lr.last_run_at AS "lastRunAt",
                    lr.last_run_status AS "lastRunStatus",
                    CASE WHEN s.is_active THEN 'ACTIVE' ELSE 'PAUSED' END AS "status",
                    s.is_active AS "active",
                    s.timezone AS "timezone",
                    s.cron_expression AS "cronExpression",
                    s.retry_on_failure AS "retryOnFailure",
                    s.retry_count AS "retryCount"
                """ + fromSql + """
                ORDER BY s.schedule_code
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid);
    }

    private Map<String, Object> listScheduleCalendar(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String recoDomain, String stage, String level, String gatewayCode, String frequency, String status,
            String from, String to
    ) {
        Map<String, Object> active = listScheduleActive(tenantUuid, activeOnly, search, 1, 500, 0,
                recoDomain, stage, level, gatewayCode, frequency, status, null);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> schedules = (List<Map<String, Object>>) active.get("items");
        List<Map<String, Object>> events = new ArrayList<>();
        int i = 0;
        for (Map<String, Object> sch : schedules) {
            String sid = Objects.toString(sch.get("scheduleId"), "");
            String start = sch.get("nextRunAt") != null ? Objects.toString(sch.get("nextRunAt"))
                    : (sch.get("lastRunAt") != null ? Objects.toString(sch.get("lastRunAt")) : null);
            if (start == null) {
                continue;
            }
            Map<String, Object> ev = new HashMap<>();
            ev.put("eventId", "evt-" + sid + "-" + (++i));
            ev.put("scheduleId", sid);
            ev.put("scheduleName", sch.get("scheduleName"));
            ev.put("startAt", start);
            ev.put("endAt", start);
            String runSt = Objects.toString(sch.get("lastRunStatus"), "SCHEDULED");
            ev.put("eventType", "SCHEDULED");
            ev.put("status", runSt);
            ev.put("color", calendarColor(runSt, boolFromMap(sch, "active", true)));
            events.add(ev);
        }
        long total = events.size();
        int fromIdx = Math.min(offset, events.size());
        int toIdx = Math.min(fromIdx + ps, events.size());
        return wrapList(events.subList(fromIdx, toIdx), total, p, ps, tenantUuid);
    }

    private static String calendarColor(String runStatus, boolean active) {
        if (!active) {
            return "gray";
        }
        if (runStatus == null) {
            return "blue";
        }
        return switch (runStatus.toUpperCase()) {
            case "COMPLETED", "SUCCESS" -> "green";
            case "RUNNING" -> "yellow";
            case "FAILED" -> "red";
            default -> "blue";
        };
    }

    private static boolean boolFromMap(Map<String, Object> m, String key, boolean def) {
        Object v = m.get(key);
        if (v instanceof Boolean b) {
            return b;
        }
        return def;
    }

    private Map<String, Object> listScheduleQueue(UUID tenantUuid, int p, int ps, int offset) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        String where = """
                 WHERE rr.application_id IS NOT DISTINCT FROM ?
                   AND st.code IN ('QUEUED','RUNNING')
                """;
        String fromSql = """
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    rr.run_reference_code AS "runReference",
                    rr.filters_json->>'scheduleId' AS "scheduleId",
                    COALESCE(s.schedule_name, rr.run_type) AS "scheduleName",
                    rr.filters_json->>'stage' AS "stage",
                    rr.created_on AS "queuedAt",
                    st.code AS "status",
                    COALESCE(rr.filters_json->>'priority', 'MEDIUM') AS "priority",
                    COALESCE((rr.filters_json->>'retryCount')::int, 0) AS "retryCount"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                LEFT JOIN reconciliation.reco_schedule s ON s.reco_schedule_id::text = rr.filters_json->>'scheduleId'
                """ + where + """
                ORDER BY rr.created_on DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid);
    }

    private Map<String, Object> listScheduleFailed(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String recoDomain, String stage, String level, String gatewayCode, String frequency, String status
    ) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        String fromSql = """
                FROM reconciliation.reco_schedule s
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = s.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = s.reconciliation_stage_id
                JOIN reconciliation.lu_schedule_frequency sf ON sf.schedule_frequency_id = s.schedule_frequency_id
                LEFT JOIN LATERAL (
                    SELECT rr.created_on AS fail_at, rr.summary_json->>'failureReason' AS reason, st.code AS retry_st
                    FROM reconciliation.reconciliation_run rr
                    JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                    WHERE rr.filters_json->>'scheduleId' = CAST(s.reco_schedule_id AS text)
                      AND st.code = 'FAILED'
                    ORDER BY rr.created_on DESC LIMIT 1
                ) fr ON true
                WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? AND fr.fail_at IS NOT NULL
                """;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(s.reco_schedule_id AS text) AS "scheduleId",
                       s.schedule_name AS "scheduleName",
                       fr.fail_at AS "lastFailureAt",
                       fr.reason AS "failureReason",
                       fr.retry_st AS "retryStatus",
                       false AS "escalated"
                """ + fromSql + " ORDER BY fr.fail_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid);
    }

    private Map<String, Object> listScheduleRetryPolicies(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String recoDomain, String stage, String level, String gatewayCode, String frequency, String status
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? AND s.retry_on_failure = true ");
        args.add(tenantUuid);
        appendScheduleFilters(where, args, activeOnly, search, recoDomain, stage, level, gatewayCode, frequency, status);
        String fromSql = scheduleBaseFrom() + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(s.reco_schedule_id AS text) AS "scheduleId",
                       s.schedule_name AS "scheduleName",
                       s.schedule_code AS "scheduleCode",
                       s.retry_count AS "retryCount",
                       s.retry_on_failure AS "retryOnFailure",
                       d.code AS "recoDomain",
                       stg.code AS "stage"
                """ + fromSql + " ORDER BY s.schedule_code LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid);
    }

    private Map<String, Object> listScheduleAudit(UUID tenantUuid, int p, int ps, int offset, String search) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE a.entity_type = 'reco_schedule' AND a.application_id IS NOT DISTINCT FROM ? ");
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (a.action_code ILIKE ? ESCAPE '\\' OR COALESCE(a.entity_reference_code,'') ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        String fromSql = " FROM reconciliation.audit_log a " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(a.audit_log_id AS text) AS "auditEventId",
                       a.entity_reference_code AS "scheduleId",
                       COALESCE(s.schedule_name, a.entity_reference_code) AS "scheduleName",
                       a.action_code AS "action",
                       CAST(a.actor_user_id AS text) AS "actorId",
                       a.event_at AS "occurredAt",
                       COALESCE(a.details_json->>'summary', a.action_code) AS "summary"
                FROM reconciliation.audit_log a
                LEFT JOIN reconciliation.reco_schedule s ON CAST(s.reco_schedule_id AS text) = a.entity_reference_code
                """ + where + """
                ORDER BY a.event_at DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid);
    }

    public Map<String, Object> getEnterpriseScheduleDetail(String tenantId, String scheduleId) {
        UUID sid = parseOptionalUuid(scheduleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (sid == null) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    CAST(s.reco_schedule_id AS text) AS "scheduleId",
                    s.schedule_name AS "scheduleName",
                    s.schedule_code AS "scheduleCode",
                    d.code AS "recoDomain",
                    stg.code AS "stage",
                    sf.code AS "frequency",
                    s.interval_n AS "interval",
                    s.cron_expression AS "cronExpression",
                    s.timezone AS "timezone",
                    CAST(s.level_id AS text) AS "locationLevelId",
                    NULLIF(TRIM(s.execution_json->>'locationId'), '') AS "locationId",
                    COALESCE(pg.name, s.execution_json->>'gatewayCode') AS "gatewayCode",
                    s.execution_json AS "executionJson",
                    s.allow_parallel_runs AS "allowParallelRuns",
                    s.max_runtime_minutes AS "maxRuntimeMinutes",
                    s.retry_on_failure AS "retryOnFailure",
                    s.retry_count AS "retryCount",
                    s.notify_on_failure AS "notifyOnFailure",
                    s.notify_on_completion AS "notifyOnCompletion",
                    s.notification_channels_json AS "notificationChannelsJson",
                    s.is_active AS "active",
                    CASE WHEN s.is_active THEN 'ACTIVE' ELSE 'PAUSED' END AS "status"
                FROM reconciliation.reco_schedule s
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = s.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = s.reconciliation_stage_id
                JOIN reconciliation.lu_schedule_frequency sf ON sf.schedule_frequency_id = s.schedule_frequency_id
                LEFT JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = s.payment_gateway_id
                WHERE s.reco_schedule_id = ? AND s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ?
                """, sid, tenantUuid);
        if (rows.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        return Map.of("status", "OK", "row", rows.get(0));
    }

    public Map<String, Object> buildScheduleFiltersForRun(String scheduleId, String tenantId) {
        Map<String, Object> detail = getEnterpriseScheduleDetail(tenantId, scheduleId);
        if (!"OK".equals(detail.get("status"))) {
            return Map.of();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) detail.get("row");
        Map<String, Object> filters = new LinkedHashMap<>();
        filters.put("scheduleId", scheduleId);
        if (row.get("recoDomain") != null) {
            filters.put("recoDomain", row.get("recoDomain"));
        }
        if (row.get("stage") != null) {
            filters.put("stage", row.get("stage"));
        }
        if (row.get("locationId") != null) {
            filters.put("locationIds", List.of(row.get("locationId")));
        }
        return filters;
    }

    public Map<String, Object> previewSchedule(String tenantId, Map<String, Object> body) {
        String scheduleId = body == null ? null : str(body, "scheduleId");
        Map<String, Object> row;
        if (scheduleId != null) {
            Map<String, Object> d = getEnterpriseScheduleDetail(tenantId, scheduleId);
            if (!"OK".equals(d.get("status"))) {
                return Map.of("warnings", List.of(Map.of("severity", "ERROR", "code", "NOT_FOUND", "message", "Schedule not found")));
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> r = (Map<String, Object>) d.get("row");
            row = r;
        } else if (body != null && body.get("row") instanceof Map<?, ?> rm) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cast = (Map<String, Object>) rm;
            row = cast;
        } else {
            row = Map.of();
        }
        Map<String, Object> out = new HashMap<>();
        out.put("nextRuns", List.of());
        out.put("estimatedRecordCount", null);
        out.put("estimatedRuntimeMinutes", row.get("maxRuntimeMinutes"));
        out.put("expectedSourceSystems", List.of());
        List<Map<String, Object>> warnings = new ArrayList<>();
        warnings.addAll(scheduleConflictWarnings(parseOptionalUuid(tenantId)));
        out.put("warnings", warnings);
        out.put("conflictWarnings", warnings);
        return out;
    }

    public List<Map<String, Object>> listScheduleRuns(String scheduleId, int page, int pageSize) {
        UUID sid = parseOptionalUuid(scheduleId);
        if (sid == null) {
            return List.of();
        }
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (Math.max(1, page) - 1) * ps;
        return jdbc.queryForList("""
                SELECT rr.run_reference_code AS "reconciliationRunId",
                       st.code AS "status",
                       rr.created_on AS "createdAt",
                       rr.started_on AS "startedOn",
                       rr.ended_on AS "endedOn"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                WHERE rr.filters_json->>'scheduleId' = ?
                ORDER BY rr.created_on DESC
                LIMIT ? OFFSET ?
                """, sid.toString(), ps, offset);
    }

    // --- mutations (delegate patterns from enterprise config repo) ---

    public Map<String, Object> upsertSchedule(String tenantId, Map<String, Object> m, String idOrNull, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID parsedId = parseOptionalUuid(idOrNull);
        boolean isUpdate = parsedId != null;
        UUID id = isUpdate ? parsedId : UUID.randomUUID();
        String name = str(m, "scheduleName");
        if (name == null || name.isBlank()) {
            name = str(m, "policyName");
        }
        name = Objects.requireNonNull(name, "scheduleName is required");
        String code = str(m, "scheduleCode");
        if (code != null && code.isBlank()) {
            code = null;
        }
        UUID freqId = scheduleFrequencyIdByCode(str(m, "frequency"));
        UUID domainId = domainIdByCode(str(m, "recoDomain"));
        UUID stageId = stageIdByCode(str(m, "stage"));
        int interval = intVal(m, "interval") == null ? 1 : intVal(m, "interval");
        Map<String, Object> exec = new LinkedHashMap<>();
        Object ej = m.get("executionJson");
        if (ej instanceof Map<?, ?> mm) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cast = (Map<String, Object>) mm;
            exec.putAll(cast);
        }
        mergeExecutionField(exec, m, "locationId");
        mergeExecutionField(exec, m, "locationLevelId");
        mergeExecutionField(exec, m, "gatewayCode");
        mergeExecutionField(exec, m, "sourceSystem");
        mergeExecutionField(exec, m, "currencyCode");
        mergeExecutionField(exec, m, "paymentMethodCode");
        mergeExecutionField(exec, m, "lineOfBusiness");
        mergeExecutionField(exec, m, "financialPeriodScope");
        mergeExecutionField(exec, m, "description");
        mergeExecutionField(exec, m, "executionWindowStart");
        mergeExecutionField(exec, m, "executionWindowEnd");
        if (m.containsKey("runIntervalMinutes")) {
            exec.put("runIntervalMinutes", m.get("runIntervalMinutes"));
        }
        if (m.containsKey("blackoutWindows")) {
            exec.put("blackoutWindows", m.get("blackoutWindows"));
        }
        exec.put("includeChildLocations", bool(m, "includeChildLocations", true));
        exec.put("allowWeekendRuns", bool(m, "allowWeekendRuns", true));
        exec.put("allowHolidayRuns", bool(m, "allowHolidayRuns", true));
        String execJson = toJson(exec);
        UUID levelId = parseOptionalUuid(str(m, "locationLevelId"));
        if (levelId == null) {
            levelId = parseOptionalUuid(str(m, "level"));
        }
        UUID paymentGatewayId = parseOptionalUuid(str(m, "paymentGatewayId"));
        Object channels = m.get("notificationChannels");
        if (channels == null) {
            channels = m.get("notificationChannelsJson");
        }
        if (channels == null) {
            channels = List.of();
        }
        boolean notifyFailure = bool(m, "notifyOnFailure", true);
        if (m.containsKey("notifyOnSuccess")) {
            notifyFailure = bool(m, "notifyOnFailure", notifyFailure);
        }
        boolean notifyCompletion = bool(m, "notifyOnCompletion", false);
        if (m.containsKey("notifyOnSuccess")) {
            notifyCompletion = bool(m, "notifyOnSuccess", notifyCompletion);
        }
        if (isUpdate) {
            int u;
            if (code != null) {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_schedule SET
                            schedule_name = ?, schedule_code = ?, reco_domain_id = ?, reconciliation_stage_id = ?, is_active = ?,
                            schedule_frequency_id = ?, interval_n = ?, cron_expression = ?, timezone = ?,
                            level_id = ?, payment_gateway_id = ?,
                            allow_parallel_runs = ?, max_runtime_minutes = ?, retry_on_failure = ?, retry_count = ?,
                            notify_on_failure = ?, notify_on_completion = ?, notification_channels_json = ?::jsonb,
                            execution_json = ?::jsonb, modified_on = now(), modified_by = ?
                        WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, code, domainId, stageId, bool(m, "active", true), freqId, interval,
                        str(m, "cronExpression"), str(m, "timezone") == null ? "UTC" : str(m, "timezone"),
                        levelId, paymentGatewayId,
                        bool(m, "allowParallelRuns", false), intVal(m, "maxRuntimeMinutes"),
                        bool(m, "retryOnFailure", true), intVal(m, "retryCount") == null ? 3 : intVal(m, "retryCount"),
                        notifyFailure, notifyCompletion,
                        toJson(channels), execJson, actor, id, tenantUuid);
            } else {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_schedule SET
                            schedule_name = ?, reco_domain_id = ?, reconciliation_stage_id = ?, is_active = ?,
                            schedule_frequency_id = ?, interval_n = ?, cron_expression = ?, timezone = ?,
                            level_id = ?, payment_gateway_id = ?,
                            allow_parallel_runs = ?, max_runtime_minutes = ?, retry_on_failure = ?, retry_count = ?,
                            notify_on_failure = ?, notify_on_completion = ?, notification_channels_json = ?::jsonb,
                            execution_json = ?::jsonb, modified_on = now(), modified_by = ?
                        WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, domainId, stageId, bool(m, "active", true), freqId, interval,
                        str(m, "cronExpression"), str(m, "timezone") == null ? "UTC" : str(m, "timezone"),
                        levelId, paymentGatewayId,
                        bool(m, "allowParallelRuns", false), intVal(m, "maxRuntimeMinutes"),
                        bool(m, "retryOnFailure", true), intVal(m, "retryCount") == null ? 3 : intVal(m, "retryCount"),
                        notifyFailure, notifyCompletion,
                        toJson(channels), execJson, actor, id, tenantUuid);
            }
            if (u == 0) {
                return Map.of("updated", false, "scheduleId", idOrNull);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_schedule (
                        reco_schedule_id, application_id, schedule_name, schedule_code, reco_domain_id, reconciliation_stage_id,
                        is_active, schedule_frequency_id, interval_n, cron_expression, timezone,
                        level_id, payment_gateway_id,
                        allow_parallel_runs, max_runtime_minutes, retry_on_failure, retry_count,
                        notify_on_failure, notify_on_completion, notification_channels_json, execution_json, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)
                    """, id, tenantUuid, name, code, domainId, stageId, bool(m, "active", true), freqId, interval,
                    str(m, "cronExpression"), str(m, "timezone") == null ? "UTC" : str(m, "timezone"),
                    levelId, paymentGatewayId,
                    bool(m, "allowParallelRuns", false), intVal(m, "maxRuntimeMinutes"),
                    bool(m, "retryOnFailure", true), intVal(m, "retryCount") == null ? 3 : intVal(m, "retryCount"),
                    notifyFailure, notifyCompletion,
                    toJson(channels), execJson, actor);
        }
        String savedCode = code != null ? code : loadScheduleCode(id);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_SCHEDULE_UPDATED" : "RECO_SCHEDULE_CREATED", id.toString(),
                Map.of("scheduleCode", savedCode));
        Map<String, Object> out = new HashMap<>();
        out.put("scheduleId", id.toString());
        out.put("scheduleCode", savedCode);
        out.put("updated", true);
        return out;
    }

    private void mergeExecutionField(Map<String, Object> exec, Map<String, Object> m, String key) {
        String v = str(m, key);
        if (v != null && !v.isBlank()) {
            exec.put(key, v);
        }
    }

    private String loadScheduleCode(UUID id) {
        List<String> rows = jdbc.query(
                "SELECT schedule_code FROM reconciliation.reco_schedule WHERE reco_schedule_id = ? LIMIT 1",
                (rs, rn) -> rs.getString(1), id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> patchScheduleActive(String tenantId, String id, boolean active) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("""
                UPDATE reconciliation.reco_schedule SET is_active = ?, modified_on = now()
                WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, active, sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id, "active", active, "status", active ? "ACTIVE" : "PAUSED");
    }

    public Map<String, Object> deactivateSchedule(String tenantId, String id, String actorUserId) {
        return patchScheduleActive(tenantId, id, false);
    }

    public Map<String, Object> cloneSchedule(String tenantId, String id, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID rid = parseOptionalUuid(id);
        UUID newId = UUID.randomUUID();
        int n = jdbc.update("""
                INSERT INTO reconciliation.reco_schedule (
                    reco_schedule_id, application_id, schedule_name, schedule_code, reco_domain_id, reconciliation_stage_id,
                    is_active, schedule_frequency_id, interval_n, cron_expression, timezone,
                    level_id, payment_gateway_id, allow_parallel_runs, max_runtime_minutes,
                    retry_on_failure, retry_count, notify_on_failure, notify_on_completion,
                    notification_channels_json, execution_json, created_by
                )
                SELECT ?, s.application_id, s.schedule_name || ' (Copy)',
                       s.schedule_code || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                       s.reco_domain_id, s.reconciliation_stage_id, false,
                       s.schedule_frequency_id, s.interval_n, s.cron_expression, s.timezone,
                       s.level_id, s.payment_gateway_id, s.allow_parallel_runs, s.max_runtime_minutes,
                       s.retry_on_failure, s.retry_count, s.notify_on_failure, s.notify_on_completion,
                       s.notification_channels_json, s.execution_json, ?
                FROM reconciliation.reco_schedule s
                WHERE s.reco_schedule_id = ? AND s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ?
                """, newId, actor, rid, tenantUuid);
        if (n == 0) {
            return Map.of("updated", false);
        }
        writeAudit(tenantUuid, actor, "RECO_SCHEDULE_CLONED", newId.toString(), Map.of("fromScheduleId", id));
        return Map.of("scheduleId", newId.toString(), "updated", true);
    }

    public int pauseAllSchedules(String tenantId) {
        return jdbc.update("""
                UPDATE reconciliation.reco_schedule SET is_active = false, modified_on = now()
                WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, parseOptionalUuid(tenantId));
    }

    public int resumeAllSchedules(String tenantId) {
        return jdbc.update("""
                UPDATE reconciliation.reco_schedule SET is_active = true, modified_on = now()
                WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, parseOptionalUuid(tenantId));
    }

    public Map<String, Object> updateRunQueuePriority(String runReference, String priority) {
        int n = jdbc.update("""
                UPDATE reconciliation.reconciliation_run
                SET filters_json = COALESCE(filters_json, '{}'::jsonb) || jsonb_build_object('priority', ?),
                    modified_on = now()
                WHERE run_reference_code = ?
                """, priority, runReference);
        return Map.of("updated", n > 0, "runReference", runReference, "priority", priority);
    }

    public Map<String, Object> cancelQueuedRun(String runReference) {
        int n = jdbc.update("""
                UPDATE reconciliation.reconciliation_run
                SET reconciliation_run_status_id = (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code = 'CANCELLED'),
                    ended_on = COALESCE(ended_on, now()), modified_on = now()
                WHERE run_reference_code = ?
                """, runReference);
        return Map.of("updated", n > 0, "runReference", runReference, "status", "CANCELLED");
    }

    public Map<String, Object> forceStartQueuedRun(String runReference) {
        int n = jdbc.update("""
                UPDATE reconciliation.reconciliation_run
                SET reconciliation_run_status_id = (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code = 'RUNNING'),
                    started_on = COALESCE(started_on, now()), modified_on = now()
                WHERE run_reference_code = ?
                """, runReference);
        return Map.of("updated", n > 0, "runReference", runReference, "status", "RUNNING");
    }

    public Map<String, Object> softDeleteSchedule(String tenantId, String id) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("""
                UPDATE reconciliation.reco_schedule SET deleted_on = now(), modified_on = now()
                WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id);
    }

    public Map<String, Object> loadRunFiltersForRetry(String runReference) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT filters_json AS "filtersJson", run_type AS "runType"
                FROM reconciliation.reconciliation_run
                WHERE run_reference_code = ?
                LIMIT 1
                """, runReference);
        if (rows.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> row = rows.get(0);
        Object fj = row.get("filtersJson");
        Map<String, Object> filters = new LinkedHashMap<>();
        if (fj instanceof Map<?, ?> m) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cast = (Map<String, Object>) m;
            filters.putAll(cast);
        }
        int retry = 0;
        Object rc = filters.get("retryCount");
        if (rc instanceof Number n) {
            retry = n.intValue();
        } else if (rc != null) {
            try {
                retry = Integer.parseInt(String.valueOf(rc));
            } catch (NumberFormatException ignored) {
                retry = 0;
            }
        }
        filters.put("retryCount", retry + 1);
        filters.put("retriedFromRunReference", runReference);
        Map<String, Object> out = new HashMap<>();
        out.put("filters", filters);
        out.put("runType", row.get("runType"));
        return out;
    }
}
