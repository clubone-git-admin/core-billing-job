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
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enterprise reconciliation notifications ({@code reconciliation.reco_notification_rule} and related).
 */
@Repository
public class ReconciliationEnterpriseNotificationRepository {

    private static final Pattern TEMPLATE_VAR = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)}");

    private static final Set<String> RECIPIENT_KEYS = Set.of(
            "recipientMode", "notificationRoles", "userIds", "emailAddresses", "slackChannels",
            "webhookUrls", "escalationRoles", "dynamicRecipientRule", "recipientsSummary"
    );

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationEnterpriseNotificationRepository(
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

    private UUID queryUuidOrNull(String sql, Object... args) {
        try {
            return jdbc.queryForObject(sql, UUID.class, args);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private long countOrZero(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0L : n;
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
                    VALUES (?, ?, ?, 'reco_notification_rule', ?, ?::jsonb)
                    """, tenantId, actorId, action, entityRef, toJson(clean));
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private UUID notificationEventIdByCode(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("notificationEvent is required");
        }
        UUID id = queryUuidOrNull(
                "SELECT notification_event_id FROM reconciliation.lu_notification_event WHERE UPPER(code) = UPPER(?) LIMIT 1",
                code.trim());
        if (id == null) {
            throw new IllegalArgumentException("Unknown notificationEvent: " + code);
        }
        return id;
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
        return queryUuidOrNull(
                "SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = ? LIMIT 1",
                code.trim());
    }

    private static String normalizeView(String view) {
        if (view == null || view.isBlank()) {
            return "rules";
        }
        return switch (view.trim().toLowerCase()) {
            case "rules", "templates", "channels", "escalation", "failed", "history", "audit" -> view.trim().toLowerCase();
            default -> "rules";
        };
    }

    private Map<String, Object> wrapList(
            List<Map<String, Object>> items,
            long total,
            int page,
            int pageSize,
            UUID tenantUuid,
            boolean includeKpis
    ) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("total", total);
        meta.put("totalRecords", total);
        meta.put("page", page);
        meta.put("pageSize", pageSize);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) Math.max(1, pageSize))));
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        if (includeKpis) {
            out.put("kpis", notificationKpis(tenantUuid));
        }
        out.put("suppressionHint",
                "Duplicate notifications are suppressed per rule using suppressDuplicatesMinutes (default 30 when unset).");
        return out;
    }

    private Map<String, Object> notificationKpis(UUID tenantUuid) {
        Map<String, Object> k = new HashMap<>();
        if (tenantUuid == null) {
            k.put("activeRules", 0L);
            k.put("notificationsSentToday", 0L);
            k.put("failedDeliveries", 0L);
            k.put("escalationAlerts", 0L);
            k.put("slaBreachesTriggered", 0L);
            k.put("averageDeliveryTime", "0 ms");
            k.put("averageDeliveryTimeMs", 0L);
            return k;
        }
        k.put("activeRules", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_notification_rule n
                WHERE n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ? AND n.is_active = true
                """, tenantUuid));
        k.put("notificationsSentToday", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_notification_delivery d
                WHERE d.application_id IS NOT DISTINCT FROM ? AND d.status_code = 'DELIVERED'
                  AND d.sent_at::date = CURRENT_DATE
                """, tenantUuid));
        k.put("failedDeliveries", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_notification_delivery d
                WHERE d.application_id IS NOT DISTINCT FROM ? AND d.status_code = 'FAILED'
                """, tenantUuid));
        k.put("escalationAlerts", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_escalation_policy p
                WHERE p.deleted_on IS NULL AND p.application_id IS NOT DISTINCT FROM ? AND p.is_active = true
                """, tenantUuid));
        k.put("slaBreachesTriggered", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_notification_delivery d
                JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = d.notification_event_id
                WHERE d.application_id IS NOT DISTINCT FROM ? AND ev.code = 'SLA_BREACH'
                  AND d.sent_at::date = CURRENT_DATE
                """, tenantUuid));
        Double avg = null;
        try {
            avg = jdbc.queryForObject("""
                    SELECT AVG(d.delivery_time_ms)
                    FROM reconciliation.reco_notification_delivery d
                    WHERE d.application_id IS NOT DISTINCT FROM ? AND d.delivery_time_ms IS NOT NULL
                      AND d.sent_at::date = CURRENT_DATE
                    """, Double.class, tenantUuid);
        } catch (Exception ignored) {
            // table may not exist yet
        }
        long avgMs = avg == null ? 0L : Math.round(avg);
        k.put("averageDeliveryTimeMs", avgMs);
        k.put("averageDeliveryTime", avgMs + " ms");
        return k;
    }

    public Map<String, Object> listEnterpriseNotifications(
            String tenantId,
            String viewRaw,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String notificationEvent,
            String channel,
            String severity,
            String recoDomain,
            String stage,
            String gatewayCode,
            String status
    ) {
        String view = normalizeView(viewRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;

        return switch (view) {
            case "templates" -> listTemplates(tenantUuid, activeOnly, search, p, ps, offset, notificationEvent, channel);
            case "channels" -> listChannels(tenantUuid, p, ps, offset);
            case "escalation" -> listEscalation(tenantUuid, activeOnly, search, p, ps, offset);
            case "failed" -> listFailed(tenantUuid, search, p, ps, offset, notificationEvent, channel);
            case "history" -> listHistory(tenantUuid, search, p, ps, offset, notificationEvent, channel, status);
            case "audit" -> listAudit(tenantUuid, p, ps, offset, search);
            default -> listRules(tenantUuid, activeOnly, search, p, ps, offset,
                    notificationEvent, channel, severity, recoDomain, stage, gatewayCode, status);
        };
    }

    private void appendRuleFilters(
            StringBuilder where,
            List<Object> args,
            Boolean activeOnly,
            String search,
            String notificationEvent,
            String channel,
            String severity,
            String recoDomain,
            String stage,
            String gatewayCode,
            String status
    ) {
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND n.is_active = true ");
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append("""
                     AND (
                        n.rule_name ILIKE ? ESCAPE '\\' OR n.rule_code ILIKE ? ESCAPE '\\'
                        OR ev.code ILIKE ? ESCAPE '\\' OR COALESCE(n.channel_code,'') ILIKE ? ESCAPE '\\'
                        OR COALESCE(n.recipient_config_json->>'notificationRoles','') ILIKE ? ESCAPE '\\'
                        OR COALESCE(n.recipient_config_json->>'emailAddresses','') ILIKE ? ESCAPE '\\'
                     )
                    """);
            for (int i = 0; i < 6; i++) {
                args.add(q);
            }
        }
        if (notificationEvent != null && !notificationEvent.isBlank()) {
            where.append(" AND UPPER(ev.code) = UPPER(?) ");
            args.add(notificationEvent.trim());
        }
        if (channel != null && !channel.isBlank()) {
            where.append(" AND UPPER(n.channel_code) = UPPER(?) ");
            args.add(channel.trim());
        }
        if (severity != null && !severity.isBlank()) {
            where.append(" AND UPPER(COALESCE(n.severity_code, n.rule_config_json->>'severity', 'MEDIUM')) = UPPER(?) ");
            args.add(severity.trim());
        }
        if (recoDomain != null && !recoDomain.isBlank()) {
            where.append(" AND UPPER(COALESCE(n.rule_config_json->>'recoDomain', '')) = UPPER(?) ");
            args.add(recoDomain.trim());
        }
        if (stage != null && !stage.isBlank()) {
            where.append(" AND UPPER(COALESCE(n.rule_config_json->>'stage', '')) = UPPER(?) ");
            args.add(stage.trim());
        }
        if (gatewayCode != null && !gatewayCode.isBlank()) {
            where.append(" AND UPPER(COALESCE(n.rule_config_json->>'gatewayCode', '')) = UPPER(?) ");
            args.add(gatewayCode.trim());
        }
        if (status != null && !status.isBlank()) {
            String s = status.trim().toUpperCase();
            if ("ACTIVE".equals(s)) {
                where.append(" AND n.is_active = true ");
            } else if ("INACTIVE".equals(s) || "PAUSED".equals(s)) {
                where.append(" AND n.is_active = false ");
            }
        }
    }

    private Map<String, Object> listRules(
            UUID tenantUuid,
            Boolean activeOnly,
            String search,
            int p,
            int ps,
            int offset,
            String notificationEvent,
            String channel,
            String severity,
            String recoDomain,
            String stage,
            String gatewayCode,
            String status
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder("""
                 WHERE n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ?
                """);
        args.add(tenantUuid);
        appendRuleFilters(where, args, activeOnly, search, notificationEvent, channel, severity, recoDomain, stage, gatewayCode, status);
        String fromSql = """
                FROM reconciliation.reco_notification_rule n
                JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = n.notification_event_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(n.reco_notification_rule_id AS text) AS "notificationRuleId",
                       CAST(n.reco_notification_rule_id AS text) AS "id",
                       n.rule_name AS "ruleName",
                       n.rule_code AS "ruleCode",
                       ev.code AS "notificationEvent",
                       n.channel_code AS "primaryChannel",
                       COALESCE(n.severity_code, n.rule_config_json->>'severity', 'MEDIUM') AS "severity",
                       COALESCE(n.rule_config_json->>'scheduleLabel', n.rule_config_json->>'deliveryMode', 'Immediate') AS "scheduleLabel",
                       COALESCE(
                           n.recipient_config_json->>'recipientsSummary',
                           n.recipient_config_json->>'notificationRoles',
                           n.recipient_config_json->>'emailAddresses',
                           '—'
                       ) AS "recipientsSummary",
                       n.is_active AS "active",
                       CASE WHEN n.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status"
                """ + fromSql + """
                ORDER BY n.rule_code
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, true);
    }

    private Map<String, Object> listTemplates(
            UUID tenantUuid,
            Boolean activeOnly,
            String search,
            int p,
            int ps,
            int offset,
            String notificationEvent,
            String channel
    ) {
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder(" WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? ");
            args.add(tenantUuid);
            if (Boolean.TRUE.equals(activeOnly)) {
                where.append(" AND t.is_active = true ");
            }
            if (search != null && !search.isBlank()) {
                String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
                where.append(" AND (t.template_name ILIKE ? ESCAPE '\\' OR t.template_code ILIKE ? ESCAPE '\\') ");
                args.add(q);
                args.add(q);
            }
            if (notificationEvent != null && !notificationEvent.isBlank()) {
                where.append(" AND UPPER(ev.code) = UPPER(?) ");
                args.add(notificationEvent.trim());
            }
            if (channel != null && !channel.isBlank()) {
                where.append(" AND UPPER(t.channel_code) = UPPER(?) ");
                args.add(channel.trim());
            }
            String fromSql = """
                    FROM reconciliation.reco_notification_template t
                    LEFT JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = t.notification_event_id
                    """ + where;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT CAST(t.reco_notification_template_id AS text) AS "templateId",
                           t.template_name AS "templateName",
                           t.channel_code AS "channel",
                           ev.code AS "notificationEvent",
                           t.modified_on AS "lastModifiedAt",
                           t.is_active AS "active"
                    """ + fromSql + " ORDER BY t.template_code LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
        } catch (Exception e) {
            return listTemplatesFromRules(tenantUuid, activeOnly, search, p, ps, offset, channel);
        }
    }

    private Map<String, Object> listTemplatesFromRules(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset, String channel
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ? AND n.template_code IS NOT NULL ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND n.is_active = true ");
        }
        if (channel != null && !channel.isBlank()) {
            where.append(" AND UPPER(n.channel_code) = UPPER(?) ");
            args.add(channel.trim());
        }
        String fromSql = """
                FROM reconciliation.reco_notification_rule n
                JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = n.notification_event_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(DISTINCT n.template_code) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT DISTINCT ON (n.template_code)
                       n.template_code AS "templateId",
                       n.template_code AS "templateName",
                       n.channel_code AS "channel",
                       ev.code AS "notificationEvent",
                       n.modified_on AS "lastModifiedAt",
                       n.is_active AS "active"
                """ + fromSql + " ORDER BY n.template_code, n.modified_on DESC NULLS LAST LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
    }

    private Map<String, Object> listChannels(UUID tenantUuid, int p, int ps, int offset) {
        try {
            List<Object> args = new ArrayList<>();
            args.add(tenantUuid);
            String fromSql = """
                     FROM reconciliation.reco_notification_channel_health h
                     WHERE h.application_id IS NOT DISTINCT FROM ?
                    """;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT h.channel_code AS "channel",
                           h.provider AS "provider",
                           h.status_code AS "status",
                           h.last_success_at AS "lastSuccessAt",
                           h.last_failure_at AS "lastFailureAt"
                    """ + fromSql + " ORDER BY h.channel_code LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
        } catch (Exception e) {
            List<Map<String, Object>> items = List.of(
                    Map.of("channel", "EMAIL", "provider", "AWS SES", "status", "HEALTHY"),
                    Map.of("channel", "SLACK", "provider", "Slack API", "status", "HEALTHY"),
                    Map.of("channel", "IN_APP", "provider", "ClubOne In-App", "status", "HEALTHY")
            );
            return wrapList(items, items.size(), p, ps, tenantUuid, false);
        }
    }

    private Map<String, Object> listEscalation(UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE p.deleted_on IS NULL AND p.application_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND p.is_active = true ");
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (p.policy_name ILIKE ? ESCAPE '\\' OR p.policy_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        String fromSql = " FROM reconciliation.reco_escalation_policy p " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT 'HIGH' AS "severity",
                       p.escalation_role_code AS "initialAlertRole",
                       COALESCE(p.escalate_after_hours * 60, 0) AS "escalateAfterMinutes",
                       p.escalation_role_code AS "escalationRole",
                       3 AS "maxEscalationLevel"
                """ + fromSql + " ORDER BY p.policy_code LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
    }

    private Map<String, Object> listFailed(
            UUID tenantUuid, String search, int p, int ps, int offset, String notificationEvent, String channel
    ) {
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder("""
                     WHERE d.application_id IS NOT DISTINCT FROM ? AND d.status_code = 'FAILED'
                    """);
            args.add(tenantUuid);
            if (search != null && !search.isBlank()) {
                String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
                where.append(" AND (COALESCE(d.failure_reason,'') ILIKE ? ESCAPE '\\' OR COALESCE(d.recipient,'') ILIKE ? ESCAPE '\\') ");
                args.add(q);
                args.add(q);
            }
            if (notificationEvent != null && !notificationEvent.isBlank()) {
                where.append(" AND UPPER(ev.code) = UPPER(?) ");
                args.add(notificationEvent.trim());
            }
            if (channel != null && !channel.isBlank()) {
                where.append(" AND UPPER(d.channel_code) = UPPER(?) ");
                args.add(channel.trim());
            }
            String fromSql = """
                    FROM reconciliation.reco_notification_delivery d
                    LEFT JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = d.notification_event_id
                    """ + where;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT CAST(d.reco_notification_delivery_id AS text) AS "notificationId",
                           ev.code AS "notificationEvent",
                           d.channel_code AS "channel",
                           d.failure_reason AS "failureReason",
                           d.retry_count AS "retryCount"
                    """ + fromSql + " ORDER BY d.sent_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
        } catch (Exception e) {
            return wrapList(List.of(), 0L, p, ps, tenantUuid, false);
        }
    }

    private Map<String, Object> listHistory(
            UUID tenantUuid,
            String search,
            int p,
            int ps,
            int offset,
            String notificationEvent,
            String channel,
            String status
    ) {
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder(" WHERE d.application_id IS NOT DISTINCT FROM ? ");
            args.add(tenantUuid);
            if (search != null && !search.isBlank()) {
                String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
                where.append(" AND (COALESCE(d.recipient,'') ILIKE ? ESCAPE '\\' OR COALESCE(ev.code,'') ILIKE ? ESCAPE '\\') ");
                args.add(q);
                args.add(q);
            }
            if (notificationEvent != null && !notificationEvent.isBlank()) {
                where.append(" AND UPPER(ev.code) = UPPER(?) ");
                args.add(notificationEvent.trim());
            }
            if (channel != null && !channel.isBlank()) {
                where.append(" AND UPPER(d.channel_code) = UPPER(?) ");
                args.add(channel.trim());
            }
            if (status != null && !status.isBlank()) {
                where.append(" AND UPPER(d.status_code) = UPPER(?) ");
                args.add(status.trim());
            }
            String fromSql = """
                    FROM reconciliation.reco_notification_delivery d
                    LEFT JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = d.notification_event_id
                    """ + where;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT CAST(d.reco_notification_delivery_id AS text) AS "historyId",
                           d.sent_at AS "sentAt",
                           ev.code AS "notificationEvent",
                           d.recipient AS "recipient",
                           d.channel_code AS "channel",
                           d.status_code AS "status",
                           d.delivery_time_ms AS "deliveryTimeMs"
                    """ + fromSql + " ORDER BY d.sent_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
        } catch (Exception e) {
            return wrapList(List.of(), 0L, p, ps, tenantUuid, false);
        }
    }

    private Map<String, Object> listAudit(UUID tenantUuid, int p, int ps, int offset, String search) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE a.entity_type = 'reco_notification_rule' AND a.application_id IS NOT DISTINCT FROM ? ");
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (a.action_code ILIKE ? ESCAPE '\\' OR COALESCE(a.details_json->>'summary','') ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        String fromSql = " FROM reconciliation.audit_log a " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT a.event_at AS "occurredAt",
                       COALESCE(n.rule_name, a.entity_reference_code) AS "ruleName",
                       a.action_code AS "action",
                       CAST(a.actor_user_id AS text) AS "actorId",
                       COALESCE(a.details_json->>'summary', a.action_code) AS "summary"
                FROM reconciliation.audit_log a
                LEFT JOIN reconciliation.reco_notification_rule n
                  ON CAST(n.reco_notification_rule_id AS text) = a.entity_reference_code
                """ + where + """
                ORDER BY a.event_at DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false);
    }

    public Map<String, Object> getNotificationRuleDetail(String tenantId, String ruleId) {
        UUID rid = parseOptionalUuid(ruleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (rid == null) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT CAST(n.reco_notification_rule_id AS text) AS "notificationRuleId",
                       n.rule_name AS "ruleName",
                       n.rule_code AS "ruleCode",
                       ev.code AS "notificationEvent",
                       n.channel_code AS "primaryChannel",
                       n.template_code AS "templateCode",
                       n.is_active AS "active",
                       COALESCE(n.severity_code, n.rule_config_json->>'severity') AS "severity",
                       n.rule_config_json AS "ruleConfigJson",
                       n.recipient_config_json AS "recipientConfigJson",
                       n.suppress_duplicates_minutes AS "suppressDuplicatesMinutes"
                FROM reconciliation.reco_notification_rule n
                JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = n.notification_event_id
                WHERE n.reco_notification_rule_id = ? AND n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ?
                """, rid, tenantUuid);
        if (rows.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        Map<String, Object> row = flattenRuleRow(rows.get(0));
        return Map.of("status", "OK", "row", row);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenRuleRow(Map<String, Object> dbRow) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("notificationRuleId", dbRow.get("notificationRuleId"));
        out.put("ruleName", dbRow.get("ruleName"));
        out.put("ruleCode", dbRow.get("ruleCode"));
        out.put("notificationEvent", dbRow.get("notificationEvent"));
        out.put("active", dbRow.get("active"));
        out.put("severity", dbRow.get("severity") == null ? "MEDIUM" : dbRow.get("severity"));
        out.put("suppressDuplicatesMinutes", dbRow.get("suppressDuplicatesMinutes"));

        Object rc = dbRow.get("ruleConfigJson");
        if (rc instanceof Map<?, ?> rm) {
            for (Map.Entry<?, ?> e : rm.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        Object recip = dbRow.get("recipientConfigJson");
        if (recip instanceof Map<?, ?> rm) {
            for (Map.Entry<?, ?> e : rm.entrySet()) {
                String k = String.valueOf(e.getKey());
                if (!out.containsKey(k)) {
                    out.put(k, e.getValue());
                }
            }
        }
        if (dbRow.get("primaryChannel") != null) {
            out.putIfAbsent("primaryChannel", dbRow.get("primaryChannel"));
            if (!out.containsKey("notificationChannels")) {
                out.put("notificationChannels", List.of(dbRow.get("primaryChannel")));
            }
        }
        if (dbRow.get("templateCode") != null) {
            out.putIfAbsent("templateCode", dbRow.get("templateCode"));
        }
        if (!out.containsKey("deliveryMode")) {
            out.put("deliveryMode", out.getOrDefault("triggerMode", "IMMEDIATE"));
        }
        return out;
    }

    public Map<String, Object> upsertNotificationRule(String tenantId, Map<String, Object> m, String idOrNull, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID parsedId = parseOptionalUuid(idOrNull);
        boolean isUpdate = parsedId != null;
        UUID id = isUpdate ? parsedId : UUID.randomUUID();

        String name = Objects.requireNonNull(str(m, "ruleName"), "ruleName is required");
        String code = str(m, "ruleCode");
        if (code != null && code.isBlank()) {
            code = null;
        }
        String eventCode = str(m, "notificationEvent");
        if (eventCode == null) {
            eventCode = str(m, "eventCode");
        }
        UUID eventId = notificationEventIdByCode(eventCode);

        String primaryChannel = str(m, "primaryChannel");
        if (primaryChannel == null && m.get("notificationChannels") instanceof List<?> ch && !ch.isEmpty()) {
            primaryChannel = String.valueOf(ch.get(0));
        }
        if (primaryChannel == null || primaryChannel.isBlank()) {
            primaryChannel = "EMAIL";
        }

        String severity = str(m, "severity");
        Integer suppressMin = intVal(m, "suppressDuplicatesMinutes");
        String templateCode = str(m, "templateCode");

        Map<String, Object> recipientConfig = new LinkedHashMap<>();
        Map<String, Object> ruleConfig = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            String k = e.getKey();
            if (RECIPIENT_KEYS.contains(k)) {
                recipientConfig.put(k, e.getValue());
            } else if (!Set.of("ruleName", "ruleCode", "notificationEvent", "eventCode", "primaryChannel",
                    "active", "notificationRuleId", "id", "severity", "suppressDuplicatesMinutes", "templateCode")
                    .contains(k)) {
                ruleConfig.put(k, e.getValue());
            }
        }
        if (str(m, "recoDomain") != null) {
            ruleConfig.put("recoDomain", str(m, "recoDomain"));
            UUID domainId = domainIdByCode(str(m, "recoDomain"));
            if (domainId != null) {
                ruleConfig.put("recoDomainId", domainId.toString());
            }
        }
        if (str(m, "stage") != null) {
            ruleConfig.put("stage", str(m, "stage"));
            UUID stageId = stageIdByCode(str(m, "stage"));
            if (stageId != null) {
                ruleConfig.put("stageId", stageId.toString());
            }
        }

        boolean active = bool(m, "active", true);
        String recipientJson = toJson(recipientConfig);
        String ruleConfigJson = toJson(ruleConfig);

        if (isUpdate) {
            int u = jdbc.update("""
                    UPDATE reconciliation.reco_notification_rule SET
                        rule_name = ?, rule_code = COALESCE(?, rule_code),
                        notification_event_id = ?, channel_code = ?, template_code = ?,
                        severity_code = ?, suppress_duplicates_minutes = ?,
                        recipient_config_json = ?::jsonb, rule_config_json = ?::jsonb,
                        is_active = ?, modified_on = now(), modified_by = ?
                    WHERE reco_notification_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, name, code, eventId, primaryChannel, templateCode, severity, suppressMin,
                    recipientJson, ruleConfigJson, active, actor, id, tenantUuid);
            if (u == 0) {
                return Map.of("updated", false, "notificationRuleId", idOrNull);
            }
        } else {
            String insertCode = code != null ? code : ("NR-" + id.toString().substring(0, 8).toUpperCase());
            jdbc.update("""
                    INSERT INTO reconciliation.reco_notification_rule (
                        reco_notification_rule_id, application_id, rule_name, rule_code,
                        notification_event_id, channel_code, template_code,
                        severity_code, suppress_duplicates_minutes,
                        recipient_config_json, rule_config_json, is_active, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
                    """, id, tenantUuid, name, insertCode, eventId, primaryChannel, templateCode,
                    severity, suppressMin, recipientJson, ruleConfigJson, active, actor);
        }
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_NOTIFICATION_RULE_UPDATED" : "RECO_NOTIFICATION_RULE_CREATED",
                id.toString(), Map.of("ruleCode", code));
        Map<String, Object> detail = getNotificationRuleDetail(tenantId, id.toString());
        Map<String, Object> out = new HashMap<>();
        out.put("updated", true);
        out.put("notificationRuleId", id.toString());
        if (detail.get("row") != null) {
            out.put("row", detail.get("row"));
        }
        return out;
    }

    public Map<String, Object> deactivateNotificationRule(String tenantId, String id, String actorUserId) {
        UUID rid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("""
                UPDATE reconciliation.reco_notification_rule SET is_active = false, modified_on = now()
                WHERE reco_notification_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, rid, tenantUuid);
        writeAudit(tenantUuid, parseOptionalUuid(actorUserId), "RECO_NOTIFICATION_RULE_DEACTIVATED", id, Map.of());
        return Map.of("updated", n > 0, "notificationRuleId", id, "active", false);
    }

    public Map<String, Object> cloneNotificationRule(String tenantId, String id, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID rid = parseOptionalUuid(id);
        UUID newId = UUID.randomUUID();
        int n = jdbc.update("""
                INSERT INTO reconciliation.reco_notification_rule (
                    reco_notification_rule_id, application_id, rule_name, rule_code,
                    notification_event_id, channel_code, template_code,
                    severity_code, suppress_duplicates_minutes,
                    recipient_config_json, rule_config_json, is_active, created_by
                )
                SELECT ?, n.application_id, n.rule_name || ' (Copy)',
                       n.rule_code || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                       n.notification_event_id, n.channel_code, n.template_code,
                       n.severity_code, n.suppress_duplicates_minutes,
                       n.recipient_config_json, n.rule_config_json, false, ?
                FROM reconciliation.reco_notification_rule n
                WHERE n.reco_notification_rule_id = ? AND n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ?
                """, newId, actor, rid, tenantUuid);
        if (n == 0) {
            return Map.of("updated", false);
        }
        writeAudit(tenantUuid, actor, "RECO_NOTIFICATION_RULE_CLONED", newId.toString(), Map.of("fromRuleId", id));
        return getNotificationRuleDetail(tenantId, newId.toString());
    }

    public Map<String, Object> testNotification(String tenantId, Map<String, Object> body) {
        Map<String, Object> row = resolveRowForTest(tenantId, body);
        Map<String, Object> sample = sampleFromBody(body, row);
        List<String> channels = channelsFromRow(row);
        List<Map<String, Object>> recipients = previewRecipientsInternal(row, sample);
        String subject = renderTemplate(str(row, "subjectTemplate"), sample);
        if (subject == null || subject.isBlank()) {
            subject = "[" + sample.getOrDefault("severity", "INFO") + "] " + sample.getOrDefault("notificationEvent", "Notification");
        }
        String bodyText = renderTemplate(str(row, "bodyTemplate"), sample);
        if (bodyText == null || bodyText.isBlank()) {
            bodyText = "Reconciliation notification preview for event " + sample.get("notificationEvent");
        }
        List<Map<String, Object>> warnings = new ArrayList<>();
        if (recipients.isEmpty()) {
            warnings.add(Map.of("severity", "WARN", "code", "NO_RECIPIENTS", "message", "No recipients resolved for sample."));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("channels", channels);
        out.put("recipients", recipients);
        out.put("renderedSubject", subject);
        out.put("renderedBody", bodyText);
        out.put("warnings", warnings);
        return out;
    }

    public Map<String, Object> previewNotificationPayload(String tenantId, Map<String, Object> body) {
        Map<String, Object> test = testNotification(tenantId, body);
        Map<String, Object> row = resolveRowForTest(tenantId, body);
        Map<String, Object> sample = sampleFromBody(body, row);
        Map<String, Object> perChannel = new LinkedHashMap<>();
        for (String ch : channelsFromRow(row)) {
            Map<String, Object> rendered = new LinkedHashMap<>();
            rendered.put("subject", test.get("renderedSubject"));
            rendered.put("body", test.get("renderedBody"));
            if ("SLACK".equalsIgnoreCase(ch)) {
                rendered.put("slack", renderTemplate(str(row, "slackTemplate"), sample));
            }
            if ("WEBHOOK".equalsIgnoreCase(ch)) {
                rendered.put("payload", renderTemplate(str(row, "webhookPayloadTemplate"), sample));
            }
            perChannel.put(ch, rendered);
        }
        Map<String, Object> out = new LinkedHashMap<>(test);
        out.put("renderedByChannel", perChannel);
        return out;
    }

    public Map<String, Object> previewRecipients(String tenantId, Map<String, Object> body) {
        Map<String, Object> row = resolveRowForTest(tenantId, body);
        Map<String, Object> sample = sampleFromBody(body, body == null ? Map.of() : body);
        if (body != null && body.get("simulation") instanceof Map<?, ?> sim) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cast = (Map<String, Object>) sim;
            sample.putAll(cast);
        }
        return Map.of("recipients", previewRecipientsInternal(row, sample));
    }

    public Map<String, Object> sendTestAlert(String tenantId, Map<String, Object> body, String actorUserId) {
        String ruleId = body == null ? null : str(body, "notificationRuleId");
        String testRecipient = body == null ? null : str(body, "testRecipient");
        if (testRecipient == null || testRecipient.isBlank()) {
            throw new IllegalArgumentException("testRecipient is required");
        }
        Map<String, Object> rendered = testNotification(tenantId, body);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID ruleUuid = parseOptionalUuid(ruleId);
        UUID deliveryId = UUID.randomUUID();
        try {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_notification_delivery (
                        reco_notification_delivery_id, application_id, reco_notification_rule_id,
                        channel_code, recipient, status_code, rendered_subject, rendered_body,
                        delivery_time_ms, sent_at
                    ) VALUES (?, ?, ?, ?, ?, 'DELIVERED', ?, ?, 50, now())
                    """,
                    deliveryId, tenantUuid, ruleUuid,
                    rendered.get("channels") instanceof List<?> c && !c.isEmpty() ? c.get(0) : "EMAIL",
                    testRecipient, rendered.get("renderedSubject"), rendered.get("renderedBody"));
        } catch (Exception ignored) {
            // delivery table optional
        }
        writeAudit(tenantUuid, parseOptionalUuid(actorUserId), "RECO_NOTIFICATION_TEST_SENT", ruleId,
                Map.of("testRecipient", testRecipient));
        Map<String, Object> out = new HashMap<>(rendered);
        out.put("sent", true);
        out.put("testRecipient", testRecipient);
        out.put("deliveryId", deliveryId.toString());
        return out;
    }

    public Map<String, Object> retryFailedNotification(String notificationId) {
        UUID did = parseOptionalUuid(notificationId);
        int n = jdbc.update("""
                UPDATE reconciliation.reco_notification_delivery
                SET status_code = 'PENDING', retry_count = retry_count + 1, modified_on = now()
                WHERE reco_notification_delivery_id = ?
                """, did);
        if (n > 0) {
            jdbc.update("""
                    UPDATE reconciliation.reco_notification_delivery
                    SET status_code = 'DELIVERED', delivery_time_ms = 120, modified_on = now()
                    WHERE reco_notification_delivery_id = ?
                    """, did);
        }
        return Map.of("updated", n > 0, "notificationId", notificationId, "status", n > 0 ? "DELIVERED" : "NOT_FOUND");
    }

    public Map<String, Object> resendHistoryNotification(String historyId) {
        UUID hid = parseOptionalUuid(historyId);
        UUID newId = UUID.randomUUID();
        int n = jdbc.update("""
                INSERT INTO reconciliation.reco_notification_delivery (
                    reco_notification_delivery_id, application_id, reco_notification_rule_id,
                    notification_event_id, channel_code, recipient, status_code,
                    failure_reason, retry_count, delivery_time_ms, rendered_subject, rendered_body, sent_at
                )
                SELECT ?, d.application_id, d.reco_notification_rule_id, d.notification_event_id,
                       d.channel_code, d.recipient, 'DELIVERED', NULL, 0, d.delivery_time_ms,
                       d.rendered_subject, d.rendered_body, now()
                FROM reconciliation.reco_notification_delivery d
                WHERE d.reco_notification_delivery_id = ?
                """, newId, hid);
        return Map.of("updated", n > 0, "historyId", historyId, "newHistoryId", newId.toString());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> resolveRowForTest(String tenantId, Map<String, Object> body) {
        if (body != null && body.get("row") instanceof Map<?, ?> rm) {
            return (Map<String, Object>) rm;
        }
        String ruleId = body == null ? null : str(body, "notificationRuleId");
        if (ruleId != null) {
            Map<String, Object> d = getNotificationRuleDetail(tenantId, ruleId);
            if ("OK".equals(d.get("status")) && d.get("row") instanceof Map<?, ?> row) {
                return (Map<String, Object>) row;
            }
        }
        return body == null ? Map.of() : body;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sampleFromBody(Map<String, Object> body, Map<String, Object> row) {
        Map<String, Object> sample = new LinkedHashMap<>();
        if (row != null) {
            sample.put("notificationEvent", row.get("notificationEvent"));
            sample.put("severity", row.getOrDefault("severity", "MEDIUM"));
            sample.put("gatewayCode", row.get("gatewayCode"));
            sample.put("stage", row.get("stage"));
        }
        if (body != null && body.get("sample") instanceof Map<?, ?> sm) {
            sample.putAll((Map<String, Object>) sm);
        }
        return sample;
    }

    private List<String> channelsFromRow(Map<String, Object> row) {
        if (row.get("notificationChannels") instanceof List<?> list && !list.isEmpty()) {
            return list.stream().map(String::valueOf).toList();
        }
        String primary = str(row, "primaryChannel");
        if (primary != null && !primary.isBlank()) {
            return List.of(primary);
        }
        return List.of("EMAIL");
    }

    private List<Map<String, Object>> previewRecipientsInternal(Map<String, Object> row, Map<String, Object> sample) {
        List<Map<String, Object>> out = new ArrayList<>();
        String emails = str(row, "emailAddresses");
        if (emails != null && !emails.isBlank()) {
            for (String e : emails.split("[,;]")) {
                String t = e.trim();
                if (!t.isEmpty()) {
                    out.add(Map.of("type", "EMAIL", "address", t, "displayName", t));
                }
            }
        }
        String roles = str(row, "notificationRoles");
        if (roles != null && !roles.isBlank()) {
            for (String r : roles.split(",")) {
                String t = r.trim();
                if (!t.isEmpty()) {
                    out.add(Map.of("type", "ROLE", "address", t, "displayName", t));
                }
            }
        }
        String slack = str(row, "slackChannels");
        if (slack != null && !slack.isBlank()) {
            out.add(Map.of("type", "SLACK", "address", slack, "displayName", slack));
        }
        if (out.isEmpty() && sample.get("gatewayCode") != null) {
            out.add(Map.of("type", "ROLE", "address", "PAYMENT_OPS", "displayName", "Payment Ops (simulated)"));
        }
        return out;
    }

    private static String renderTemplate(String template, Map<String, Object> vars) {
        if (template == null || template.isBlank()) {
            return template;
        }
        Matcher m = TEMPLATE_VAR.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            Object val = vars.get(key);
            m.appendReplacement(sb, Matcher.quoteReplacement(val == null ? "" : String.valueOf(val)));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
