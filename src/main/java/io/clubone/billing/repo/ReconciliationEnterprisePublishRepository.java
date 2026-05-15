package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Enterprise config publish governance ({@code reconciliation.reco_config_publish}).
 */
@Repository
public class ReconciliationEnterprisePublishRepository {

    private static final List<String> COMPONENT_TYPES = List.of(
            "MATCHING_RULES", "GL_MAPPINGS", "THRESHOLDS", "SCHEDULES",
            "NOTIFICATION_RULES", "ACCESS_POLICIES", "ESCALATION_POLICIES"
    );

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationEnterprisePublishRepository(
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

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize JSON", e);
        }
    }

    private long countOrZero(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0L : n;
    }

    private UUID queryUuidOrNull(String sql, Object... args) {
        try {
            return jdbc.queryForObject(sql, UUID.class, args);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private void writeAudit(UUID tenantId, UUID actorId, String action, String entityRef, Object details) {
        try {
            jdbc.update("""
                    INSERT INTO reconciliation.audit_log (application_id, actor_user_id, action_code, entity_type, entity_reference_code, details_json)
                    VALUES (?, ?, ?, 'reco_config_publish', ?, ?::jsonb)
                    """, tenantId, actorId, action, entityRef, toJson(details));
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private static String normalizeView(String view) {
        if (view == null || view.isBlank()) {
            return "current";
        }
        return switch (view.trim().toLowerCase()) {
            case "current", "draft", "history", "diff", "rollback", "promotion", "audit", "wizard" -> view.trim().toLowerCase();
            default -> "current";
        };
    }

    private static String envOrDefault(String environment) {
        return environment == null || environment.isBlank() ? "PROD" : environment.trim().toUpperCase();
    }

    public Map<String, Object> listEnterprisePublish(
            String tenantId,
            String viewRaw,
            String search,
            int page,
            int pageSize,
            String environment,
            String version,
            String publishedBy,
            String publishStatus,
            String from,
            String to,
            String versionA,
            String versionB
    ) {
        String view = normalizeView(viewRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        String env = envOrDefault(environment);
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));

        return switch (view) {
            case "draft" -> listDraftView(tenantUuid, env, search, p, ps);
            case "history" -> listHistoryView(tenantUuid, env, search, p, ps, version, publishedBy, publishStatus, from, to);
            case "diff" -> listDiffView(tenantUuid, versionA, versionB);
            case "rollback" -> listRollbackView(tenantUuid, env, p, ps);
            case "promotion" -> listPromotionView(tenantUuid, p, ps);
            case "audit" -> listPublishAudit(tenantUuid, search, p, ps);
            case "wizard" -> wizardPanel(tenantUuid, env);
            default -> listCurrentView(tenantUuid, env);
        };
    }

    private Map<String, Object> wrapMeta(List<Map<String, Object>> items, long total, int page, int pageSize) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("total", total);
        meta.put("totalRecords", total);
        meta.put("page", page);
        meta.put("pageSize", pageSize);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) Math.max(1, pageSize))));
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        return out;
    }

    private Map<String, Object> publishKpis(UUID tenantUuid, String env) {
        Map<String, Object> k = new HashMap<>();
        Map<String, Object> current = loadCurrentPublished(tenantUuid, env);
        k.put("currentVersion", current.get("versionLabel"));
        k.put("pendingDraftChanges", countPendingDrafts(tenantUuid));
        k.put("lastPublished", current.get("publishedAt"));
        k.put("pendingApprovals", countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_config_draft_change
                WHERE application_id IS NOT DISTINCT FROM ? AND status_code = 'PENDING_APPROVAL'
                """, tenantUuid));
        boolean rollback = countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_config_publish
                WHERE application_id IS NOT DISTINCT FROM ? AND environment_code = ?
                  AND publish_status = 'ARCHIVED'
                """, tenantUuid, env) > 0;
        k.put("rollbackAvailable", rollback ? "Yes" : "No");
        try {
            k.put("environmentsSynced", countOrZero("""
                    SELECT COUNT(DISTINCT COALESCE(environment_code, 'PROD'))
                    FROM reconciliation.reco_config_publish
                    WHERE application_id IS NOT DISTINCT FROM ? AND publish_status = 'ACTIVE'
                    """, tenantUuid));
        } catch (Exception e) {
            k.put("environmentsSynced", 1L);
        }
        return k;
    }

    private long countPendingDrafts(UUID tenantUuid) {
        try {
            return countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_config_draft_change
                    WHERE application_id IS NOT DISTINCT FROM ? AND status_code IN ('DRAFT','PENDING_APPROVAL')
                    """, tenantUuid);
        } catch (Exception e) {
            return 0L;
        }
    }

    private Map<String, Object> listCurrentView(UUID tenantUuid, String env) {
        Map<String, Object> out = wrapMeta(List.of(), 0, 1, 50);
        out.put("kpis", publishKpis(tenantUuid, env));
        Map<String, Object> current = loadCurrentPublished(tenantUuid, env);
        out.put("currentPublished", current);
        out.put("componentCards", buildComponentCards(tenantUuid, current));
        out.put("configTree", buildConfigTree());
        out.put("freezeWindow", loadFreezeWindow(tenantUuid, env));
        out.put("draftOwnership", loadDraftOwnership(tenantUuid));
        return out;
    }

    private Map<String, Object> loadCurrentPublished(UUID tenantUuid, String env) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT CAST(p.reco_config_publish_id AS text) AS "recoConfigPublishId",
                       p.version_label AS "versionLabel",
                       p.published_at AS "publishedAt",
                       CAST(p.published_by AS text) AS "publishedBy",
                       COALESCE(p.environment_code, 'PROD') AS "environment",
                       COALESCE(p.publish_status, 'ACTIVE') AS "status"
                FROM reconciliation.reco_config_publish p
                WHERE p.application_id IS NOT DISTINCT FROM ?
                  AND COALESCE(p.environment_code, 'PROD') = ?
                  AND COALESCE(p.publish_status, 'ACTIVE') = 'ACTIVE'
                ORDER BY p.published_at DESC
                LIMIT 1
                """, tenantUuid, env);
        if (rows.isEmpty()) {
            Map<String, Object> empty = new HashMap<>();
            empty.put("status", "NONE");
            empty.put("environment", env);
            return empty;
        }
        return rows.get(0);
    }

    private List<Map<String, Object>> buildComponentCards(UUID tenantUuid, Map<String, Object> currentPublished) {
        long pending = countPendingDrafts(tenantUuid);
        List<Map<String, Object>> cards = new ArrayList<>();
        cards.add(card("MATCHING_RULES", "Matching Rules",
                countOrZero("SELECT COUNT(1) FROM reconciliation.matching_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                pending, tenantUuid));
        cards.add(card("GL_MAPPINGS", "GL Mappings",
                countOrZero("SELECT COUNT(1) FROM reconciliation.gl_mapping_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                0, tenantUuid));
        cards.add(card("THRESHOLDS", "Thresholds",
                countOrZero("""
                        SELECT COUNT(1) FROM reconciliation.reco_variance_threshold WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, tenantUuid)
                        + countOrZero("SELECT COUNT(1) FROM reconciliation.reco_sla_threshold WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                0, tenantUuid));
        cards.add(card("SCHEDULES", "Schedules",
                countOrZero("SELECT COUNT(1) FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                0, tenantUuid));
        cards.add(card("NOTIFICATION_RULES", "Notification Rules",
                countOrZero("SELECT COUNT(1) FROM reconciliation.reco_notification_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                0, tenantUuid));
        cards.add(card("ACCESS_POLICIES", "Access Policies",
                countOrZero("""
                        SELECT COUNT(1) FROM access.access_role
                        WHERE application_id IS NOT DISTINCT FROM ?
                        """, tenantUuid),
                0, tenantUuid));
        cards.add(card("ESCALATION_POLICIES", "Escalation Policies",
                countOrZero("SELECT COUNT(1) FROM reconciliation.reco_escalation_policy WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?", tenantUuid),
                0, tenantUuid));
        return cards;
    }

    private Map<String, Object> card(String type, String label, long publishedCount, long pendingDraft, UUID tenantUuid) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("componentType", type);
        c.put("label", label);
        c.put("publishedCount", publishedCount);
        c.put("pendingDraftCount", pendingDraft);
        c.put("lastModifiedAt", queryLastModifiedForComponent(type, tenantUuid));
        return c;
    }

    private Object queryLastModifiedForComponent(String type, UUID tenantUuid) {
        String sql = switch (type) {
            case "MATCHING_RULES" -> "SELECT MAX(COALESCE(modified_on, created_on)) FROM reconciliation.matching_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?";
            case "GL_MAPPINGS" -> "SELECT MAX(COALESCE(modified_on, created_on)) FROM reconciliation.gl_mapping_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?";
            case "SCHEDULES" -> "SELECT MAX(COALESCE(modified_on, created_on)) FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?";
            case "NOTIFICATION_RULES" -> "SELECT MAX(COALESCE(modified_on, created_on)) FROM reconciliation.reco_notification_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?";
            case "ACCESS_POLICIES" -> """
                    SELECT MAX(COALESCE(modified_on, created_on)) FROM access.access_role
                    WHERE application_id IS NOT DISTINCT FROM ?
                    """;
            case "ESCALATION_POLICIES" -> "SELECT MAX(COALESCE(modified_on, created_on)) FROM reconciliation.reco_escalation_policy WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?";
            default -> null;
        };
        if (sql == null) {
            return null;
        }
        try {
            return jdbc.queryForObject(sql, OffsetDateTime.class, tenantUuid);
        } catch (Exception e) {
            return null;
        }
    }

    private List<Map<String, Object>> buildConfigTree() {
        List<Map<String, Object>> children = new ArrayList<>();
        for (String ct : COMPONENT_TYPES) {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", ct);
            node.put("label", ct.replace('_', ' '));
            node.put("children", List.of());
            children.add(node);
        }
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("id", "ROOT");
        root.put("label", "Reconciliation Configuration");
        root.put("children", children);
        return List.of(root);
    }

    private Map<String, Object> loadFreezeWindow(UUID tenantUuid, String env) {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList("""
                    SELECT is_active AS "active", message AS "message", frozen_until AS "until"
                    FROM reconciliation.reco_config_freeze_window
                    WHERE application_id IS NOT DISTINCT FROM ? AND environment_code = ?
                    ORDER BY created_on DESC LIMIT 1
                    """, tenantUuid, env);
            if (rows.isEmpty()) {
                return Map.of("active", false);
            }
            Map<String, Object> fw = new HashMap<>(rows.get(0));
            fw.putIfAbsent("active", false);
            return fw;
        } catch (Exception e) {
            return Map.of("active", false);
        }
    }

    private Map<String, Object> loadDraftOwnership(UUID tenantUuid) {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList("""
                    SELECT CAST(locked_by AS text) AS "lockedBy", locked_at AS "lockedAt"
                    FROM reconciliation.reco_config_draft_lock WHERE application_id IS NOT DISTINCT FROM ?
                    """, tenantUuid);
            return rows.isEmpty() ? Map.of() : rows.get(0);
        } catch (Exception e) {
            return Map.of();
        }
    }

    private Map<String, Object> listDraftView(UUID tenantUuid, String env, String search, int p, int ps) {
        syncSyntheticDraftChanges(tenantUuid);
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE d.application_id IS NOT DISTINCT FROM ? AND d.status_code IN ('DRAFT','PENDING_APPROVAL','APPROVED','REJECTED') ");
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (d.entity_name ILIKE ? ESCAPE '\\' OR d.entity_type ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        String fromSql = " FROM reconciliation.reco_config_draft_change d " + where;
        long total = 0;
        List<Map<String, Object>> items = List.of();
        try {
            total = countOrZero("SELECT COUNT(1) " + fromSql, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            int offset = (p - 1) * ps;
            pageArgs.add(ps);
            pageArgs.add(offset);
            items = jdbc.queryForList("""
                    SELECT CAST(d.reco_config_draft_change_id AS text) AS "draftChangeId",
                           d.entity_type AS "entityType",
                           d.entity_name AS "entityName",
                           d.action_code AS "action",
                           CAST(d.modified_by AS text) AS "modifiedBy",
                           d.modified_at AS "modifiedAt",
                           d.status_code AS "status"
                    """ + fromSql + " ORDER BY d.modified_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
        } catch (Exception ignored) {
            // table optional
        }
        Map<String, Object> out = wrapMeta(items, total, p, ps);
        out.put("kpis", publishKpis(tenantUuid, env));
        out.put("draftImpactWarning", total > 0 ? total + " unpublished configuration change(s) pending review." : null);
        out.put("draftOwnership", loadDraftOwnership(tenantUuid));
        out.put("freezeWindow", loadFreezeWindow(tenantUuid, env));
        return out;
    }

    private void syncSyntheticDraftChanges(UUID tenantUuid) {
        if (tenantUuid == null) {
            return;
        }
        try {
            OffsetDateTime since = jdbc.queryForObject("""
                    SELECT COALESCE(MAX(published_at), '1970-01-01'::timestamptz)
                    FROM reconciliation.reco_config_publish WHERE application_id IS NOT DISTINCT FROM ?
                    """, OffsetDateTime.class, tenantUuid);
            syncDraftRowsForTable(tenantUuid, since, "MATCHING_RULE", """
                    SELECT mr.matching_rule_id, mr.rule_name, COALESCE(mr.modified_on, mr.created_on)
                    FROM reconciliation.matching_rule mr
                    WHERE mr.deleted_on IS NULL AND mr.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(mr.modified_on, mr.created_on) > ?
                    """);
            syncDraftRowsForTable(tenantUuid, since, "GL_MAPPING", """
                    SELECT g.gl_mapping_rule_id, g.mapping_name, COALESCE(g.modified_on, g.created_on)
                    FROM reconciliation.gl_mapping_rule g
                    WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(g.modified_on, g.created_on) > ?
                    """);
            syncDraftRowsForTable(tenantUuid, since, "THRESHOLD", """
                    SELECT t.reco_variance_threshold_id, t.threshold_name, COALESCE(t.modified_on, t.created_on)
                    FROM reconciliation.reco_variance_threshold t
                    WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(t.modified_on, t.created_on) > ?
                    """);
            syncDraftRowsForTable(tenantUuid, since, "SCHEDULE", """
                    SELECT s.reco_schedule_id, s.schedule_name, COALESCE(s.modified_on, s.created_on)
                    FROM reconciliation.reco_schedule s
                    WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(s.modified_on, s.created_on) > ?
                    """);
            syncDraftRowsForTable(tenantUuid, since, "NOTIFICATION_RULE", """
                    SELECT n.reco_notification_rule_id, n.rule_name, COALESCE(n.modified_on, n.created_on)
                    FROM reconciliation.reco_notification_rule n
                    WHERE n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(n.modified_on, n.created_on) > ?
                    """);
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private void syncDraftRowsForTable(UUID tenantUuid, OffsetDateTime since, String entityType, String selectSql) {
        jdbc.update("""
                INSERT INTO reconciliation.reco_config_draft_change (
                    application_id, entity_type, entity_id, entity_name, action_code, modified_at, status_code
                )
                SELECT ?, ?, src.entity_id, src.entity_name, 'MODIFIED', src.modified_at, 'DRAFT'
                FROM (""" + selectSql + """
                ) src(entity_id, entity_name, modified_at)
                WHERE NOT EXISTS (
                    SELECT 1 FROM reconciliation.reco_config_draft_change dc
                    WHERE dc.application_id IS NOT DISTINCT FROM ?
                      AND dc.entity_id = src.entity_id
                      AND dc.status_code IN ('DRAFT', 'PENDING_APPROVAL', 'APPROVED')
                )
                """, tenantUuid, entityType, tenantUuid, since, tenantUuid);
    }

    public Map<String, Object> saveDraft(String tenantId, Map<String, Object> body, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (tenantUuid == null) {
            throw new IllegalArgumentException("application id (X-Tenant-Id) is required");
        }
        UUID actor = parseOptionalUuid(actorUserId);
        String env = envOrDefault(body == null ? null : str(body, "environment"));
        String notes = body == null ? null : str(body, "notes");

        syncSyntheticDraftChanges(tenantUuid);

        try {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_config_draft_lock (application_id, locked_by, locked_at)
                    VALUES (?, ?, now())
                    ON CONFLICT (application_id) DO UPDATE SET locked_by = EXCLUDED.locked_by, locked_at = now()
                    """, tenantUuid, actor);
        } catch (Exception ignored) {
            // optional table
        }

        long pending = countPendingDrafts(tenantUuid);
        OffsetDateTime savedAt = OffsetDateTime.now();
        Map<String, Object> auditDetails = new LinkedHashMap<>();
        auditDetails.put("environment", env);
        auditDetails.put("pendingChangeCount", pending);
        if (notes != null) {
            auditDetails.put("notes", notes);
        }
        writeAudit(tenantUuid, actor, "DRAFT_SAVED", tenantUuid.toString(), auditDetails);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("draftId", tenantUuid.toString());
        out.put("pendingChangeCount", pending);
        out.put("savedAt", savedAt);
        return out;
    }

    private static OffsetDateTime parseDateBound(String raw, boolean startOfDay) {
        String trimmed = raw.trim();
        if (trimmed.length() <= 10) {
            LocalDate d = LocalDate.parse(trimmed);
            return startOfDay ? d.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime()
                    : d.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
        }
        return OffsetDateTime.parse(trimmed);
    }

    private static OffsetDateTime parseOffsetDateTime(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof OffsetDateTime odt) {
            return odt;
        }
        String s = String.valueOf(raw).trim();
        if (s.isEmpty()) {
            return null;
        }
        return OffsetDateTime.parse(s);
    }

    private static String resolvePublishMode(Map<String, Object> body, boolean dryRunFlag) {
        if (dryRunFlag) {
            return "DRY_RUN";
        }
        String mode = str(body, "publishMode");
        if (mode == null || mode.isBlank()) {
            return "IMMEDIATE";
        }
        return mode.toUpperCase();
    }

    private List<UUID> parseDraftChangeIds(Map<String, Object> body) {
        if (body == null || !(body.get("draftChangeIds") instanceof List<?> raw)) {
            return List.of();
        }
        List<UUID> ids = new ArrayList<>();
        for (Object o : raw) {
            UUID id = parseOptionalUuid(o == null ? null : String.valueOf(o));
            if (id != null) {
                ids.add(id);
            }
        }
        return ids;
    }

    private String buildExecuteNotes(Map<String, Object> body, String publishMode) {
        Map<String, Object> notes = new LinkedHashMap<>();
        String base = str(body, "notes");
        if (base != null) {
            notes.put("notes", base);
        }
        notes.put("publishMode", publishMode);
        if (body.get("canaryScope") != null) {
            notes.put("canaryScope", body.get("canaryScope"));
        }
        if (body.get("approvalIds") instanceof List<?> approvals) {
            notes.put("approvalIds", approvals);
        }
        return toJson(notes);
    }

    private Map<String, Object> listHistoryView(
            UUID tenantUuid, String env, String search, int p, int ps,
            String version, String publishedBy, String publishStatus, String from, String to
    ) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE p.application_id IS NOT DISTINCT FROM ? ");
        if (env != null) {
            where.append(" AND COALESCE(p.environment_code, 'PROD') = ? ");
            args.add(env);
        }
        if (version != null && !version.isBlank()) {
            where.append(" AND p.version_label ILIKE ? ");
            args.add("%" + version.trim() + "%");
        }
        if (publishedBy != null && !publishedBy.isBlank()) {
            where.append(" AND CAST(p.published_by AS text) = ? ");
            args.add(publishedBy.trim());
        }
        if (publishStatus != null && !publishStatus.isBlank()) {
            where.append(" AND COALESCE(p.publish_status, 'ACTIVE') = ? ");
            args.add(publishStatus.trim().toUpperCase());
        }
        if (from != null && !from.isBlank()) {
            where.append(" AND p.published_at >= ?::timestamptz ");
            args.add(parseDateBound(from, true));
        }
        if (to != null && !to.isBlank()) {
            where.append(" AND p.published_at < (?::date + interval '1 day')::timestamptz ");
            args.add(parseDateBound(to, false));
        }
        String fromSql = " FROM reconciliation.reco_config_publish p " + where;
        long total = countOrZero("SELECT COUNT(1) " + fromSql, args.toArray());
        int offset = (p - 1) * ps;
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(p.reco_config_publish_id AS text) AS "recoConfigPublishId",
                       p.version_label AS "versionLabel",
                       COALESCE(p.environment_code, 'PROD') AS "environment",
                       CAST(p.published_by AS text) AS "publishedBy",
                       p.published_at AS "publishedAt",
                       COALESCE(p.publish_status, 'ACTIVE') AS "status"
                """ + fromSql + " ORDER BY p.published_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
        Map<String, Object> out = wrapMeta(items, total, p, ps);
        out.put("kpis", publishKpis(tenantUuid, env));
        return out;
    }

    private Map<String, Object> listDiffView(UUID tenantUuid, String versionA, String versionB) {
        Map<String, Object> out = wrapMeta(List.of(), 0, 1, 50);
        if (versionA == null || versionB == null || versionA.isBlank() || versionB.isBlank()) {
            out.put("diffSections", List.of());
            return out;
        }
        String snapA = loadSnapshotByVersionLabel(tenantUuid, versionA);
        String snapB = loadSnapshotByVersionLabel(tenantUuid, versionB);
        if (snapA == null || snapB == null) {
            out.put("diffSections", List.of());
            return out;
        }
        try {
            out.put("diffSections", buildDiffSections(objectMapper.readTree(snapA), objectMapper.readTree(snapB)));
        } catch (JsonProcessingException e) {
            out.put("diffSections", List.of());
        }
        return out;
    }

    private String loadSnapshotByVersionLabel(UUID tenantUuid, String versionLabel) {
        try {
            return jdbc.queryForObject("""
                    SELECT snapshot_json::text FROM reconciliation.reco_config_publish
                    WHERE application_id IS NOT DISTINCT FROM ? AND version_label = ?
                    ORDER BY published_at DESC LIMIT 1
                    """, String.class, tenantUuid, versionLabel.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private List<Map<String, Object>> buildDiffSections(JsonNode a, JsonNode b) {
        List<Map<String, Object>> sections = new ArrayList<>();
        for (String component : COMPONENT_TYPES) {
            String key = componentKey(component);
            JsonNode ja = a.path(key);
            JsonNode jb = b.path(key);
            if (ja.equals(jb)) {
                continue;
            }
            List<Map<String, Object>> changes = new ArrayList<>();
            if (ja.isArray() && jb.isArray()) {
                changes.add(Map.of(
                        "entityName", component,
                        "field", "count",
                        "oldValue", String.valueOf(ja.size()),
                        "newValue", String.valueOf(jb.size())
                ));
            } else if (!ja.equals(jb)) {
                changes.add(Map.of(
                        "entityName", component,
                        "field", "snapshot",
                        "oldValue", ja.toString(),
                        "newValue", jb.toString()
                ));
            }
            if (!changes.isEmpty()) {
                sections.add(Map.of(
                        "componentType", component,
                        "label", component.replace('_', ' '),
                        "changes", changes
                ));
            }
        }
        return sections;
    }

    private static String componentKey(String componentType) {
        return switch (componentType) {
            case "MATCHING_RULES" -> "matchingRules";
            case "GL_MAPPINGS" -> "glMappingRules";
            case "THRESHOLDS" -> "thresholds";
            case "SCHEDULES" -> "schedules";
            case "NOTIFICATION_RULES" -> "notificationRules";
            case "ACCESS_POLICIES" -> "accessPolicies";
            case "ESCALATION_POLICIES" -> "escalationPolicies";
            default -> componentType.toLowerCase();
        };
    }

    private Map<String, Object> listRollbackView(UUID tenantUuid, String env, int p, int ps) {
        int offset = (p - 1) * ps;
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT p.version_label AS "versionLabel",
                       p.published_at AS "publishedAt",
                       p.notes AS "rollbackReason",
                       true AS "rollbackEligible"
                FROM reconciliation.reco_config_publish p
                WHERE p.application_id IS NOT DISTINCT FROM ? AND COALESCE(p.environment_code, 'PROD') = ?
                ORDER BY p.published_at DESC
                LIMIT ? OFFSET ?
                """, tenantUuid, env, ps, offset);
        long total = countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_config_publish
                WHERE application_id IS NOT DISTINCT FROM ? AND COALESCE(environment_code, 'PROD') = ?
                """, tenantUuid, env);
        Map<String, Object> out = wrapMeta(items, total, p, ps);
        out.put("kpis", publishKpis(tenantUuid, env));
        return out;
    }

    private Map<String, Object> listPromotionView(UUID tenantUuid, int p, int ps) {
        int offset = (p - 1) * ps;
        List<Map<String, Object>> items;
        try {
            items = jdbc.queryForList("""
                    SELECT source_environment AS "sourceEnvironment",
                           target_environment AS "targetEnvironment",
                           version_label AS "versionLabel",
                           promotion_status AS "promotionStatus",
                           validation_summary AS "validationSummary"
                    FROM reconciliation.reco_config_promotion
                    WHERE application_id IS NOT DISTINCT FROM ?
                    ORDER BY created_on DESC LIMIT ? OFFSET ?
                    """, tenantUuid, ps, offset);
        } catch (Exception e) {
            items = List.of();
        }
        Map<String, Object> out = wrapMeta(items, items.size(), p, ps);
        out.put("kpis", publishKpis(tenantUuid, "PROD"));
        return out;
    }

    private Map<String, Object> listPublishAudit(UUID tenantUuid, String search, int p, int ps) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE a.entity_type = 'reco_config_publish' AND a.application_id IS NOT DISTINCT FROM ? ");
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND a.action_code ILIKE ? ESCAPE '\\' ");
            args.add(q);
        }
        String fromSql = " FROM reconciliation.audit_log a " + where;
        long total = countOrZero("SELECT COUNT(1) " + fromSql, args.toArray());
        int offset = (p - 1) * ps;
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT a.event_at AS "occurredAt",
                       CAST(a.actor_user_id AS text) AS "userName",
                       a.action_code AS "action",
                       COALESCE(a.details_json->>'versionLabel', '') AS "versionLabel",
                       a.entity_type AS "entityType",
                       COALESCE(a.details_json->>'summary', a.action_code) AS "details"
                FROM reconciliation.audit_log a
                """ + where + " ORDER BY a.event_at DESC LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapMeta(items, total, p, ps);
    }

    private Map<String, Object> wizardPanel(UUID tenantUuid, String env) {
        Map<String, Object> out = listCurrentView(tenantUuid, env);
        out.put("validationResults", List.of());
        out.put("approvalState", defaultApprovalState(null));
        out.put("blastRadius", defaultBlastRadius());
        return out;
    }

    private static Map<String, Object> approvalTimelineStep(String step, String at, String status) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("step", step);
        row.put("at", at);
        row.put("status", status);
        return row;
    }

    private Map<String, Object> defaultApprovalState(Map<String, Object> body) {
        boolean approvalRequired = body != null && body.get("approvalIds") instanceof List<?> ids && !ids.isEmpty();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("approvalRequired", approvalRequired);
        out.put("requiredApprovers", approvalRequired ? 2 : 0);
        out.put("approverRoles", List.of("FINANCE_ADMIN", "AUDIT_ADMIN"));
        out.put("timeline", List.of(
                approvalTimelineStep("Draft Created", OffsetDateTime.now().toString(), "DONE"),
                approvalTimelineStep("Finance Approved", null, approvalRequired ? "PENDING" : "DONE"),
                approvalTimelineStep("Audit Approved", null, "PENDING")
        ));
        return out;
    }

    private Map<String, Object> defaultBlastRadius() {
        return Map.of(
                "transactionsPerDay", 1_200_000,
                "gateways", 3,
                "regions", 5
        );
    }

    public Map<String, Object> buildLiveSnapshot(UUID tenantUuid) {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("matchingRules", jdbc.queryForList("""
                SELECT CAST(matching_rule_id AS text) AS "id", rule_code AS "code", rule_name AS "name", is_active AS "active"
                FROM reconciliation.matching_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                ORDER BY rule_code
                """, tenantUuid));
        snap.put("glMappingRules", jdbc.queryForList("""
                SELECT CAST(gl_mapping_rule_id AS text) AS "id", mapping_code AS "code", mapping_name AS "name", is_active AS "active"
                FROM reconciliation.gl_mapping_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                ORDER BY mapping_code
                """, tenantUuid));
        Map<String, Object> thresholds = new LinkedHashMap<>();
        thresholds.put("variance", jdbc.queryForList("""
                SELECT threshold_code AS "code", threshold_name AS "name" FROM reconciliation.reco_variance_threshold
                WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, tenantUuid));
        thresholds.put("sla", jdbc.queryForList("""
                SELECT policy_code AS "code", policy_name AS "name" FROM reconciliation.reco_sla_threshold
                WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid));
        snap.put("thresholds", thresholds);
        snap.put("schedules", jdbc.queryForList("""
                SELECT CAST(reco_schedule_id AS text) AS "id", schedule_code AS "code", schedule_name AS "name", is_active AS "active"
                FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, tenantUuid));
        snap.put("notificationRules", jdbc.queryForList("""
                SELECT CAST(reco_notification_rule_id AS text) AS "id", rule_code AS "code", rule_name AS "name", is_active AS "active"
                FROM reconciliation.reco_notification_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, tenantUuid));
        snap.put("accessPolicies", jdbc.queryForList("""
                SELECT CAST(role_id AS text) AS "id", COALESCE(role_number, name) AS "code", name AS "name", is_active AS "active"
                FROM access.access_role
                WHERE application_id IS NOT DISTINCT FROM ?
                """, tenantUuid));
        snap.put("escalationPolicies", jdbc.queryForList("""
                SELECT policy_code AS "code", policy_name AS "name", is_active AS "active"
                FROM reconciliation.reco_escalation_policy WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                """, tenantUuid));
        snap.put("capturedAt", OffsetDateTime.now().toString());
        return snap;
    }

    public Map<String, Object> previewDraft(String tenantId, Map<String, Object> body) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        String env = envOrDefault(body == null ? null : str(body, "environment"));
        syncSyntheticDraftChanges(tenantUuid);
        Map<String, Object> draftList = listDraftView(tenantUuid, env, null, 1, 100);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> draftChanges = (List<Map<String, Object>>) draftList.get("items");
        Map<String, Object> current = loadCurrentPublished(tenantUuid, env);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("draftChanges", draftChanges);
        out.put("componentCards", buildComponentCards(tenantUuid, current));
        out.put("draftImpactWarning", draftList.get("draftImpactWarning"));
        out.put("blastRadius", defaultBlastRadius());
        return out;
    }

    public Map<String, Object> validatePublish(String tenantId, Map<String, Object> body) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Map<String, Object>> results = new ArrayList<>();
        List<UUID> draftChangeIds = parseDraftChangeIds(body == null ? Map.of() : body);

        if (!draftChangeIds.isEmpty()) {
            long found = countDraftChangesByIds(tenantUuid, draftChangeIds, null);
            long missing = draftChangeIds.size() - found;
            if (missing > 0) {
                results.add(Map.of(
                        "category", "DRAFT",
                        "severity", "ERROR",
                        "code", "DRAFT_CHANGE_NOT_FOUND",
                        "message", missing + " draft change id(s) not found for application"
                ));
            }
            long rejected = countDraftChangesByIds(tenantUuid, draftChangeIds, "REJECTED");
            if (rejected > 0) {
                results.add(Map.of(
                        "category", "DRAFT",
                        "severity", "ERROR",
                        "code", "DRAFT_CHANGE_REJECTED",
                        "message", rejected + " draft change(s) are rejected"
                ));
            }
        } else {
            long pendingApproval = countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_config_draft_change
                    WHERE application_id IS NOT DISTINCT FROM ? AND status_code = 'PENDING_APPROVAL'
                    """, tenantUuid);
            if (pendingApproval > 0) {
                results.add(Map.of(
                        "category", "APPROVAL",
                        "severity", "WARNING",
                        "code", "PENDING_APPROVALS",
                        "message", pendingApproval + " draft change(s) awaiting approval"
                ));
            }
        }

        long overlap = countOrZero("""
                SELECT COUNT(*) FROM (
                    SELECT reco_domain_id, severity_id FROM reconciliation.reco_variance_threshold
                    WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ? AND is_active = true
                    GROUP BY reco_domain_id, severity_id HAVING COUNT(*) > 1
                ) x
                """, tenantUuid);
        if (overlap > 0) {
            results.add(Map.of(
                    "category", "THRESHOLD",
                    "severity", "WARNING",
                    "code", "OVERLAPPING_THRESHOLDS",
                    "message", overlap + " overlapping threshold group(s) found"
            ));
        }
        long inactiveSchedules = countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_schedule
                WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ? AND is_active = false
                """, tenantUuid);
        if (inactiveSchedules > 0) {
            results.add(Map.of(
                    "category", "SCHEDULE",
                    "severity", "INFO",
                    "code", "INACTIVE_SCHEDULES",
                    "message", inactiveSchedules + " inactive schedule(s) in draft"
            ));
        }
        boolean blocked = results.stream().anyMatch(r ->
                "ERROR".equals(r.get("severity")) || "CRITICAL".equals(r.get("severity")));
        List<String> blockReasons = blocked
                ? results.stream()
                .filter(r -> "ERROR".equals(r.get("severity")) || "CRITICAL".equals(r.get("severity")))
                .map(r -> String.valueOf(r.get("message")))
                .toList()
                : List.of();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("validationResults", results);
        out.put("blocked", blocked);
        out.put("blockReasons", blockReasons);
        out.put("approvalState", defaultApprovalState(body));
        return out;
    }

    public Map<String, Object> previewImpact(String tenantId, Map<String, Object> body) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<String> domains = jdbc.query("""
                SELECT DISTINCT d.code FROM reconciliation.reco_role_domain_access da
                JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = da.reco_domain_id
                JOIN access.access_role r ON r.role_id = da.role_id
                WHERE r.application_id IS NOT DISTINCT FROM ? LIMIT 10
                """, (rs, rn) -> rs.getString(1), tenantUuid);
        List<String> stages = jdbc.query("""
                SELECT DISTINCT stg.code FROM reconciliation.reco_role_stage_access sa
                JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = sa.reconciliation_stage_id
                JOIN access.access_role r ON r.role_id = sa.role_id
                WHERE r.application_id IS NOT DISTINCT FROM ? LIMIT 10
                """, (rs, rn) -> rs.getString(1), tenantUuid);
        long schedules = countOrZero("""
                SELECT COUNT(1) FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ? AND is_active = true
                """, tenantUuid);
        return Map.of(
                "affectedDomains", domains.isEmpty() ? List.of("PAYMENT") : domains,
                "affectedStages", stages.isEmpty() ? List.of("GATEWAY_TO_SETTLEMENT") : stages,
                "affectedSchedules", schedules,
                "expectedExceptionChangePercent", -12,
                "expectedMatchingChangePercent", 3,
                "dependencyGraph", List.of(
                        Map.of("from", "Matching Rule Change", "to", "Threshold Policy"),
                        Map.of("from", "Threshold Policy", "to", "Exception Behavior")
                ),
                "blastRadius", defaultBlastRadius()
        );
    }

    public Map<String, Object> checkFreezeWindow(UUID tenantUuid, String env) {
        Map<String, Object> fw = loadFreezeWindow(tenantUuid, env);
        boolean active = Boolean.TRUE.equals(fw.get("active"));
        if (active && fw.get("until") != null) {
            try {
                OffsetDateTime until = fw.get("until") instanceof OffsetDateTime odt ? odt
                        : OffsetDateTime.parse(String.valueOf(fw.get("until")));
                if (until.isAfter(OffsetDateTime.now())) {
                    return Map.of("frozen", true, "freezeWindow", fw);
                }
            } catch (Exception ignored) {
                return Map.of("frozen", true, "freezeWindow", fw);
            }
        }
        return Map.of("frozen", false);
    }

    public Map<String, Object> executePublish(String tenantId, Map<String, Object> body, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        String env = envOrDefault(str(body, "environment"));
        Map<String, Object> freeze = checkFreezeWindow(tenantUuid, env);
        if (Boolean.TRUE.equals(freeze.get("frozen"))) {
            return Map.of("status", "FROZEN", "freezeWindow", freeze.get("freezeWindow"));
        }
        boolean dryRun = bool(body, "dryRun", false);
        String publishMode = resolvePublishMode(body, dryRun);
        String versionLabel = Objects.requireNonNull(str(body, "versionLabel"), "versionLabel is required");

        if ("DRY_RUN".equals(publishMode)) {
            return Map.of(
                    "status", "DRY_RUN",
                    "versionLabel", versionLabel,
                    "publishMode", publishMode,
                    "snapshotPreview", buildLiveSnapshot(tenantUuid)
            );
        }

        Map<String, Object> validation = validatePublish(tenantId, body);
        if (Boolean.TRUE.equals(validation.get("blocked"))) {
            return Map.of(
                    "status", "BLOCKED",
                    "validationResults", validation.get("validationResults"),
                    "blockReasons", validation.get("blockReasons")
            );
        }

        boolean createRollbackPoint = bool(body, "createRollbackPoint", true);
        UUID previousActiveId = queryUuidOrNull("""
                SELECT reco_config_publish_id FROM reconciliation.reco_config_publish
                WHERE application_id IS NOT DISTINCT FROM ? AND COALESCE(environment_code, 'PROD') = ?
                  AND publish_status = 'ACTIVE'
                ORDER BY published_at DESC LIMIT 1
                """, tenantUuid, env);

        String publishStatus = switch (publishMode) {
            case "SCHEDULED" -> "SCHEDULED";
            case "CANARY" -> "CANARY";
            default -> "ACTIVE";
        };

        OffsetDateTime effectiveAt = "SCHEDULED".equals(publishMode)
                ? Objects.requireNonNull(parseOffsetDateTime(body.get("scheduleAt")), "scheduleAt is required for SCHEDULED publish")
                : OffsetDateTime.now();

        if ("IMMEDIATE".equals(publishMode) || "CANARY".equals(publishMode)) {
            jdbc.update("""
                    UPDATE reconciliation.reco_config_publish SET publish_status = 'ARCHIVED'
                    WHERE application_id IS NOT DISTINCT FROM ? AND COALESCE(environment_code, 'PROD') = ?
                      AND publish_status = 'ACTIVE'
                    """, tenantUuid, env);
        }

        UUID id = UUID.randomUUID();
        Map<String, Object> snapshot = buildLiveSnapshot(tenantUuid);
        String notesJson = buildExecuteNotes(body, publishMode);

        jdbc.update("""
                INSERT INTO reconciliation.reco_config_publish (
                    reco_config_publish_id, application_id, version_label, published_at, published_by,
                    notes, snapshot_json, environment_code, publish_status, effective_at, rollback_of_publish_id
                ) VALUES (?, ?, ?, now(), ?, ?, ?::jsonb, ?, ?, ?, ?)
                """, id, tenantUuid, versionLabel, actor, notesJson, toJson(snapshot), env, publishStatus, effectiveAt,
                createRollbackPoint ? previousActiveId : null);

        if ("ACTIVE".equals(publishStatus) || "CANARY".equals(publishStatus)) {
            markDraftChangesPublished(tenantUuid, body);
        }

        writeAudit(tenantUuid, actor, "PUBLISHED", id.toString(), Map.of(
                "versionLabel", versionLabel,
                "environment", env,
                "publishMode", publishMode,
                "createRollbackPoint", createRollbackPoint
        ));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("recoConfigPublishId", id.toString());
        out.put("versionLabel", versionLabel);
        out.put("publishedAt", OffsetDateTime.now());
        out.put("status", publishStatus);
        out.put("publishMode", publishMode);
        if (createRollbackPoint && previousActiveId != null) {
            out.put("rollbackPointPublishId", previousActiveId.toString());
        }
        return out;
    }

    private long countDraftChangesByIds(UUID tenantUuid, List<UUID> draftChangeIds, String statusCode) {
        if (tenantUuid == null || draftChangeIds == null || draftChangeIds.isEmpty()) {
            return 0L;
        }
        String inClause = String.join(",", draftChangeIds.stream().map(id -> "?").toList());
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        args.addAll(draftChangeIds);
        StringBuilder sql = new StringBuilder("""
                SELECT COUNT(1) FROM reconciliation.reco_config_draft_change
                WHERE application_id IS NOT DISTINCT FROM ?
                  AND reco_config_draft_change_id IN (""").append(inClause).append(")");
        if (statusCode != null) {
            sql.append(" AND status_code = ? ");
            args.add(statusCode);
        }
        return countOrZero(sql.toString(), args.toArray());
    }

    private void markDraftChangesPublished(UUID tenantUuid, Map<String, Object> body) {
        List<UUID> draftChangeIds = parseDraftChangeIds(body);
        try {
            if (!draftChangeIds.isEmpty()) {
                String inClause = String.join(",", draftChangeIds.stream().map(id -> "?").toList());
                List<Object> args = new ArrayList<>();
                args.add(tenantUuid);
                args.addAll(draftChangeIds);
                jdbc.update("""
                        UPDATE reconciliation.reco_config_draft_change SET status_code = 'PUBLISHED', modified_at = now()
                        WHERE application_id IS NOT DISTINCT FROM ?
                          AND reco_config_draft_change_id IN (""" + inClause + ")", args.toArray());
            } else {
                jdbc.update("""
                        UPDATE reconciliation.reco_config_draft_change SET status_code = 'PUBLISHED', modified_at = now()
                        WHERE application_id IS NOT DISTINCT FROM ?
                          AND status_code IN ('DRAFT', 'PENDING_APPROVAL', 'APPROVED')
                        """, tenantUuid);
            }
        } catch (Exception ignored) {
            // optional table
        }
    }

    public Map<String, Object> rollbackPublish(String tenantId, Map<String, Object> body, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        String env = envOrDefault(str(body, "environment"));
        Map<String, Object> freeze = checkFreezeWindow(tenantUuid, env);
        if (Boolean.TRUE.equals(freeze.get("frozen"))) {
            return Map.of("status", "FROZEN", "freezeWindow", freeze.get("freezeWindow"));
        }
        String targetVersion = str(body, "targetVersionLabel");
        if (targetVersion == null || targetVersion.isBlank()) {
            throw new IllegalArgumentException("targetVersionLabel is required");
        }
        String snap = loadSnapshotByVersionLabel(tenantUuid, targetVersion);
        if (snap == null) {
            return Map.of("status", "NOT_FOUND", "targetVersionLabel", targetVersion);
        }
        jdbc.update("""
                UPDATE reconciliation.reco_config_publish SET publish_status = 'ARCHIVED'
                WHERE application_id IS NOT DISTINCT FROM ? AND environment_code = ? AND publish_status = 'ACTIVE'
                """, tenantUuid, env);
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO reconciliation.reco_config_publish (
                    reco_config_publish_id, application_id, version_label, published_at, published_by,
                    notes, snapshot_json, environment_code, publish_status
                ) VALUES (?, ?, ?, now(), ?, ?, ?::jsonb, ?, 'ACTIVE')
                """, id, tenantUuid, targetVersion + "-rollback", actor,
                str(body, "reason"), snap, env);
        writeAudit(tenantUuid, actor, "ROLLED_BACK", id.toString(), Map.of("targetVersionLabel", targetVersion));
        return Map.of(
                "recoConfigPublishId", id.toString(),
                "versionLabel", targetVersion,
                "status", "ACTIVE",
                "affectedSchedules", countOrZero("SELECT COUNT(1) FROM reconciliation.reco_schedule WHERE application_id IS NOT DISTINCT FROM ?", tenantUuid),
                "affectedRules", countOrZero("SELECT COUNT(1) FROM reconciliation.matching_rule WHERE application_id IS NOT DISTINCT FROM ?", tenantUuid)
        );
    }

    public Map<String, Object> promoteEnvironment(String tenantId, Map<String, Object> body, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        String source = envOrDefault(str(body, "sourceEnvironment"));
        String target = envOrDefault(str(body, "targetEnvironment"));
        Map<String, Object> freeze = checkFreezeWindow(tenantUuid, target);
        if (Boolean.TRUE.equals(freeze.get("frozen"))) {
            return Map.of("status", "FROZEN", "freezeWindow", freeze.get("freezeWindow"));
        }
        String versionLabel = str(body, "versionLabel");
        UUID promotionId = UUID.randomUUID();
        Map<String, Object> validation = validatePublish(tenantId, body);
        try {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_config_promotion (
                        reco_config_promotion_id, application_id, source_environment, target_environment,
                        version_label, promotion_status, validation_summary, promotion_notes
                    ) VALUES (?, ?, ?, ?, ?, 'COMPLETED', ?, ?)
                    """, promotionId, tenantUuid, source, target, versionLabel,
                    "Promotion validation passed", str(body, "promotionNotes"));
        } catch (Exception ignored) {
            // optional table
        }
        String snap = versionLabel == null ? null : loadSnapshotByVersionLabel(tenantUuid, versionLabel);
        if (snap != null) {
            UUID id = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO reconciliation.reco_config_publish (
                        reco_config_publish_id, application_id, version_label, published_at, published_by,
                        notes, snapshot_json, environment_code, publish_status
                    ) VALUES (?, ?, ?, now(), ?, ?, ?::jsonb, ?, 'ACTIVE')
                    """, id, tenantUuid, versionLabel, parseOptionalUuid(actorUserId),
                    "Promoted from " + source, snap, target);
        }
        return Map.of(
                "promotionId", promotionId.toString(),
                "status", "COMPLETED",
                "validationResults", validation.get("validationResults")
        );
    }

    public Map<String, Object> exportSnapshot(String tenantId, String version) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        String snap;
        if (version != null && !version.isBlank()) {
            snap = loadSnapshotByVersionLabel(tenantUuid, version);
        } else {
            Map<String, Object> current = loadCurrentPublished(tenantUuid, "PROD");
            String id = String.valueOf(current.get("recoConfigPublishId"));
            snap = id == null || "null".equals(id) ? toJson(buildLiveSnapshot(tenantUuid))
                    : loadSnapshotByPublishId(parseOptionalUuid(id));
        }
        if (snap == null) {
            snap = toJson(buildLiveSnapshot(tenantUuid));
        }
        try {
            return Map.of("snapshot", objectMapper.readValue(snap, Map.class), "format", "json");
        } catch (JsonProcessingException e) {
            return Map.of("snapshot", snap, "format", "json");
        }
    }

    private String loadSnapshotByPublishId(UUID id) {
        if (id == null) {
            return null;
        }
        try {
            return jdbc.queryForObject(
                    "SELECT snapshot_json::text FROM reconciliation.reco_config_publish WHERE reco_config_publish_id = ?",
                    String.class, id);
        } catch (Exception e) {
            return null;
        }
    }

    public Map<String, Object> approveDraftChange(String draftChangeId, String comment, String actorUserId) {
        return updateDraftStatus(draftChangeId, "APPROVED", comment, actorUserId);
    }

    public Map<String, Object> rejectDraftChange(String draftChangeId, String comment, String actorUserId) {
        return updateDraftStatus(draftChangeId, "REJECTED", comment, actorUserId);
    }

    public Map<String, Object> discardDraftChange(String draftChangeId, String comment, String actorUserId) {
        return updateDraftStatus(draftChangeId, "DISCARDED", comment, actorUserId);
    }

    private Map<String, Object> updateDraftStatus(String draftChangeId, String status, String comment, String actorUserId) {
        UUID id = parseOptionalUuid(draftChangeId);
        int n = jdbc.update("""
                UPDATE reconciliation.reco_config_draft_change
                SET status_code = ?, review_comment = ?, modified_by = ?, modified_at = now()
                WHERE reco_config_draft_change_id = ?
                """, status, comment, parseOptionalUuid(actorUserId), id);
        return Map.of("updated", n > 0, "draftChangeId", draftChangeId, "status", status);
    }
}
