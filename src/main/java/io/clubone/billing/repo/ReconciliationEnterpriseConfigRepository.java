package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Relational enterprise reconciliation configuration (see docs/ddl/reconciliation_enterprise_config_v1.sql).
 */
@Repository
public class ReconciliationEnterpriseConfigRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationEnterpriseConfigRepository(
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

    private static BigDecimal dec(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            return null;
        }
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        try {
            return new BigDecimal(String.valueOf(v).trim());
        } catch (Exception e) {
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

    private void writeAudit(UUID tenantId, UUID actorId, String action, String entityType, String entityRef, Object details) {
        try {
            jdbc.update("""
                    INSERT INTO reconciliation.audit_log (tenant_id, actor_user_id, action_code, entity_type, entity_reference_code, details_json)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    """, tenantId, actorId, action, entityType, entityRef, toJson(details));
        } catch (Exception ignored) {
            // best-effort audit
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

    private UUID operatorIdByCode(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("criterion.operator is required");
        }
        UUID id = queryUuidOrNull("SELECT matching_operator_id FROM reconciliation.lu_matching_operator WHERE code = ? LIMIT 1", code.trim().toUpperCase());
        if (id == null) {
            throw new IllegalArgumentException("Unknown operator: " + code);
        }
        return id;
    }

    private UUID matchKindIdByCode(String code) {
        String c = code == null || code.isBlank() ? "EXACT" : code.trim().toUpperCase();
        UUID id = queryUuidOrNull("SELECT match_kind_id FROM reconciliation.lu_match_kind WHERE code = ? LIMIT 1", c);
        return id == null ? queryUuidOrNull("SELECT match_kind_id FROM reconciliation.lu_match_kind WHERE code = 'EXACT' LIMIT 1") : id;
    }

    private UUID toleranceKindIdByCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull("SELECT tolerance_kind_id FROM reconciliation.lu_tolerance_kind WHERE code = ? LIMIT 1", code.trim().toUpperCase());
    }

    private UUID severityIdByCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull("SELECT severity_id FROM reconciliation.lu_severity WHERE UPPER(code) = UPPER(?) LIMIT 1", code.trim());
    }

    private UUID scheduleFrequencyIdByCode(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("schedule.frequency is required");
        }
        UUID id = queryUuidOrNull("SELECT schedule_frequency_id FROM reconciliation.lu_schedule_frequency WHERE code = ? LIMIT 1", code.trim().toUpperCase());
        if (id == null) {
            throw new IllegalArgumentException("Unknown frequency: " + code);
        }
        return id;
    }

    private UUID sourceSystemIdByCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull("SELECT source_system_id FROM reconciliation.lu_source_system WHERE code = ? LIMIT 1", code.trim().toUpperCase());
    }

    public Map<String, Object> getEnterpriseConfigLookups() {
        Map<String, Object> out = new HashMap<>();
        out.put("recoDomains", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_reco_domain WHERE is_active = true ORDER BY code
                """));
        out.put("matchingOperators", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_matching_operator WHERE is_active = true ORDER BY code
                """));
        out.put("matchKinds", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_match_kind WHERE is_active = true ORDER BY code
                """));
        out.put("toleranceKinds", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_tolerance_kind WHERE is_active = true ORDER BY code
                """));
        out.put("scheduleFrequencies", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_schedule_frequency WHERE is_active = true ORDER BY code
                """));
        out.put("notificationEvents", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_notification_event WHERE is_active = true ORDER BY code
                """));
        out.put("recoPermissions", jdbc.queryForList("""
                SELECT permission_code AS "code", display_name AS "displayName", module_code AS "moduleCode", is_active AS "active"
                FROM reconciliation.reco_permission WHERE is_active = true ORDER BY module_code, permission_code
                """));
        return out;
    }

    private void appendSearchMatching(StringBuilder where, List<Object> args, String search) {
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (mr.rule_name ILIKE ? ESCAPE '\\' OR mr.rule_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
    }

    private void appendLocationFilter(StringBuilder where, List<Object> args, String locationId) {
        UUID loc = parseOptionalUuid(locationId);
        if (loc == null) {
            return;
        }
        where.append("""
                 AND (
                    NOT EXISTS (SELECT 1 FROM reconciliation.matching_rule_scope sc0 WHERE sc0.matching_rule_id = mr.matching_rule_id)
                    OR EXISTS (
                        SELECT 1 FROM reconciliation.matching_rule_scope sc2
                        WHERE sc2.matching_rule_id = mr.matching_rule_id
                          AND (sc2.location_id IS NULL OR sc2.location_id = ?)
                    )
                ) """);
        args.add(loc);
    }

    public Map<String, Object> listEnterpriseMatchingRules(
            String tenantId,
            String recoDomainCode,
            String stageCode,
            Boolean activeOnly,
            String search,
            String locationId,
            int page,
            int pageSize
    ) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE mr.deleted_on IS NULL ");
        where.append(" AND mr.tenant_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND mr.is_active = true ");
        }
        if (recoDomainCode != null && !recoDomainCode.isBlank()) {
            where.append(" AND d.code = ? ");
            args.add(recoDomainCode.trim());
        }
        if (stageCode != null && !stageCode.isBlank()) {
            where.append(" AND stg.code = ? ");
            args.add(stageCode.trim());
        }
        appendSearchMatching(where, args, search);
        appendLocationFilter(where, args, locationId);
        String fromSql = """
                FROM reconciliation.matching_rule mr
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = mr.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = mr.reconciliation_stage_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    CAST(mr.matching_rule_id AS text) AS "matchingRuleId",
                    mr.rule_name AS "ruleName",
                    mr.rule_code AS "ruleCode",
                    d.code AS "recoDomain",
                    stg.code AS "stage",
                    mr.priority AS "priority",
                    mr.is_active AS "active",
                    mr.effective_from::text AS "effectiveFrom",
                    mr.effective_to::text AS "effectiveTo",
                    mr.created_on AS "createdAt",
                    mr.modified_on AS "updatedAt"
                """ + fromSql + """
                ORDER BY mr.priority ASC, mr.rule_code ASC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        return out;
    }

    public Map<String, Object> getEnterpriseMatchingRuleDetail(String matchingRuleId) {
        UUID rulePk = parseOptionalUuid(matchingRuleId);
        if (rulePk == null) {
            return Map.of("status", "NOT_FOUND", "matchingRule", null);
        }
        List<Map<String, Object>> rules = jdbc.queryForList("""
                SELECT
                    CAST(mr.matching_rule_id AS text) AS "matchingRuleId",
                    mr.rule_name AS "ruleName",
                    mr.rule_code AS "ruleCode",
                    CAST(mr.tenant_id AS text) AS "tenantId",
                    d.code AS "recoDomain",
                    stg.code AS "stage",
                    mr.priority AS "priority",
                    mr.is_active AS "active",
                    mr.effective_from::text AS "effectiveFrom",
                    mr.effective_to::text AS "effectiveTo",
                    mr.notes AS "notes",
                    mr.created_on AS "createdAt",
                    mr.modified_on AS "updatedAt"
                FROM reconciliation.matching_rule mr
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = mr.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = mr.reconciliation_stage_id
                WHERE mr.matching_rule_id = ? AND mr.deleted_on IS NULL
                """, rulePk);
        if (rules.isEmpty()) {
            return Map.of("status", "NOT_FOUND", "matchingRule", null);
        }
        List<Map<String, Object>> criteria = jdbc.queryForList("""
                SELECT
                    CAST(c.matching_rule_criterion_id AS text) AS "criterionId",
                    c.line_order AS "lineOrder",
                    c.left_operand AS "leftOperand",
                    mo.code AS "operator",
                    c.right_operand AS "rightOperand",
                    c.tolerance_amount AS "toleranceAmount",
                    tk.code AS "toleranceKind",
                    mk.code AS "matchKind",
                    c.is_required AS "required",
                    c.extra_json AS "extra"
                FROM reconciliation.matching_rule_criterion c
                JOIN reconciliation.lu_matching_operator mo ON mo.matching_operator_id = c.matching_operator_id
                JOIN reconciliation.lu_match_kind mk ON mk.match_kind_id = c.match_kind_id
                LEFT JOIN reconciliation.lu_tolerance_kind tk ON tk.tolerance_kind_id = c.tolerance_kind_id
                WHERE c.matching_rule_id = ?
                ORDER BY c.line_order
                """, rulePk);
        List<Map<String, Object>> scopes = jdbc.queryForList("""
                SELECT
                    CAST(s.matching_rule_scope_id AS text) AS "scopeId",
                    CAST(s.location_id AS text) AS "locationId",
                    CAST(s.location_level_id AS text) AS "locationLevelId",
                    s.include_child_locations AS "includeChildLocations",
                    s.gateway_code AS "gatewayCode",
                    s.payment_method_code AS "paymentMethodCode",
                    s.currency_code AS "currencyCode",
                    ss.code AS "sourceSystem",
                    s.scope_json AS "scopeJson"
                FROM reconciliation.matching_rule_scope s
                LEFT JOIN reconciliation.lu_source_system ss ON ss.source_system_id = s.source_system_id
                WHERE s.matching_rule_id = ?
                """, rulePk);
        List<Map<String, Object>> outcomes = jdbc.queryForList("""
                SELECT
                    CAST(o.matching_rule_outcome_id AS text) AS "outcomeId",
                    o.auto_resolve AS "autoResolve",
                    o.create_exception AS "createException",
                    sev.code AS "failureSeverity",
                    o.retry_eligible AS "retryEligible",
                    o.outcome_json AS "outcomeJson"
                FROM reconciliation.matching_rule_outcome o
                LEFT JOIN reconciliation.lu_severity sev ON sev.severity_id = o.failure_severity_id
                WHERE o.matching_rule_id = ?
                """, rulePk);
        Map<String, Object> body = new HashMap<>();
        body.put("matchingRule", rules.get(0));
        body.put("criteria", criteria);
        body.put("scopes", scopes);
        body.put("outcomes", outcomes);
        body.put("status", "OK");
        return body;
    }

    private void appendSearchGl(StringBuilder where, List<Object> args, String search) {
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (g.mapping_name ILIKE ? ESCAPE '\\' OR g.mapping_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
    }

    public Map<String, Object> listEnterpriseGlMappingRules(
            String tenantId,
            Boolean activeOnly,
            String search,
            String locationId,
            int page,
            int pageSize
    ) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE g.deleted_on IS NULL AND g.tenant_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND g.is_active = true ");
        }
        appendSearchGl(where, args, search);
        if (parseOptionalUuid(locationId) != null) {
            where.append(" AND (g.dimensions_json->>'locationId' = ? OR g.dimensions_json->>'location_id' = ?) ");
            args.add(locationId.trim());
            args.add(locationId.trim());
        }
        String fromSql = " FROM reconciliation.gl_mapping_rule g " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    CAST(g.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    g.mapping_name AS "mappingName",
                    g.mapping_code AS "mappingCode",
                    g.is_active AS "active",
                    g.debit_gl_account_code AS "debitGlAccountCode",
                    g.credit_gl_account_code AS "creditGlAccountCode",
                    g.currency_code AS "currencyCode",
                    g.match_rank AS "matchRank",
                    g.effective_from::text AS "effectiveFrom",
                    g.effective_to::text AS "effectiveTo"
                """ + fromSql + """
                ORDER BY g.match_rank ASC, g.mapping_code ASC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        return out;
    }

    private void appendSearchSchedule(StringBuilder where, List<Object> args, String search) {
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (s.schedule_name ILIKE ? ESCAPE '\\' OR s.schedule_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
    }

    public Map<String, Object> listEnterpriseSchedules(
            String tenantId,
            Boolean activeOnly,
            String search,
            String locationId,
            int page,
            int pageSize
    ) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID loc = parseOptionalUuid(locationId);
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE s.deleted_on IS NULL AND s.tenant_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND s.is_active = true ");
        }
        appendSearchSchedule(where, args, search);
        if (loc != null) {
            where.append(" AND (s.location_id IS NULL OR s.location_id = ?) ");
            args.add(loc);
        }
        String fromSql = """
                FROM reconciliation.reco_schedule s
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = s.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = s.reconciliation_stage_id
                JOIN reconciliation.lu_schedule_frequency sf ON sf.schedule_frequency_id = s.schedule_frequency_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    CAST(s.reco_schedule_id AS text) AS "scheduleId",
                    s.schedule_name AS "scheduleName",
                    s.schedule_code AS "scheduleCode",
                    d.code AS "recoDomain",
                    stg.code AS "stage",
                    s.is_active AS "active",
                    sf.code AS "frequency",
                    s.interval_n AS "interval",
                    s.cron_expression AS "cronExpression",
                    s.timezone AS "timezone",
                    CAST(s.location_id AS text) AS "locationId",
                    s.allow_parallel_runs AS "allowParallelRuns",
                    s.max_runtime_minutes AS "maxRuntimeMinutes",
                    s.retry_on_failure AS "retryOnFailure",
                    s.retry_count AS "retryCount"
                """ + fromSql + """
                ORDER BY s.schedule_code ASC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        return out;
    }

    public Map<String, Object> listEnterpriseConfigPublishes(String tenantId, int page, int pageSize) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        String fromSql = " FROM reconciliation.reco_config_publish p WHERE p.tenant_id IS NOT DISTINCT FROM ? ";
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    CAST(p.reco_config_publish_id AS text) AS "recoConfigPublishId",
                    p.version_label AS "versionLabel",
                    p.published_at AS "publishedAt",
                    CAST(p.published_by AS text) AS "publishedBy",
                    p.notes AS "notes"
                """ + fromSql + """
                ORDER BY p.published_at DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        return out;
    }

    public Map<String, Object> publishEnterpriseConfig(String tenantId, String versionLabel, Object snapshot, String publishedBy, String notes) {
        UUID id = UUID.randomUUID();
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID publishedByUuid = parseOptionalUuid(publishedBy);
        Object snap = snapshot == null ? Map.of() : snapshot;
        jdbc.update("""
                INSERT INTO reconciliation.reco_config_publish (
                    reco_config_publish_id, tenant_id, version_label, published_at, published_by, notes, snapshot_json
                ) VALUES (?, ?, ?, now(), ?, ?, ?::jsonb)
                """, id, tenantUuid, versionLabel.trim(), publishedByUuid, notes == null ? null : notes.trim(), toJson(snap));
        writeAudit(tenantUuid, publishedByUuid, "RECO_CONFIG_PUBLISHED", "reco_config_publish", id.toString(), Map.of("versionLabel", versionLabel.trim()));
        return Map.of(
                "recoConfigPublishId", id.toString(),
                "versionLabel", versionLabel.trim(),
                "status", "PUBLISHED"
        );
    }

    public Map<String, Object> attachConfigPublishToRun(String runReferenceCode, String recoConfigPublishId) {
        UUID publishPk = parseOptionalUuid(recoConfigPublishId);
        if (publishPk == null || runReferenceCode == null || runReferenceCode.isBlank()) {
            return Map.of("updated", false, "reason", "INVALID_INPUT");
        }
        int n = jdbc.update("""
                UPDATE reconciliation.reconciliation_run
                SET reco_config_publish_id = ?, modified_on = now()
                WHERE run_reference_code = ?
                """, publishPk, runReferenceCode.trim());
        return Map.of("updated", n > 0, "reconciliationRunId", runReferenceCode.trim(), "recoConfigPublishId", recoConfigPublishId.trim());
    }

    public Map<String, Object> rollbackEnterprisePublish(String tenantId, String fromPublishId) {
        UUID fromId = parseOptionalUuid(fromPublishId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (fromId == null) {
            return Map.of("status", "INVALID_INPUT");
        }
        List<Map<String, Object>> cur = jdbc.queryForList("""
                SELECT published_at, snapshot_json::text AS snap, tenant_id
                FROM reconciliation.reco_config_publish WHERE reco_config_publish_id = ?
                """, fromId);
        if (cur.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Map<String, Object>> prev = jdbc.queryForList("""
                SELECT CAST(reco_config_publish_id AS text) AS "recoConfigPublishId", snapshot_json::text AS "snap"
                FROM reconciliation.reco_config_publish
                WHERE tenant_id IS NOT DISTINCT FROM ? AND published_at < ?::timestamptz
                ORDER BY published_at DESC LIMIT 1
                """, tenantUuid, cur.get(0).get("published_at"));
        if (prev.isEmpty()) {
            return Map.of("status", "NO_PRIOR_PUBLISH", "fromPublishId", fromPublishId);
        }
        UUID newId = UUID.randomUUID();
        String priorSnap = prev.get(0).get("snap") == null ? "{}" : String.valueOf(prev.get(0).get("snap"));
        jdbc.update("""
                INSERT INTO reconciliation.reco_config_publish (
                    reco_config_publish_id, tenant_id, version_label, published_at, published_by, notes, snapshot_json
                ) VALUES (?, ?, ?, now(), NULL, ?, ?::jsonb)
                """, newId, tenantUuid, "rollback-from-" + fromId, "Rollback prior snapshot", priorSnap);
        writeAudit(tenantUuid, null, "RECO_CONFIG_ROLLBACK_PUBLISH", "reco_config_publish", newId.toString(),
                Map.of("fromPublishId", fromId.toString(), "priorPublishId", String.valueOf(prev.get(0).get("recoConfigPublishId"))));
        return Map.of(
                "status", "PUBLISHED",
                "recoConfigPublishId", newId.toString(),
                "sourcePriorPublishId", String.valueOf(prev.get(0).get("recoConfigPublishId")),
                "versionLabel", "rollback-from-" + fromId
        );
    }

    public Map<String, Object> compareEnterprisePublishes(String aId, String bId) {
        UUID a = parseOptionalUuid(aId);
        UUID b = parseOptionalUuid(bId);
        if (a == null || b == null) {
            return Map.of("status", "INVALID_INPUT", "diff", List.of());
        }
        String sa = loadPublishSnapshot(a);
        String sb = loadPublishSnapshot(b);
        if (sa == null || sb == null) {
            return Map.of("status", "NOT_FOUND", "diff", List.of());
        }
        try {
            JsonNode ja = objectMapper.readTree(sa);
            JsonNode jb = objectMapper.readTree(sb);
            List<Map<String, Object>> diff = diffJson(ja, jb, "");
            return Map.of("status", "OK", "aId", aId, "bId", bId, "diff", diff);
        } catch (JsonProcessingException e) {
            return Map.of("status", "INVALID_JSON", "diff", List.of(), "message", e.getMessage());
        }
    }

    private String loadPublishSnapshot(UUID id) {
        try {
            return jdbc.queryForObject(
                    "SELECT snapshot_json::text FROM reconciliation.reco_config_publish WHERE reco_config_publish_id = ?",
                    String.class, id);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private List<Map<String, Object>> diffJson(JsonNode a, JsonNode b, String path) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (a.equals(b)) {
            return out;
        }
        if (a.getNodeType() != b.getNodeType()) {
            out.add(Map.of("path", path.isEmpty() ? "$" : path, "change", "TYPE_MISMATCH", "a", a.toString(), "b", b.toString()));
            return out;
        }
        if (!a.isObject() || !b.isObject()) {
            out.add(Map.of("path", path.isEmpty() ? "$" : path, "change", "VALUE", "a", a.toString(), "b", b.toString()));
            return out;
        }
        ObjectNode oa = (ObjectNode) a;
        ObjectNode ob = (ObjectNode) b;
        Iterator<String> it = oa.fieldNames();
        while (it.hasNext()) {
            String f = it.next();
            String p = path.isEmpty() ? "$." + f : path + "." + f;
            if (!ob.has(f)) {
                out.add(Map.of("path", p, "change", "REMOVED_IN_B"));
            } else {
                out.addAll(diffJson(oa.get(f), ob.get(f), p));
            }
        }
        Iterator<String> itb = ob.fieldNames();
        while (itb.hasNext()) {
            String f = itb.next();
            String p = path.isEmpty() ? "$." + f : path + "." + f;
            if (!oa.has(f)) {
                out.add(Map.of("path", p, "change", "ADDED_IN_B"));
            }
        }
        return out;
    }

    public Map<String, Object> getEnterpriseConfigDraftSummary(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        Long mr = jdbc.queryForObject(
                "SELECT COUNT(1) FROM reconciliation.matching_rule WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        Long gl = jdbc.queryForObject(
                "SELECT COUNT(1) FROM reconciliation.gl_mapping_rule WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        Long sch = jdbc.queryForObject(
                "SELECT COUNT(1) FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        String h = Integer.toHexString(Objects.hash(mr, gl, sch, String.valueOf(tenantUuid)));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("matchingRulesCount", mr == null ? 0 : mr);
        out.put("glMappingRulesCount", gl == null ? 0 : gl);
        out.put("schedulesCount", sch == null ? 0 : sch);
        out.put("draftFingerprint", h);
        out.put("tenantId", tenantUuid == null ? null : tenantUuid.toString());
        return out;
    }

    public Map<String, Object> testMatchingRulePreview(String matchingRuleId, Map<String, Object> samplePayload) {
        UUID rulePk = parseOptionalUuid(matchingRuleId);
        if (rulePk == null) {
            return Map.of("status", "NOT_FOUND", "matched", false, "message", "Invalid matchingRuleId");
        }
        List<Map<String, Object>> exists = jdbc.queryForList(
                "SELECT 1 FROM reconciliation.matching_rule WHERE matching_rule_id = ? AND deleted_on IS NULL", rulePk);
        if (exists.isEmpty()) {
            return Map.of("status", "NOT_FOUND", "matched", false);
        }
        Map<String, Object> detail = getEnterpriseMatchingRuleDetail(matchingRuleId);
        return Map.of(
                "status", "STUB",
                "matched", true,
                "message", "Server-side rule evaluation is not implemented; sample payload echoed for contract testing.",
                "samplePayload", samplePayload == null ? Map.of() : samplePayload,
                "ruleSummary", Map.of(
                        "matchingRuleId", matchingRuleId,
                        "criteriaCount", ((List<?>) detail.getOrDefault("criteria", List.of())).size()
                )
        );
    }

    public Map<String, Object> getEnterpriseThresholdsBundle(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Map<String, Object>> variance = jdbc.queryForList("""
                SELECT CAST(reco_variance_threshold_id AS text) AS "id", threshold_name AS "thresholdName",
                       threshold_code AS "thresholdCode", d.code AS "recoDomain",
                       min_amount AS "minAmount", max_amount AS "maxAmount", sev.code AS "severity",
                       is_active AS "active", effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_variance_threshold v
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = v.reco_domain_id
                JOIN reconciliation.lu_severity sev ON sev.severity_id = v.severity_id
                WHERE v.deleted_on IS NULL AND v.tenant_id IS NOT DISTINCT FROM ?
                ORDER BY v.threshold_code
                """, tenantUuid);
        List<Map<String, Object>> sla = jdbc.queryForList("""
                SELECT CAST(reco_sla_threshold_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       settlement_delay_hours AS "settlementDelayHours", refund_sla_hours AS "refundSlaHours",
                       exception_aging_hours AS "exceptionAgingHours", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_sla_threshold WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid);
        List<Map<String, Object>> op = jdbc.queryForList("""
                SELECT CAST(reco_operational_threshold_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       pending_invoice_timeout_minutes AS "pendingInvoiceTimeoutMinutes", max_retry_count AS "maxRetryCount",
                       duplicate_event_window_minutes AS "duplicateEventWindowMinutes", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_operational_threshold WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid);
        List<Map<String, Object>> esc = jdbc.queryForList("""
                SELECT CAST(reco_escalation_policy_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       auto_escalate AS "autoEscalate", escalate_after_hours AS "escalateAfterHours",
                       escalation_role_code AS "escalationRoleCode", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_escalation_policy WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid);
        return Map.of(
                "varianceRecords", variance,
                "slaRecords", sla,
                "operationalRecords", op,
                "escalationRecords", esc
        );
    }

    public List<Map<String, Object>> listEnterpriseRoles(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        return jdbc.queryForList("""
                SELECT CAST(reco_role_id AS text) AS "roleId", role_name AS "roleName", role_code AS "roleCode",
                       is_active AS "active", financial_period_access_json AS "financialPeriodAccess"
                FROM reconciliation.reco_role
                WHERE deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                ORDER BY role_code
                """, tenantUuid);
    }

    public List<Map<String, Object>> listEnterpriseNotificationRules(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        return jdbc.queryForList("""
                SELECT CAST(n.reco_notification_rule_id AS text) AS "notificationRuleId", n.rule_name AS "ruleName",
                       n.rule_code AS "ruleCode", ev.code AS "eventCode", n.channel_code AS "channelCode",
                       n.template_code AS "templateCode", n.is_active AS "active",
                       CAST(n.reco_schedule_id AS text) AS "scheduleId"
                FROM reconciliation.reco_notification_rule n
                JOIN reconciliation.lu_notification_event ev ON ev.notification_event_id = n.notification_event_id
                WHERE n.deleted_on IS NULL AND n.tenant_id IS NOT DISTINCT FROM ?
                ORDER BY n.rule_code
                """, tenantUuid);
    }

    public Map<String, Object> createEnterpriseMatchingRule(String tenantId, ReconciliationRequests.EnterpriseMatchingRuleWriteRequest req, String actorUserId) {
        Map<String, Object> mr = req.matchingRule();
        List<Map<String, Object>> criteria = req.criteria() == null ? List.of() : req.criteria();
        if (criteria.isEmpty()) {
            throw new IllegalArgumentException("At least one criterion is required");
        }
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID ruleId = UUID.randomUUID();
        insertMatchingRuleHeader(ruleId, tenantUuid, mr, false);
        replaceCriteriaScopesOutcomes(ruleId, criteria, req.scopes(), req.outcomes());
        writeAudit(tenantUuid, actor, "MATCHING_RULE_CREATED", "matching_rule", ruleId.toString(), Map.of("ruleCode", str(mr, "ruleCode")));
        return getEnterpriseMatchingRuleDetail(ruleId.toString());
    }

    public Map<String, Object> updateEnterpriseMatchingRule(String tenantId, String matchingRuleId, ReconciliationRequests.EnterpriseMatchingRuleWriteRequest req, String actorUserId) {
        UUID ruleId = parseOptionalUuid(matchingRuleId);
        if (ruleId == null) {
            return Map.of("status", "NOT_FOUND");
        }
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        List<Map<String, Object>> exists = jdbc.queryForList(
                "SELECT 1 FROM reconciliation.matching_rule WHERE matching_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                ruleId, tenantUuid);
        if (exists.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        insertMatchingRuleHeader(ruleId, tenantUuid, req.matchingRule(), true);
        jdbc.update("DELETE FROM reconciliation.matching_rule_criterion WHERE matching_rule_id = ?", ruleId);
        jdbc.update("DELETE FROM reconciliation.matching_rule_scope WHERE matching_rule_id = ?", ruleId);
        jdbc.update("DELETE FROM reconciliation.matching_rule_outcome WHERE matching_rule_id = ?", ruleId);
        replaceCriteriaScopesOutcomes(ruleId, req.criteria(), req.scopes(), req.outcomes());
        writeAudit(tenantUuid, actor, "MATCHING_RULE_UPDATED", "matching_rule", ruleId.toString(), Map.of());
        return getEnterpriseMatchingRuleDetail(matchingRuleId);
    }

    private void insertMatchingRuleHeader(UUID ruleId, UUID tenantUuid, Map<String, Object> mr, boolean isUpdate) {
        String ruleName = Objects.requireNonNull(str(mr, "ruleName"), "ruleName is required");
        String ruleCode = Objects.requireNonNull(str(mr, "ruleCode"), "ruleCode is required");
        UUID domainId = domainIdByCode(str(mr, "recoDomain"));
        UUID stageId = stageIdByCode(str(mr, "stage"));
        int priority = intVal(mr, "priority") == null ? 100 : intVal(mr, "priority");
        boolean active = bool(mr, "active", true);
        String ef = str(mr, "effectiveFrom");
        String et = str(mr, "effectiveTo");
        String notes = str(mr, "notes");
        if (isUpdate) {
            jdbc.update("""
                    UPDATE reconciliation.matching_rule SET
                        rule_name = ?, rule_code = ?, reco_domain_id = ?, reconciliation_stage_id = ?,
                        priority = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date, notes = ?
                    WHERE matching_rule_id = ?
                    """, ruleName, ruleCode, domainId, stageId, priority, active,
                    ef == null || ef.isBlank() ? null : ef,
                    et == null || et.isBlank() ? null : et,
                    notes, ruleId);
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule (
                        matching_rule_id, tenant_id, rule_name, rule_code, reco_domain_id, reconciliation_stage_id,
                        priority, is_active, effective_from, effective_to, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::date, ?::date, ?)
                    """, ruleId, tenantUuid, ruleName, ruleCode, domainId, stageId, priority, active,
                    ef == null || ef.isBlank() ? null : ef,
                    et == null || et.isBlank() ? null : et,
                    notes);
        }
    }

    private void replaceCriteriaScopesOutcomes(
            UUID ruleId,
            List<Map<String, Object>> criteria,
            List<Map<String, Object>> scopes,
            List<Map<String, Object>> outcomes
    ) {
        int order = 1;
        for (Map<String, Object> c : criteria) {
            UUID opId = operatorIdByCode(str(c, "operator"));
            if (opId == null) {
                throw new IllegalArgumentException("Unknown operator: " + str(c, "operator"));
            }
            UUID mkId = matchKindIdByCode(str(c, "matchKind"));
            UUID tkId = toleranceKindIdByCode(str(c, "toleranceKind"));
            Integer lo = intVal(c, "lineOrder");
            int line = lo == null ? order : lo;
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_criterion (
                        matching_rule_id, line_order, left_operand, matching_operator_id, right_operand,
                        tolerance_amount, tolerance_kind_id, match_kind_id, is_required, extra_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                    """, ruleId, line, Objects.requireNonNull(str(c, "leftOperand"), "leftOperand is required"),
                    opId, str(c, "rightOperand"), dec(c, "toleranceAmount"), tkId, mkId,
                    bool(c, "required", true), toJson(c.get("extra") == null ? Map.of() : c.get("extra")));
            order++;
        }
        if (scopes != null && !scopes.isEmpty()) {
            Map<String, Object> s = scopes.get(0);
            if (scopes.size() > 1) {
                Map<String, Object> merged = new LinkedHashMap<>(s);
                merged.put("_additionalScopes", scopes.subList(1, scopes.size()));
                s = merged;
            }
            UUID locId = parseOptionalUuid(str(s, "locationId"));
            UUID lvlId = parseOptionalUuid(str(s, "locationLevelId"));
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_scope (
                        matching_rule_id, location_id, location_level_id, include_child_locations,
                        gateway_code, payment_method_code, currency_code, source_system_id, scope_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                    """, ruleId, locId, lvlId,
                    bool(s, "includeChildLocations", true),
                    str(s, "gatewayCode"),
                    str(s, "paymentMethodCode"),
                    str(s, "currencyCode"),
                    sourceSystemIdByCode(str(s, "sourceSystem")),
                    toJson(s.get("scopeJson") == null ? Map.of() : s.get("scopeJson")));
        }
        if (outcomes != null && !outcomes.isEmpty()) {
            Map<String, Object> o = outcomes.get(0);
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_outcome (
                        matching_rule_id, auto_resolve, create_exception, failure_severity_id, retry_eligible, outcome_json
                    ) VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    """, ruleId,
                    bool(o, "autoResolve", false),
                    bool(o, "createException", true),
                    severityIdByCode(str(o, "failureSeverity")),
                    bool(o, "retryEligible", false),
                    toJson(o.get("outcomeJson") == null ? Map.of() : o.get("outcomeJson")));
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_outcome (
                        matching_rule_id, auto_resolve, create_exception, failure_severity_id, retry_eligible, outcome_json
                    ) VALUES (?, false, true, (SELECT severity_id FROM reconciliation.lu_severity WHERE code = 'MEDIUM' LIMIT 1), false, '{}'::jsonb)
                    """, ruleId);
        }
    }

    public Map<String, Object> patchMatchingRuleActive(String tenantId, String matchingRuleId, boolean active) {
        UUID ruleId = parseOptionalUuid(matchingRuleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (ruleId == null) {
            return Map.of("updated", false);
        }
        int n = jdbc.update("""
                UPDATE reconciliation.matching_rule SET is_active = ?, modified_on = now()
                WHERE matching_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                """, active, ruleId, tenantUuid);
        return Map.of("updated", n > 0, "matchingRuleId", matchingRuleId, "active", active);
    }

    public Map<String, Object> softDeleteMatchingRule(String tenantId, String matchingRuleId) {
        UUID ruleId = parseOptionalUuid(matchingRuleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (ruleId == null) {
            return Map.of("updated", false);
        }
        int n = jdbc.update("""
                UPDATE reconciliation.matching_rule SET deleted_on = now(), modified_on = now()
                WHERE matching_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                """, ruleId, tenantUuid);
        return Map.of("updated", n > 0, "matchingRuleId", matchingRuleId);
    }

    public Map<String, Object> upsertGlMappingRule(String tenantId, Map<String, Object> m, String idOrNull, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID parsedId = parseOptionalUuid(idOrNull);
        boolean isUpdate = parsedId != null;
        UUID id = isUpdate ? parsedId : UUID.randomUUID();
        String name = Objects.requireNonNull(str(m, "mappingName"), "mappingName is required");
        String code = Objects.requireNonNull(str(m, "mappingCode"), "mappingCode is required");
        if (isUpdate) {
            int u = jdbc.update("""
                    UPDATE reconciliation.gl_mapping_rule SET
                        mapping_name = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                        source_type = ?, transaction_type = ?, item_category = ?, payment_method_code = ?, refund_type = ?,
                        debit_gl_account_code = ?, credit_gl_account_code = ?, tax_gl_account_code = ?, refund_gl_account_code = ?,
                        currency_code = ?, line_of_business = ?, auto_post = ?, requires_approval = ?, reversal_allowed = ?,
                        journal_type = ?, match_rank = ?, dimensions_json = ?::jsonb, modified_on = now(), modified_by = ?
                    WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                    """, name, bool(m, "active", true), str(m, "effectiveFrom"), str(m, "effectiveTo"),
                    str(m, "sourceType"), str(m, "transactionType"), str(m, "itemCategory"), str(m, "paymentMethodCode"), str(m, "refundType"),
                    Objects.requireNonNull(str(m, "debitGlAccountCode"), "debitGlAccountCode required"),
                    Objects.requireNonNull(str(m, "creditGlAccountCode"), "creditGlAccountCode required"),
                    str(m, "taxGlAccountCode"), str(m, "refundGlAccountCode"),
                    str(m, "currencyCode"), str(m, "lineOfBusiness"),
                    bool(m, "autoPost", false), bool(m, "requiresApproval", true), bool(m, "reversalAllowed", true),
                    str(m, "journalType"), intVal(m, "matchRank") == null ? 100 : intVal(m, "matchRank"),
                    toJson(m.get("dimensionsJson") == null ? Map.of() : m.get("dimensionsJson")),
                    actor, id, tenantUuid);
            if (u == 0) {
                return Map.of("updated", false, "glMappingRuleId", idOrNull);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.gl_mapping_rule (
                        gl_mapping_rule_id, tenant_id, mapping_name, mapping_code, is_active, effective_from, effective_to,
                        source_type, transaction_type, item_category, payment_method_code, refund_type,
                        debit_gl_account_code, credit_gl_account_code, tax_gl_account_code, refund_gl_account_code,
                        currency_code, line_of_business, auto_post, requires_approval, reversal_allowed, journal_type,
                        match_rank, dimensions_json, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?::date, ?::date, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                    """, id, tenantUuid, name, code, bool(m, "active", true), str(m, "effectiveFrom"), str(m, "effectiveTo"),
                    str(m, "sourceType"), str(m, "transactionType"), str(m, "itemCategory"), str(m, "paymentMethodCode"), str(m, "refundType"),
                    Objects.requireNonNull(str(m, "debitGlAccountCode")),
                    Objects.requireNonNull(str(m, "creditGlAccountCode")),
                    str(m, "taxGlAccountCode"), str(m, "refundGlAccountCode"),
                    str(m, "currencyCode"), str(m, "lineOfBusiness"),
                    bool(m, "autoPost", false), bool(m, "requiresApproval", true), bool(m, "reversalAllowed", true),
                    str(m, "journalType"), intVal(m, "matchRank") == null ? 100 : intVal(m, "matchRank"),
                    toJson(m.get("dimensionsJson") == null ? Map.of() : m.get("dimensionsJson")), actor);
        }
        writeAudit(tenantUuid, actor, isUpdate ? "GL_MAPPING_UPDATED" : "GL_MAPPING_CREATED", "gl_mapping_rule", id.toString(), Map.of("mappingCode", code));
        return Map.of("glMappingRuleId", id.toString(), "updated", true);
    }

    public Map<String, Object> patchGlMappingActive(String tenantId, String id, boolean active) {
        UUID gid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.gl_mapping_rule SET is_active = ?, modified_on = now() WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                active, gid, tenantUuid);
        return Map.of("updated", n > 0, "glMappingRuleId", id, "active", active);
    }

    public Map<String, Object> softDeleteGlMappingRule(String tenantId, String id) {
        UUID gid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.gl_mapping_rule SET deleted_on = now() WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                gid, tenantUuid);
        return Map.of("updated", n > 0, "glMappingRuleId", id);
    }

    public Map<String, Object> upsertSchedule(String tenantId, Map<String, Object> m, String idOrNull, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID parsedId = parseOptionalUuid(idOrNull);
        boolean isUpdate = parsedId != null;
        UUID id = isUpdate ? parsedId : UUID.randomUUID();
        String name = Objects.requireNonNull(str(m, "scheduleName"), "scheduleName is required");
        String code = Objects.requireNonNull(str(m, "scheduleCode"), "scheduleCode is required");
        UUID freqId = scheduleFrequencyIdByCode(str(m, "frequency"));
        UUID domainId = domainIdByCode(str(m, "recoDomain"));
        UUID stageId = stageIdByCode(str(m, "stage"));
        int interval = intVal(m, "interval") == null ? 1 : intVal(m, "interval");
        if (isUpdate) {
            int u = jdbc.update("""
                    UPDATE reconciliation.reco_schedule SET
                        schedule_name = ?, reco_domain_id = ?, reconciliation_stage_id = ?, is_active = ?,
                        schedule_frequency_id = ?, interval_n = ?, cron_expression = ?, timezone = ?,
                        location_id = ?, include_child_locations = ?, gateway_code = ?,
                        allow_parallel_runs = ?, max_runtime_minutes = ?, retry_on_failure = ?, retry_count = ?,
                        notify_on_failure = ?, notify_on_completion = ?, notification_channels_json = ?::jsonb,
                        execution_json = ?::jsonb, modified_on = now(), modified_by = ?
                    WHERE reco_schedule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?
                    """, name, domainId, stageId, bool(m, "active", true), freqId, interval,
                    str(m, "cronExpression"), str(m, "timezone") == null ? "UTC" : str(m, "timezone"),
                    parseOptionalUuid(str(m, "locationId")), bool(m, "includeChildLocations", true), str(m, "gatewayCode"),
                    bool(m, "allowParallelRuns", false), intVal(m, "maxRuntimeMinutes"),
                    bool(m, "retryOnFailure", true), intVal(m, "retryCount") == null ? 3 : intVal(m, "retryCount"),
                    bool(m, "notifyOnFailure", true), bool(m, "notifyOnCompletion", false),
                    toJson(m.get("notificationChannelsJson") == null ? List.of() : m.get("notificationChannelsJson")),
                    toJson(m.get("executionJson") == null ? Map.of() : m.get("executionJson")),
                    actor, id, tenantUuid);
            if (u == 0) {
                return Map.of("updated", false, "scheduleId", idOrNull);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_schedule (
                        reco_schedule_id, tenant_id, schedule_name, schedule_code, reco_domain_id, reconciliation_stage_id,
                        is_active, schedule_frequency_id, interval_n, cron_expression, timezone,
                        location_id, include_child_locations, gateway_code,
                        allow_parallel_runs, max_runtime_minutes, retry_on_failure, retry_count,
                        notify_on_failure, notify_on_completion, notification_channels_json, execution_json, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)
                    """, id, tenantUuid, name, code, domainId, stageId, bool(m, "active", true), freqId, interval,
                    str(m, "cronExpression"), str(m, "timezone") == null ? "UTC" : str(m, "timezone"),
                    parseOptionalUuid(str(m, "locationId")), bool(m, "includeChildLocations", true), str(m, "gatewayCode"),
                    bool(m, "allowParallelRuns", false), intVal(m, "maxRuntimeMinutes"),
                    bool(m, "retryOnFailure", true), intVal(m, "retryCount") == null ? 3 : intVal(m, "retryCount"),
                    bool(m, "notifyOnFailure", true), bool(m, "notifyOnCompletion", false),
                    toJson(m.get("notificationChannelsJson") == null ? List.of() : m.get("notificationChannelsJson")),
                    toJson(m.get("executionJson") == null ? Map.of() : m.get("executionJson")), actor);
        }
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_SCHEDULE_UPDATED" : "RECO_SCHEDULE_CREATED", "reco_schedule", id.toString(), Map.of("scheduleCode", code));
        return Map.of("scheduleId", id.toString(), "updated", true);
    }

    public Map<String, Object> patchScheduleActive(String tenantId, String id, boolean active) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.reco_schedule SET is_active = ?, modified_on = now() WHERE reco_schedule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                active, sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id, "active", active);
    }

    public Map<String, Object> softDeleteSchedule(String tenantId, String id) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.reco_schedule SET deleted_on = now() WHERE reco_schedule_id = ? AND deleted_on IS NULL AND tenant_id IS NOT DISTINCT FROM ?",
                sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id);
    }
}
