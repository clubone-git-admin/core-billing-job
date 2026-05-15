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
                    INSERT INTO reconciliation.audit_log (application_id, actor_user_id, action_code, entity_type, entity_reference_code, details_json)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    """, tenantId, actorId, action, entityType, entityRef, toJson(sanitizeAuditDetails(details)));
        } catch (Exception ignored) {
            // best-effort audit
        }
    }

    /** Map.of() rejects null values; audit payloads may omit codes filled by DB triggers. */
    private Object sanitizeAuditDetails(Object details) {
        if (!(details instanceof Map<?, ?> raw)) {
            return details == null ? Map.of() : details;
        }
        Map<String, Object> out = new HashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            if (e.getValue() != null) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        return out;
    }

    private static String optionalTrimmedCode(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return raw.trim();
    }

    private static String resolveThresholdDisplayName(Map<?, ?> m) {
        String name = str(m, "thresholdName");
        if (name == null || name.isBlank()) {
            name = str(m, "policyName");
        }
        return name == null ? null : name.trim();
    }

    private String loadVarianceThresholdCode(UUID id) {
        List<String> rows = jdbc.query(
                "SELECT threshold_code FROM reconciliation.reco_variance_threshold WHERE reco_variance_threshold_id = ? LIMIT 1",
                (rs, rn) -> rs.getString(1),
                id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private String loadPolicyCode(String table, String idColumn, UUID id) {
        String sql = "SELECT policy_code FROM reconciliation." + table + " WHERE " + idColumn + " = ? LIMIT 1";
        List<String> rows = jdbc.query(sql, (rs, rn) -> rs.getString(1), id);
        return rows.isEmpty() ? null : rows.get(0);
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

    private UUID glCodeIdForApplication(String glCode, UUID applicationId) {
        if (glCode == null || glCode.isBlank()) {
            return null;
        }
        String c = glCode.trim();
        if (applicationId != null) {
            UUID id = queryUuidOrNull(
                    "SELECT gl_code_id FROM finance.gl_code WHERE gl_code = ? AND application_id = ? LIMIT 1",
                    c, applicationId);
            if (id != null) {
                return id;
            }
        }
        return queryUuidOrNull(
                "SELECT gl_code_id FROM finance.gl_code WHERE gl_code = ? ORDER BY created_on DESC LIMIT 1",
                c);
    }

    private UUID luGlSourceTypeId(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull(
                "SELECT gl_source_type_id FROM reconciliation.lu_gl_source_type WHERE UPPER(code) = UPPER(?) AND is_active = true LIMIT 1",
                code.trim());
    }

    private UUID luGlTransactionTypeId(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull(
                "SELECT gl_transaction_type_id FROM reconciliation.lu_gl_transaction_type WHERE UPPER(code) = UPPER(?) AND is_active = true LIMIT 1",
                code.trim());
    }

    private UUID luJournalTypeId(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull(
                "SELECT journal_type_id FROM reconciliation.lu_journal_type WHERE UPPER(code) = UPPER(?) AND is_active = true LIMIT 1",
                code.trim());
    }

    private UUID luLineOfBusinessId(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        return queryUuidOrNull(
                "SELECT line_of_business_id FROM reconciliation.lu_line_of_business WHERE UPPER(code) = UPPER(?) AND is_active = true LIMIT 1",
                code.trim());
    }

    private UUID itemCategoryIdByCode(String code, UUID applicationId) {
        if (code == null || code.isBlank()) {
            return null;
        }
        if (applicationId != null) {
            return queryUuidOrNull(
                    "SELECT item_category_id FROM items.lu_item_category WHERE UPPER(code) = UPPER(?) AND application_id = ? LIMIT 1",
                    code.trim(), applicationId);
        }
        return queryUuidOrNull(
                "SELECT item_category_id FROM items.lu_item_category WHERE UPPER(code) = UPPER(?) ORDER BY created_on DESC LIMIT 1",
                code.trim());
    }

    private UUID paymentCurrencyTypeIdByCode(String currencyCode) {
        if (currencyCode == null || currencyCode.isBlank()) {
            return null;
        }
        return queryUuidOrNull(
                "SELECT payment_gateway_currency_type_id FROM payment_gateway.lu_payment_gateway_currency_type WHERE UPPER(currency_code) = UPPER(?) LIMIT 1",
                currencyCode.trim());
    }

    private UUID paymentMethodTypeIdByCode(String methodCode) {
        if (methodCode == null || methodCode.isBlank()) {
            return null;
        }
        UUID asId = parseOptionalUuid(methodCode);
        if (asId != null) {
            return asId;
        }
        return queryUuidOrNull(
                "SELECT payment_gateway_method_type_id FROM payment_gateway.lu_payment_gateway_method_type WHERE UPPER(COALESCE(method_type_code, '')) = UPPER(?) LIMIT 1",
                methodCode.trim());
    }

    private record OperandBinding(String operandType, UUID operandId, String operandValue) {
    }

    private OperandBinding resolveOperandBinding(String raw, boolean required, String fieldLabel) {
        if (raw == null || raw.isBlank()) {
            if (required) {
                throw new IllegalArgumentException(fieldLabel + " is required");
            }
            return new OperandBinding("VALUE", null, null);
        }
        String t = raw.trim();
        UUID asUuid = parseOptionalUuid(t);
        if (asUuid != null) {
            UUID exists = queryUuidOrNull(
                    "SELECT matching_operand_id FROM reconciliation.matching_operand_catalog WHERE matching_operand_id = ? AND is_active = true LIMIT 1",
                    asUuid);
            if (exists != null) {
                return new OperandBinding("FIELD", exists, null);
            }
        }
        UUID byCode = queryUuidOrNull(
                "SELECT matching_operand_id FROM reconciliation.matching_operand_catalog WHERE UPPER(operand_code) = UPPER(?) AND is_active = true LIMIT 1",
                t);
        if (byCode != null) {
            return new OperandBinding("FIELD", byCode, null);
        }
        return new OperandBinding("VALUE", null, t);
    }

    /**
     * GL mapping editor lookups (also merged into {@link #getEnterpriseConfigLookups(String)} when tenant is known).
     */
    public Map<String, Object> getGlMappingRuleLookupsBundle(String tenantId) {
        Map<String, Object> m = new HashMap<>();
        UUID app = parseOptionalUuid(tenantId);
        m.put("glSourceTypes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_gl_source_type WHERE is_active = true ORDER BY code
                """));
        m.put("glTransactionTypes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_gl_transaction_type WHERE is_active = true ORDER BY code
                """));
        m.put("lineOfBusinesses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_line_of_business WHERE is_active = true ORDER BY code
                """));
        m.put("journalTypes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_journal_type WHERE is_active = true ORDER BY code
                """));
        m.put("paymentGatewayMethodTypes", jdbc.queryForList("""
                SELECT CAST(payment_gateway_method_type_id AS text) AS "methodTypeId",
                       method_type_code AS "methodTypeCode", method_type_name AS "methodTypeName",
                       COALESCE(is_active, true) AS "active"
                FROM payment_gateway.lu_payment_gateway_method_type
                WHERE COALESCE(is_active, true) = true
                ORDER BY method_type_name
                """));
        m.put("paymentGatewayCurrencyTypes", jdbc.queryForList("""
                SELECT CAST(payment_gateway_currency_type_id AS text) AS "currencyTypeId",
                       currency_code AS "currencyCode", currency_name AS "currencyName", currency_symbol AS "currencySymbol",
                       COALESCE(is_active, true) AS "active"
                FROM payment_gateway.lu_payment_gateway_currency_type
                WHERE COALESCE(is_active, true) = true
                ORDER BY currency_code
                """));
        List<Object> icArgs = new ArrayList<>();
        StringBuilder icSql = new StringBuilder("""
                SELECT CAST(item_category_id AS text) AS "itemCategoryId", code AS "code", name AS "name",
                       description AS "description", is_active AS "active",
                       CAST(application_id AS text) AS "applicationId"
                FROM items.lu_item_category
                WHERE is_active = true
                """);
        if (app != null) {
            icSql.append(" AND application_id = ? ");
            icArgs.add(app);
        }
        icSql.append(" ORDER BY code LIMIT 2000 ");
        m.put("itemCategories", jdbc.queryForList(icSql.toString(), icArgs.toArray()));
        List<Object> glArgs = new ArrayList<>();
        StringBuilder glSql = new StringBuilder("""
                SELECT gc.gl_code AS "code", gc.description AS "description",
                       lat.name AS "accountTypeName", gc.is_active AS "active",
                       CAST(gc.gl_code_id AS text) AS "glCodeId",
                       CAST(gc.application_id AS text) AS "applicationId"
                FROM finance.gl_code gc
                JOIN finance.lu_gl_account_type lat ON lat.gl_account_type_id = gc.gl_account_type_id
                WHERE gc.is_active = true
                """);
        if (app != null) {
            glSql.append(" AND gc.application_id = ? ");
            glArgs.add(app);
        }
        glSql.append(" ORDER BY gc.gl_code LIMIT 5000 ");
        m.put("glCodes", jdbc.queryForList(glSql.toString(), glArgs.toArray()));
        m.put("refundTypes", jdbc.queryForList("""
                SELECT DISTINCT refund_type AS "code", refund_type AS "displayName"
                FROM reconciliation.gl_mapping_rule
                WHERE deleted_on IS NULL AND refund_type IS NOT NULL AND TRIM(refund_type) <> ''
                ORDER BY 1
                """));
        return m;
    }

    public Map<String, Object> getEnterpriseConfigLookups(String tenantId) {
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
        List<Map<String, Object>> stageRows = jdbc.queryForList("""
                SELECT CAST(reconciliation_stage_id AS text) AS "reconciliationStageId",
                       code AS "code", display_name AS "displayName", stage_order AS "stageOrder", is_active AS "active"
                FROM reconciliation.lu_reconciliation_stage WHERE is_active = true ORDER BY stage_order
                """);
        out.put("reconciliationStages", stageRows);
        out.put("reconStages", stageRows);
        out.put("reconciliationEntityTypes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_reconciliation_entity_type WHERE is_active = true ORDER BY code
                """));
        out.put("matchingOperandCatalog", jdbc.queryForList("""
                SELECT CAST(moc.matching_operand_id AS text) AS "operandId",
                       moc.operand_code AS "operandCode",
                       moc.display_name AS "displayName",
                       moc.description AS "description",
                       CAST(moc.reco_domain_id AS text) AS "recoDomainId",
                       rd.code AS "recoDomainCode",
                       CAST(moc.reconciliation_stage_id AS text) AS "reconciliationStageId",
                       st.code AS "reconciliationStageCode",
                       moc.source_alias AS "sourceAlias",
                       moc.field_path AS "fieldPath",
                       moc.data_type AS "dataType",
                       moc.is_nullable AS "nullable",
                       moc.sample_value AS "sampleValue",
                       CAST(moc.allowed_operators_json AS text) AS "allowedOperatorsJson",
                       moc.is_active AS "active"
                FROM reconciliation.matching_operand_catalog moc
                LEFT JOIN reconciliation.lu_reco_domain rd ON rd.reco_domain_id = moc.reco_domain_id
                LEFT JOIN reconciliation.lu_reconciliation_stage st ON st.reconciliation_stage_id = moc.reconciliation_stage_id
                WHERE moc.is_active = true
                ORDER BY COALESCE(st.stage_order, 999), moc.display_name
                """));
        out.put("gateways", jdbc.queryForList("""
                SELECT CAST(payment_gateway_id AS text) AS "gatewayId",
                       name AS "name",
                       base_url AS "baseUrl",
                       COALESCE(is_active, true) AS "active"
                FROM payment_gateway.payment_gateway
                WHERE COALESCE(is_active, true) = true
                ORDER BY name
                """));
        out.put("paymentGatewaySupportedMethods", jdbc.queryForList("""
                SELECT CAST(psm.payment_gateway_supported_method_id AS text) AS "supportedMethodId",
                       CAST(psm.payment_gateway_id AS text) AS "gatewayId",
                       pg.name AS "gatewayName",
                       CAST(psm.payment_gateway_method_type_id AS text) AS "methodTypeId",
                       mt.method_type_code AS "methodTypeCode",
                       mt.method_type_name AS "methodTypeName",
                       COALESCE(psm.is_active, true) AS "active"
                FROM payment_gateway.payment_gateway_supported_method psm
                JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = psm.payment_gateway_id
                JOIN payment_gateway.lu_payment_gateway_method_type mt ON mt.payment_gateway_method_type_id = psm.payment_gateway_method_type_id
                WHERE COALESCE(psm.is_active, true) = true AND COALESCE(pg.is_active, true) = true
                ORDER BY pg.name, mt.method_type_name
                """));
        out.put("paymentGatewaySupportedCurrencies", jdbc.queryForList("""
                SELECT CAST(psc.payment_gateway_supported_currency_id AS text) AS "supportedCurrencyId",
                       CAST(psc.payment_gateway_id AS text) AS "gatewayId",
                       pg.name AS "gatewayName",
                       CAST(psc.payment_gateway_currency_type_id AS text) AS "currencyTypeId",
                       ct.currency_code AS "currencyCode",
                       ct.currency_name AS "currencyName",
                       ct.currency_symbol AS "currencySymbol",
                       COALESCE(psc.is_active, true) AS "active"
                FROM payment_gateway.payment_gateway_supported_currency psc
                JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = psc.payment_gateway_id
                JOIN payment_gateway.lu_payment_gateway_currency_type ct ON ct.payment_gateway_currency_type_id = psc.payment_gateway_currency_type_id
                WHERE COALESCE(psc.is_active, true) = true AND COALESCE(pg.is_active, true) = true
                ORDER BY pg.name, ct.currency_code
                """));
        out.put("sourceSystems", jdbc.queryForList("""
                SELECT CAST(source_system_id AS text) AS "sourceSystemId",
                       code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_source_system WHERE is_active = true ORDER BY code
                """));
        out.put("severities", jdbc.queryForList("""
                SELECT CAST(severity_id AS text) AS "severityId",
                       code AS "code", display_name AS "displayName", rank_order AS "rankOrder", is_active AS "active"
                FROM reconciliation.lu_severity WHERE is_active = true ORDER BY rank_order, code
                """));
        out.put("recoPermissions", List.of());
        out.putAll(getGlMappingRuleLookupsBundle(tenantId));
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
        String locText = loc.toString();
        where.append("""
                 AND (
                    NOT EXISTS (SELECT 1 FROM reconciliation.matching_rule_scope sc0 WHERE sc0.matching_rule_id = mr.matching_rule_id)
                    OR EXISTS (
                        SELECT 1 FROM reconciliation.matching_rule_scope sc2
                        WHERE sc2.matching_rule_id = mr.matching_rule_id
                          AND (
                              sc2.scope_json->>'locationId' IS NULL
                              OR TRIM(sc2.scope_json->>'locationId') = ''
                              OR sc2.scope_json->>'locationId' = ?
                              OR (sc2.level_id IS NOT NULL AND sc2.level_id = ?)
                          )
                    )
                ) """);
        args.add(locText);
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
        where.append(" AND mr.application_id IS NOT DISTINCT FROM ? ");
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
                    CAST(mr.application_id AS text) AS "tenantId",
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
                    COALESCE(lo.operand_code, c.left_operand_value, CAST(c.left_operand_id AS text)) AS "leftOperand",
                    mo.code AS "operator",
                    COALESCE(ro.operand_code, c.right_operand_value, CAST(c.right_operand_id AS text)) AS "rightOperand",
                    c.tolerance_amount AS "toleranceAmount",
                    tk.code AS "toleranceKind",
                    mk.code AS "matchKind",
                    c.is_required AS "required",
                    c.extra_json AS "extra"
                FROM reconciliation.matching_rule_criterion c
                JOIN reconciliation.lu_matching_operator mo ON mo.matching_operator_id = c.matching_operator_id
                JOIN reconciliation.lu_match_kind mk ON mk.match_kind_id = c.match_kind_id
                LEFT JOIN reconciliation.lu_tolerance_kind tk ON tk.tolerance_kind_id = c.tolerance_kind_id
                LEFT JOIN reconciliation.matching_operand_catalog lo ON lo.matching_operand_id = c.left_operand_id
                LEFT JOIN reconciliation.matching_operand_catalog ro ON ro.matching_operand_id = c.right_operand_id
                WHERE c.matching_rule_id = ?
                ORDER BY c.line_order
                """, rulePk);
        List<Map<String, Object>> scopes = jdbc.queryForList("""
                SELECT
                    CAST(s.matching_rule_scope_id AS text) AS "scopeId",
                    NULLIF(TRIM(s.scope_json->>'locationId'), '') AS "locationId",
                    CAST(s.level_id AS text) AS "locationLevelId",
                    (CASE WHEN s.scope_json->>'includeChildLocations' IS NULL THEN true
                          ELSE (s.scope_json->>'includeChildLocations')::boolean END) AS "includeChildLocations",
                    NULLIF(TRIM(s.scope_json->>'gatewayCode'), '') AS "gatewayCode",
                    NULLIF(TRIM(s.scope_json->>'paymentMethodCode'), '') AS "paymentMethodCode",
                    NULLIF(TRIM(s.scope_json->>'currencyCode'), '') AS "currencyCode",
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

    private void appendGlMappingListFilters(
            StringBuilder where,
            List<Object> args,
            String sourceType,
            String transactionType,
            String lineOfBusiness,
            String journalType,
            String itemCategory,
            String paymentMethodCode,
            String currencyCode,
            String status,
            String effectiveFrom,
            String effectiveTo,
            String refundType
    ) {
        if (sourceType != null && !sourceType.isBlank()) {
            where.append(" AND UPPER(COALESCE(gst.code, '')) = UPPER(?) ");
            args.add(sourceType.trim());
        }
        if (transactionType != null && !transactionType.isBlank()) {
            where.append(" AND UPPER(COALESCE(gtx.code, '')) = UPPER(?) ");
            args.add(transactionType.trim());
        }
        if (lineOfBusiness != null && !lineOfBusiness.isBlank()) {
            where.append(" AND UPPER(COALESCE(lob.code, '')) = UPPER(?) ");
            args.add(lineOfBusiness.trim());
        }
        if (journalType != null && !journalType.isBlank()) {
            where.append(" AND UPPER(COALESCE(jt.code, '')) = UPPER(?) ");
            args.add(journalType.trim());
        }
        if (itemCategory != null && !itemCategory.isBlank()) {
            where.append(" AND UPPER(COALESCE(ic.code, '')) = UPPER(?) ");
            args.add(itemCategory.trim());
        }
        if (paymentMethodCode != null && !paymentMethodCode.isBlank()) {
            where.append(" AND UPPER(COALESCE(pmt.method_type_code, '')) = UPPER(?) ");
            args.add(paymentMethodCode.trim());
        }
        if (currencyCode != null && !currencyCode.isBlank()) {
            where.append(" AND UPPER(COALESCE(pcur.currency_code, '')) = UPPER(?) ");
            args.add(currencyCode.trim());
        }
        if (status != null && !status.isBlank()) {
            String s = status.trim().toUpperCase();
            if ("ACTIVE".equals(s) || "true".equalsIgnoreCase(status.trim())) {
                where.append(" AND g.is_active = true ");
            } else if ("INACTIVE".equals(s) || "false".equalsIgnoreCase(status.trim())) {
                where.append(" AND g.is_active = false ");
            }
        }
        if (refundType != null && !refundType.isBlank()) {
            where.append(" AND UPPER(COALESCE(g.refund_type, '')) = UPPER(?) ");
            args.add(refundType.trim());
        }
        if (effectiveFrom != null && !effectiveFrom.isBlank() && effectiveTo != null && !effectiveTo.isBlank()) {
            where.append("""
                     AND (g.effective_to IS NULL OR g.effective_to::date >= ?::date)
                     AND (g.effective_from IS NULL OR g.effective_from::date <= ?::date)
                    """);
            args.add(effectiveFrom.trim());
            args.add(effectiveTo.trim());
        } else if (effectiveFrom != null && !effectiveFrom.isBlank()) {
            where.append("""
                     AND (g.effective_to IS NULL OR g.effective_to::date >= ?::date)
                     AND (g.effective_from IS NULL OR g.effective_from::date <= ?::date)
                    """);
            args.add(effectiveFrom.trim());
            args.add(effectiveFrom.trim());
        } else if (effectiveTo != null && !effectiveTo.isBlank()) {
            where.append("""
                     AND (g.effective_to IS NULL OR g.effective_to::date >= ?::date)
                     AND (g.effective_from IS NULL OR g.effective_from::date <= ?::date)
                    """);
            args.add(effectiveTo.trim());
            args.add(effectiveTo.trim());
        }
    }

    private Map<String, Object> glMappingKpis(UUID tenantUuid) {
        Map<String, Object> k = new HashMap<>();
        if (tenantUuid == null) {
            k.put("activeMappings", 0L);
            k.put("missingMappings", 0L);
            k.put("autoPostEnabled", 0L);
            k.put("approvalRequired", 0L);
            k.put("invalidGlAccounts", 0L);
            return k;
        }
        Long activeMappings = jdbc.queryForObject("""
                SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ? AND g.is_active = true
                """, Long.class, tenantUuid);
        Long autoPost = jdbc.queryForObject("""
                SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ? AND g.auto_post = true
                """, Long.class, tenantUuid);
        Long approval = jdbc.queryForObject("""
                SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ? AND g.requires_approval = true
                """, Long.class, tenantUuid);
        Long invalidGl = jdbc.queryForObject("""
                SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                JOIN finance.gl_code dg ON dg.gl_code_id = g.debit_gl_code_id
                JOIN finance.gl_code cg ON cg.gl_code_id = g.credit_gl_code_id
                LEFT JOIN finance.gl_code tg ON tg.gl_code_id = g.tax_gl_code_id
                LEFT JOIN finance.gl_code rg ON rg.gl_code_id = g.refund_gl_code_id
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                  AND (
                    dg.is_active = false OR cg.is_active = false
                    OR (g.tax_gl_code_id IS NOT NULL AND (tg.gl_code_id IS NULL OR tg.is_active = false))
                    OR (g.refund_gl_code_id IS NOT NULL AND (rg.gl_code_id IS NULL OR rg.is_active = false))
                  )
                """, Long.class, tenantUuid);
        Long missingAgg = jdbc.queryForObject("""
                SELECT COUNT(1) FROM (
                    SELECT 1
                    FROM reconciliation.gl_entry ge
                    JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = ge.reconciliation_run_id
                    WHERE rr.application_id IS NOT DISTINCT FROM ? AND ge.gl_mapping_rule_id IS NULL
                    GROUP BY ge.line_of_business_id, ge.payment_currency_type_id, ge.journal_type_id
                ) x
                """, Long.class, tenantUuid);
        k.put("activeMappings", activeMappings == null ? 0L : activeMappings);
        k.put("missingMappings", missingAgg == null ? 0L : missingAgg);
        k.put("autoPostEnabled", autoPost == null ? 0L : autoPost);
        k.put("approvalRequired", approval == null ? 0L : approval);
        k.put("invalidGlAccounts", invalidGl == null ? 0L : invalidGl);
        return k;
    }

    private List<Map<String, Object>> missingGlMappingsPanel(UUID tenantUuid, int limit) {
        if (tenantUuid == null) {
            return List.of();
        }
        int lim = Math.max(1, Math.min(limit, 200));
        return jdbc.queryForList("""
                SELECT
                    CAST(NULL AS text) AS "sourceType",
                    CAST(NULL AS text) AS "transactionType",
                    lob.code AS "lineOfBusiness",
                    CAST(NULL AS text) AS "paymentMethodCode",
                    pcur.currency_code AS "currencyCode",
                    jt.code AS "journalType",
                    COUNT(*)::bigint AS "txnCount",
                    COALESCE(SUM(GREATEST(ge.debit_amount, ge.credit_amount)), 0) AS "amountSum"
                FROM reconciliation.gl_entry ge
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = ge.reconciliation_run_id
                LEFT JOIN reconciliation.lu_line_of_business lob ON lob.line_of_business_id = ge.line_of_business_id
                LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur ON pcur.payment_gateway_currency_type_id = ge.payment_currency_type_id
                LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = ge.journal_type_id
                WHERE rr.application_id IS NOT DISTINCT FROM ? AND ge.gl_mapping_rule_id IS NULL
                GROUP BY lob.code, pcur.currency_code, jt.code
                ORDER BY COUNT(*) DESC
                LIMIT ?
                """, tenantUuid, lim);
    }

    public Map<String, Object> listEnterpriseGlMappingRules(
            String tenantId,
            Boolean activeOnly,
            String search,
            String locationId,
            String sourceType,
            String transactionType,
            String lineOfBusiness,
            String journalType,
            String itemCategory,
            String paymentMethodCode,
            String currencyCode,
            String status,
            String effectiveFrom,
            String effectiveTo,
            String refundType,
            int page,
            int pageSize
    ) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND g.is_active = true ");
        }
        appendSearchGl(where, args, search);
        appendGlMappingListFilters(where, args, sourceType, transactionType, lineOfBusiness, journalType,
                itemCategory, paymentMethodCode, currencyCode, status, effectiveFrom, effectiveTo, refundType);
        if (parseOptionalUuid(locationId) != null) {
            where.append(" AND (g.dimensions_json->>'locationId' = ? OR g.dimensions_json->>'location_id' = ?) ");
            args.add(locationId.trim());
            args.add(locationId.trim());
        }
        String fromSql = """
                 FROM reconciliation.gl_mapping_rule g
                 LEFT JOIN reconciliation.lu_gl_source_type gst ON gst.gl_source_type_id = g.gl_source_type_id
                 LEFT JOIN reconciliation.lu_gl_transaction_type gtx ON gtx.gl_transaction_type_id = g.gl_transaction_type_id
                 LEFT JOIN reconciliation.lu_line_of_business lob ON lob.line_of_business_id = g.line_of_business_id
                 LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = g.journal_type_id
                 LEFT JOIN items.lu_item_category ic ON ic.item_category_id = g.item_category_id
                 LEFT JOIN payment_gateway.lu_payment_gateway_method_type pmt ON pmt.payment_gateway_method_type_id = g.payment_method_type_id
                 LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur ON pcur.payment_gateway_currency_type_id = g.payment_currency_type_id
                 LEFT JOIN finance.gl_code dg ON dg.gl_code_id = g.debit_gl_code_id
                 LEFT JOIN finance.gl_code cg ON cg.gl_code_id = g.credit_gl_code_id
                 LEFT JOIN finance.gl_code tg ON tg.gl_code_id = g.tax_gl_code_id
                 LEFT JOIN finance.gl_code rg ON rg.gl_code_id = g.refund_gl_code_id
                """ + where;
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
                    CASE WHEN g.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                    gst.code AS "sourceType",
                    gtx.code AS "transactionType",
                    lob.code AS "lineOfBusiness",
                    jt.code AS "journalType",
                    ic.code AS "itemCategory",
                    pmt.method_type_code AS "paymentMethodCode",
                    pcur.currency_code AS "currencyCode",
                    dg.gl_code AS "debitGlAccountCode",
                    cg.gl_code AS "creditGlAccountCode",
                    tg.gl_code AS "taxGlAccountCode",
                    rg.gl_code AS "refundGlAccountCode",
                    g.match_rank AS "matchRank",
                    g.refund_type AS "refundType",
                    g.auto_post AS "autoPost",
                    g.requires_approval AS "requiresApproval",
                    g.effective_from::text AS "effectiveFrom",
                    g.effective_to::text AS "effectiveTo"
                """ + fromSql + """
                ORDER BY g.match_rank ASC, g.mapping_code ASC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        out.put("kpis", glMappingKpis(tenantUuid));
        out.put("missingGlMappings", missingGlMappingsPanel(tenantUuid, 50));
        return out;
    }

    public Map<String, Object> getEnterpriseGlMappingRuleDetail(String tenantId, String glMappingRuleId) {
        UUID gid = parseOptionalUuid(glMappingRuleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (gid == null) {
            return Map.of("status", "NOT_FOUND", "glMappingRule", null);
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    CAST(g.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    g.mapping_name AS "mappingName",
                    g.mapping_code AS "mappingCode",
                    g.is_active AS "active",
                    CASE WHEN g.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                    gst.code AS "sourceType",
                    gtx.code AS "transactionType",
                    lob.code AS "lineOfBusiness",
                    jt.code AS "journalType",
                    ic.code AS "itemCategory",
                    pmt.method_type_code AS "paymentMethodCode",
                    pcur.currency_code AS "currencyCode",
                    dg.gl_code AS "debitGlAccountCode",
                    dg.description AS "debitGlDescription",
                    cg.gl_code AS "creditGlAccountCode",
                    cg.description AS "creditGlDescription",
                    tg.gl_code AS "taxGlAccountCode",
                    tg.description AS "taxGlDescription",
                    rg.gl_code AS "refundGlAccountCode",
                    rg.description AS "refundGlDescription",
                    g.match_rank AS "matchRank",
                    g.refund_type AS "refundType",
                    g.auto_post AS "autoPost",
                    g.requires_approval AS "requiresApproval",
                    g.reversal_allowed AS "reversalAllowed",
                    g.effective_from::text AS "effectiveFrom",
                    g.effective_to::text AS "effectiveTo",
                    CAST(g.dimensions_json AS text) AS "dimensionsJson",
                    CAST(g.created_on AS text) AS "createdOn",
                    CAST(g.modified_on AS text) AS "modifiedOn",
                    CAST(g.created_by AS text) AS "createdBy",
                    CAST(g.modified_by AS text) AS "modifiedBy"
                FROM reconciliation.gl_mapping_rule g
                LEFT JOIN reconciliation.lu_gl_source_type gst ON gst.gl_source_type_id = g.gl_source_type_id
                LEFT JOIN reconciliation.lu_gl_transaction_type gtx ON gtx.gl_transaction_type_id = g.gl_transaction_type_id
                LEFT JOIN reconciliation.lu_line_of_business lob ON lob.line_of_business_id = g.line_of_business_id
                LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = g.journal_type_id
                LEFT JOIN items.lu_item_category ic ON ic.item_category_id = g.item_category_id
                LEFT JOIN payment_gateway.lu_payment_gateway_method_type pmt ON pmt.payment_gateway_method_type_id = g.payment_method_type_id
                LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur ON pcur.payment_gateway_currency_type_id = g.payment_currency_type_id
                LEFT JOIN finance.gl_code dg ON dg.gl_code_id = g.debit_gl_code_id
                LEFT JOIN finance.gl_code cg ON cg.gl_code_id = g.credit_gl_code_id
                LEFT JOIN finance.gl_code tg ON tg.gl_code_id = g.tax_gl_code_id
                LEFT JOIN finance.gl_code rg ON rg.gl_code_id = g.refund_gl_code_id
                WHERE g.gl_mapping_rule_id = ? AND g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                """, gid, tenantUuid);
        if (rows.isEmpty()) {
            return Map.of("status", "NOT_FOUND", "glMappingRule", null);
        }
        return Map.of("status", "OK", "glMappingRule", rows.get(0));
    }

    public Map<String, Object> cloneGlMappingRule(String tenantId, String glMappingRuleId, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID newId = UUID.randomUUID();
        int n = jdbc.update("""
                INSERT INTO reconciliation.gl_mapping_rule (
                    gl_mapping_rule_id, application_id, mapping_name, mapping_code, is_active,
                    effective_from, effective_to, refund_type, auto_post, requires_approval, reversal_allowed, match_rank, dimensions_json,
                    debit_gl_code_id, credit_gl_code_id, tax_gl_code_id, refund_gl_code_id,
                    gl_source_type_id, gl_transaction_type_id, line_of_business_id, journal_type_id,
                    item_category_id, payment_method_type_id, payment_currency_type_id, created_by
                )
                SELECT
                    ?, g.application_id,
                    g.mapping_name || ' (Copy)',
                    g.mapping_code || '-CL-' || left(replace(gen_random_uuid()::text, '-', ''), 8),
                    true,
                    g.effective_from, g.effective_to, g.refund_type, g.auto_post, g.requires_approval, g.reversal_allowed, g.match_rank, g.dimensions_json,
                    g.debit_gl_code_id, g.credit_gl_code_id, g.tax_gl_code_id, g.refund_gl_code_id,
                    g.gl_source_type_id, g.gl_transaction_type_id, g.line_of_business_id, g.journal_type_id,
                    g.item_category_id, g.payment_method_type_id, g.payment_currency_type_id, ?
                FROM reconciliation.gl_mapping_rule g
                WHERE g.gl_mapping_rule_id = ? AND g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                """, newId, actor, parseOptionalUuid(glMappingRuleId), tenantUuid);
        if (n == 0) {
            return Map.of("updated", false, "status", "NOT_FOUND");
        }
        writeAudit(tenantUuid, actor, "GL_MAPPING_CLONED", "gl_mapping_rule", newId.toString(), Map.of("fromRuleId", glMappingRuleId));
        return Map.of("glMappingRuleId", newId.toString(), "updated", true, "status", "OK");
    }

    public Map<String, Object> deactivateGlMappingRule(String tenantId, String glMappingRuleId, String actorUserId) {
        Map<String, Object> p = patchGlMappingActive(tenantId, glMappingRuleId, false);
        writeAudit(parseOptionalUuid(tenantId), parseOptionalUuid(actorUserId), "GL_MAPPING_DEACTIVATED", "gl_mapping_rule", glMappingRuleId, Map.of());
        return p;
    }

    public List<Map<String, Object>> glMappingRuleUsage(String glMappingRuleId, int limit) {
        UUID gid = parseOptionalUuid(glMappingRuleId);
        if (gid == null) {
            return List.of();
        }
        int lim = Math.max(1, Math.min(limit, 200));
        return jdbc.queryForList("""
                SELECT
                    ge.gl_entry_reference_code AS "glEntryReferenceCode",
                    CAST(ge.gl_entry_id AS text) AS "glEntryId",
                    ge.debit_amount AS "debitAmount",
                    ge.credit_amount AS "creditAmount",
                    ge.posting_reference AS "postingReference",
                    CAST(ge.posted_at AS text) AS "postedAt",
                    CAST(ge.created_on AS text) AS "createdOn"
                FROM reconciliation.gl_entry ge
                WHERE ge.gl_mapping_rule_id = ?
                ORDER BY ge.created_on DESC
                LIMIT ?
                """, gid, lim);
    }

    public List<Map<String, Object>> exportGlMappingRules(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        return jdbc.queryForList("""
                SELECT
                    CAST(g.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    g.mapping_name AS "mappingName",
                    g.mapping_code AS "mappingCode",
                    g.is_active AS "active",
                    gst.code AS "sourceType",
                    gtx.code AS "transactionType",
                    lob.code AS "lineOfBusiness",
                    jt.code AS "journalType",
                    ic.code AS "itemCategory",
                    pmt.method_type_code AS "paymentMethodCode",
                    pcur.currency_code AS "currencyCode",
                    dg.gl_code AS "debitGlAccountCode",
                    cg.gl_code AS "creditGlAccountCode",
                    tg.gl_code AS "taxGlAccountCode",
                    rg.gl_code AS "refundGlAccountCode",
                    g.match_rank AS "matchRank",
                    g.refund_type AS "refundType",
                    g.auto_post AS "autoPost",
                    g.requires_approval AS "requiresApproval",
                    g.reversal_allowed AS "reversalAllowed",
                    g.effective_from::text AS "effectiveFrom",
                    g.effective_to::text AS "effectiveTo",
                    CAST(g.dimensions_json AS text) AS "dimensionsJson"
                FROM reconciliation.gl_mapping_rule g
                LEFT JOIN reconciliation.lu_gl_source_type gst ON gst.gl_source_type_id = g.gl_source_type_id
                LEFT JOIN reconciliation.lu_gl_transaction_type gtx ON gtx.gl_transaction_type_id = g.gl_transaction_type_id
                LEFT JOIN reconciliation.lu_line_of_business lob ON lob.line_of_business_id = g.line_of_business_id
                LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = g.journal_type_id
                LEFT JOIN items.lu_item_category ic ON ic.item_category_id = g.item_category_id
                LEFT JOIN payment_gateway.lu_payment_gateway_method_type pmt ON pmt.payment_gateway_method_type_id = g.payment_method_type_id
                LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur ON pcur.payment_gateway_currency_type_id = g.payment_currency_type_id
                LEFT JOIN finance.gl_code dg ON dg.gl_code_id = g.debit_gl_code_id
                LEFT JOIN finance.gl_code cg ON cg.gl_code_id = g.credit_gl_code_id
                LEFT JOIN finance.gl_code tg ON tg.gl_code_id = g.tax_gl_code_id
                LEFT JOIN finance.gl_code rg ON rg.gl_code_id = g.refund_gl_code_id
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                ORDER BY g.match_rank, g.mapping_code
                """, tenantUuid);
    }

    public Map<String, Object> importGlMappingRules(String tenantId, List<Map<String, Object>> rows, String actorUserId) {
        int ok = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();
        if (rows == null) {
            return Map.of("imported", 0, "failed", 0, "errors", List.of("No rows"));
        }
        for (int i = 0; i < rows.size(); i++) {
            try {
                upsertGlMappingRule(tenantId, rows.get(i), null, actorUserId);
                ok++;
            } catch (Exception e) {
                failed++;
                errors.add("Row " + i + ": " + e.getMessage());
            }
        }
        writeAudit(parseOptionalUuid(tenantId), parseOptionalUuid(actorUserId), "GL_MAPPING_IMPORT", "gl_mapping_rule", null,
                Map.of("imported", ok, "failed", failed));
        return Map.of("imported", ok, "failed", failed, "errors", errors);
    }

    /**
     * Resolves best GL mapping for a sample transaction and returns a balanced journal preview.
     */
    public Map<String, Object> previewGlMappingJournal(String tenantId, Map<String, Object> sample) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (sample == null) {
            sample = Map.of();
        }
        UUID srcId = luGlSourceTypeId(str(sample, "sourceType"));
        UUID txnId = luGlTransactionTypeId(str(sample, "transactionType"));
        UUID lobId = luLineOfBusinessId(str(sample, "lineOfBusiness"));
        UUID jrId = luJournalTypeId(str(sample, "journalType"));
        UUID itemId = itemCategoryIdByCode(str(sample, "itemCategory"), tenantUuid);
        UUID pmId = paymentMethodTypeIdByCode(str(sample, "paymentMethodCode"));
        UUID curId = paymentCurrencyTypeIdByCode(str(sample, "currencyCode"));
        String asOf = str(sample, "asOfDate");
        BigDecimal amount = dec(sample, "amount");
        BigDecimal taxAmount = dec(sample, "taxAmount");
        BigDecimal refundAmount = dec(sample, "refundAmount");
        if (amount == null) {
            amount = BigDecimal.ZERO;
        }
        if (taxAmount == null) {
            taxAmount = BigDecimal.ZERO;
        }
        if (refundAmount == null) {
            refundAmount = BigDecimal.ZERO;
        }
        String currency = str(sample, "currencyCode");
        if (currency == null || currency.isBlank()) {
            currency = "USD";
        }
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder("""
                WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ? AND g.is_active = true
                """);
        args.add(tenantUuid);
        if (srcId != null) {
            where.append(" AND (g.gl_source_type_id IS NULL OR g.gl_source_type_id = ?) ");
            args.add(srcId);
        }
        if (txnId != null) {
            where.append(" AND (g.gl_transaction_type_id IS NULL OR g.gl_transaction_type_id = ?) ");
            args.add(txnId);
        }
        if (lobId != null) {
            where.append(" AND (g.line_of_business_id IS NULL OR g.line_of_business_id = ?) ");
            args.add(lobId);
        }
        if (jrId != null) {
            where.append(" AND (g.journal_type_id IS NULL OR g.journal_type_id = ?) ");
            args.add(jrId);
        }
        if (itemId != null) {
            where.append(" AND (g.item_category_id IS NULL OR g.item_category_id = ?) ");
            args.add(itemId);
        }
        if (pmId != null) {
            where.append(" AND (g.payment_method_type_id IS NULL OR g.payment_method_type_id = ?) ");
            args.add(pmId);
        }
        if (curId != null) {
            where.append(" AND (g.payment_currency_type_id IS NULL OR g.payment_currency_type_id = ?) ");
            args.add(curId);
        }
        if (asOf != null && !asOf.isBlank()) {
            where.append("""
                     AND (g.effective_from IS NULL OR g.effective_from::date <= ?::date)
                     AND (g.effective_to IS NULL OR g.effective_to::date >= ?::date)
                    """);
            args.add(asOf.trim());
            args.add(asOf.trim());
        } else {
            where.append("""
                     AND (g.effective_from IS NULL OR g.effective_from::date <= CURRENT_DATE)
                     AND (g.effective_to IS NULL OR g.effective_to::date >= CURRENT_DATE)
                    """);
        }
        String sql = """
                SELECT
                    CAST(g.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    g.mapping_name AS "mappingName",
                    g.match_rank AS "matchRank",
                    dg.gl_code AS "debitGlCode",
                    dg.description AS "debitGlDescription",
                    cg.gl_code AS "creditGlCode",
                    cg.description AS "creditGlDescription",
                    tg.gl_code AS "taxGlCode",
                    tg.description AS "taxGlDescription",
                    rg.gl_code AS "refundGlCode",
                    rg.description AS "refundGlDescription"
                FROM reconciliation.gl_mapping_rule g
                LEFT JOIN finance.gl_code dg ON dg.gl_code_id = g.debit_gl_code_id
                LEFT JOIN finance.gl_code cg ON cg.gl_code_id = g.credit_gl_code_id
                LEFT JOIN finance.gl_code tg ON tg.gl_code_id = g.tax_gl_code_id
                LEFT JOIN finance.gl_code rg ON rg.gl_code_id = g.refund_gl_code_id
                """ + where + """
                ORDER BY g.match_rank ASC, g.mapping_code ASC
                LIMIT 1
                """;
        List<Map<String, Object>> hits = jdbc.queryForList(sql, args.toArray());
        if (hits.isEmpty()) {
            Map<String, Object> suggested = new HashMap<>();
            suggested.put("sourceType", str(sample, "sourceType"));
            suggested.put("transactionType", str(sample, "transactionType"));
            suggested.put("lineOfBusiness", str(sample, "lineOfBusiness"));
            suggested.put("paymentMethodCode", str(sample, "paymentMethodCode"));
            suggested.put("currencyCode", str(sample, "currencyCode"));
            suggested.put("journalType", str(sample, "journalType"));
            suggested.put("itemCategory", str(sample, "itemCategory"));
            Map<String, Object> nm = new HashMap<>();
            nm.put("status", "NO_MATCH");
            nm.put("matchedMappingName", null);
            nm.put("matchRank", null);
            nm.put("journalLines", List.of());
            nm.put("balanced", true);
            nm.put("suggestedTemplate", suggested);
            return nm;
        }
        Map<String, Object> rule = hits.get(0);
        String debitCode = Objects.toString(rule.get("debitGlCode"), "");
        String creditCode = Objects.toString(rule.get("creditGlCode"), "");
        String debitDesc = Objects.toString(rule.get("debitGlDescription"), "");
        String creditDesc = Objects.toString(rule.get("creditGlDescription"), "");
        String taxCode = Objects.toString(rule.get("taxGlCode"), null);
        String taxDesc = Objects.toString(rule.get("taxGlDescription"), "");
        String refundCode = Objects.toString(rule.get("refundGlCode"), null);
        String refundDesc = Objects.toString(rule.get("refundGlDescription"), "");
        List<Map<String, Object>> lines = new ArrayList<>();
        if (amount.compareTo(BigDecimal.ZERO) > 0) {
            lines.add(Map.of("side", "DEBIT", "glCode", debitCode, "glDescription", debitDesc, "amount", amount, "currency", currency));
            lines.add(Map.of("side", "CREDIT", "glCode", creditCode, "glDescription", creditDesc, "amount", amount, "currency", currency));
        }
        if (taxAmount.compareTo(BigDecimal.ZERO) > 0 && taxCode != null && !taxCode.isBlank()) {
            lines.add(Map.of("side", "DEBIT", "glCode", taxCode, "glDescription", taxDesc, "amount", taxAmount, "currency", currency));
            lines.add(Map.of("side", "CREDIT", "glCode", creditCode, "glDescription", creditDesc, "amount", taxAmount, "currency", currency));
        }
        if (refundAmount.compareTo(BigDecimal.ZERO) > 0 && refundCode != null && !refundCode.isBlank()) {
            lines.add(Map.of("side", "DEBIT", "glCode", creditCode, "glDescription", creditDesc, "amount", refundAmount, "currency", currency));
            lines.add(Map.of("side", "CREDIT", "glCode", refundCode, "glDescription", refundDesc, "amount", refundAmount, "currency", currency));
        }
        BigDecimal deb = BigDecimal.ZERO;
        BigDecimal cred = BigDecimal.ZERO;
        for (Map<String, Object> ln : lines) {
            if ("DEBIT".equals(ln.get("side"))) {
                deb = deb.add((BigDecimal) ln.get("amount"));
            } else {
                cred = cred.add((BigDecimal) ln.get("amount"));
            }
        }
        boolean balanced = deb.subtract(cred).abs().compareTo(new BigDecimal("0.01")) < 0;
        Map<String, Object> out = new HashMap<>();
        out.put("status", "OK");
        out.put("matchedMappingName", rule.get("mappingName"));
        out.put("glMappingRuleId", rule.get("glMappingRuleId"));
        out.put("matchRank", rule.get("matchRank"));
        out.put("journalLines", lines);
        out.put("balanced", balanced);
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
        StringBuilder where = new StringBuilder(" WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? ");
        args.add(tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND s.is_active = true ");
        }
        appendSearchSchedule(where, args, search);
        if (loc != null) {
            where.append("""
                     AND (
                        NULLIF(TRIM(s.execution_json->>'locationId'), '') IS NULL
                        OR NULLIF(TRIM(s.execution_json->>'locationId'), '') = ?
                    )
                    """);
            args.add(loc.toString());
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
                    NULLIF(TRIM(s.execution_json->>'locationId'), '') AS "locationId",
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
        String fromSql = " FROM reconciliation.reco_config_publish p WHERE p.application_id IS NOT DISTINCT FROM ? ";
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
                    reco_config_publish_id, application_id, version_label, published_at, published_by, notes, snapshot_json
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
                SELECT published_at, snapshot_json::text AS snap, application_id
                FROM reconciliation.reco_config_publish WHERE reco_config_publish_id = ?
                """, fromId);
        if (cur.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Map<String, Object>> prev = jdbc.queryForList("""
                SELECT CAST(reco_config_publish_id AS text) AS "recoConfigPublishId", snapshot_json::text AS "snap"
                FROM reconciliation.reco_config_publish
                WHERE application_id IS NOT DISTINCT FROM ? AND published_at < ?::timestamptz
                ORDER BY published_at DESC LIMIT 1
                """, tenantUuid, cur.get(0).get("published_at"));
        if (prev.isEmpty()) {
            return Map.of("status", "NO_PRIOR_PUBLISH", "fromPublishId", fromPublishId);
        }
        UUID newId = UUID.randomUUID();
        String priorSnap = prev.get(0).get("snap") == null ? "{}" : String.valueOf(prev.get(0).get("snap"));
        jdbc.update("""
                INSERT INTO reconciliation.reco_config_publish (
                    reco_config_publish_id, application_id, version_label, published_at, published_by, notes, snapshot_json
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
                "SELECT COUNT(1) FROM reconciliation.matching_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        Long gl = jdbc.queryForObject(
                "SELECT COUNT(1) FROM reconciliation.gl_mapping_rule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        Long sch = jdbc.queryForObject(
                "SELECT COUNT(1) FROM reconciliation.reco_schedule WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                Long.class, tenantUuid);
        String h = Integer.toHexString(Objects.hash(mr, gl, sch, String.valueOf(tenantUuid)));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("matchingRulesCount", mr == null ? 0 : mr);
        out.put("glMappingRulesCount", gl == null ? 0 : gl);
        out.put("schedulesCount", sch == null ? 0 : sch);
        out.put("draftFingerprint", h);
        out.put("tenantId", tenantUuid == null ? null : tenantUuid.toString());
        out.put("applicationId", tenantUuid == null ? null : tenantUuid.toString());
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
                WHERE v.deleted_on IS NULL AND v.application_id IS NOT DISTINCT FROM ?
                ORDER BY v.threshold_code
                """, tenantUuid);
        List<Map<String, Object>> sla = jdbc.queryForList("""
                SELECT CAST(reco_sla_threshold_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       settlement_delay_hours AS "settlementDelayHours", refund_sla_hours AS "refundSlaHours",
                       exception_aging_hours AS "exceptionAgingHours", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_sla_threshold WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid);
        List<Map<String, Object>> op = jdbc.queryForList("""
                SELECT CAST(reco_operational_threshold_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       pending_invoice_timeout_minutes AS "pendingInvoiceTimeoutMinutes", max_retry_count AS "maxRetryCount",
                       duplicate_event_window_minutes AS "duplicateEventWindowMinutes", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_operational_threshold WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                ORDER BY policy_code
                """, tenantUuid);
        List<Map<String, Object>> esc = jdbc.queryForList("""
                SELECT CAST(reco_escalation_policy_id AS text) AS "id", policy_name AS "policyName", policy_code AS "policyCode",
                       auto_escalate AS "autoEscalate", escalate_after_hours AS "escalateAfterHours",
                       escalation_role_code AS "escalationRoleCode", is_active AS "active",
                       effective_from::text AS "effectiveFrom", effective_to::text AS "effectiveTo"
                FROM reconciliation.reco_escalation_policy WHERE deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
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
        return List.of();
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
                WHERE n.deleted_on IS NULL AND n.application_id IS NOT DISTINCT FROM ?
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
                "SELECT 1 FROM reconciliation.matching_rule WHERE matching_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
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
                        matching_rule_id, application_id, rule_name, rule_code, reco_domain_id, reconciliation_stage_id,
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
            OperandBinding left = resolveOperandBinding(str(c, "leftOperand"), true, "leftOperand");
            OperandBinding right = resolveOperandBinding(str(c, "rightOperand"), false, "rightOperand");
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_criterion (
                        matching_rule_id, line_order, matching_operator_id,
                        tolerance_amount, tolerance_kind_id, match_kind_id, is_required, extra_json,
                        left_operand_id, right_operand_id, left_operand_type, right_operand_type,
                        left_operand_value, right_operand_value
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)
                    """, ruleId, line, opId,
                    dec(c, "toleranceAmount"), tkId, mkId,
                    bool(c, "required", true), toJson(c.get("extra") == null ? Map.of() : c.get("extra")),
                    left.operandId(), right.operandId(), left.operandType(), right.operandType(),
                    left.operandValue(), right.operandValue());
            order++;
        }
        if (scopes != null && !scopes.isEmpty()) {
            Map<String, Object> s = scopes.get(0);
            if (scopes.size() > 1) {
                Map<String, Object> merged = new LinkedHashMap<>(s);
                merged.put("_additionalScopes", scopes.subList(1, scopes.size()));
                s = merged;
            }
            Map<String, Object> sj = new LinkedHashMap<>();
            Object rawSj = s.get("scopeJson");
            if (rawSj instanceof Map<?, ?> rm) {
                @SuppressWarnings("unchecked")
                Map<String, Object> cast = (Map<String, Object>) rm;
                sj.putAll(cast);
            }
            if (str(s, "locationId") != null && !str(s, "locationId").isBlank()) {
                sj.put("locationId", str(s, "locationId"));
            }
            if (str(s, "gatewayCode") != null) {
                sj.put("gatewayCode", str(s, "gatewayCode"));
            }
            if (str(s, "paymentMethodCode") != null) {
                sj.put("paymentMethodCode", str(s, "paymentMethodCode"));
            }
            if (str(s, "currencyCode") != null) {
                sj.put("currencyCode", str(s, "currencyCode"));
            }
            sj.put("includeChildLocations", bool(s, "includeChildLocations", true));
            jdbc.update("""
                    INSERT INTO reconciliation.matching_rule_scope (
                        matching_rule_id, level_id, source_system_id, scope_json,
                        payment_gateway_id, payment_gateway_supported_currency_id, payment_gateway_supported_method_id
                    ) VALUES (?, ?, ?, ?::jsonb, ?, ?, ?)
                    """, ruleId,
                    parseOptionalUuid(str(s, "locationLevelId")),
                    sourceSystemIdByCode(str(s, "sourceSystem")),
                    toJson(sj),
                    parseOptionalUuid(str(s, "paymentGatewayId")),
                    parseOptionalUuid(str(s, "paymentGatewaySupportedCurrencyId")),
                    parseOptionalUuid(str(s, "paymentGatewaySupportedMethodId")));
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
                WHERE matching_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
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
                WHERE matching_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
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
        String debitCode = Objects.requireNonNull(str(m, "debitGlAccountCode"), "debitGlAccountCode is required");
        String creditCode = Objects.requireNonNull(str(m, "creditGlAccountCode"), "creditGlAccountCode is required");
        int rank = intVal(m, "matchRank") == null ? 100 : intVal(m, "matchRank");
        if (rank < 1) {
            throw new IllegalArgumentException("matchRank must be >= 1");
        }
        UUID debitGlId = glCodeIdForApplication(debitCode, tenantUuid);
        UUID creditGlId = glCodeIdForApplication(creditCode, tenantUuid);
        if (debitGlId == null) {
            throw new IllegalArgumentException("Unknown debit GL code: " + debitCode);
        }
        if (creditGlId == null) {
            throw new IllegalArgumentException("Unknown credit GL code: " + creditCode);
        }
        if (debitGlId.equals(creditGlId)) {
            throw new IllegalArgumentException("debitGlAccountCode and creditGlAccountCode must differ");
        }
        if (!isUpdate) {
            Long dup = jdbc.queryForObject("""
                    SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                    WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                      AND UPPER(g.mapping_code) = UPPER(?)
                    """, Long.class, tenantUuid, code.trim());
            if (dup != null && dup > 0) {
                throw new IllegalArgumentException("mappingCode already exists for this application: " + code);
            }
        } else {
            Long dup = jdbc.queryForObject("""
                    SELECT COUNT(1) FROM reconciliation.gl_mapping_rule g
                    WHERE g.deleted_on IS NULL AND g.application_id IS NOT DISTINCT FROM ?
                      AND UPPER(g.mapping_code) = UPPER(?)
                      AND g.gl_mapping_rule_id <> ?
                    """, Long.class, tenantUuid, code.trim(), id);
            if (dup != null && dup > 0) {
                throw new IllegalArgumentException("mappingCode already exists for this application: " + code);
            }
        }
        UUID taxGlId = glCodeIdForApplication(str(m, "taxGlAccountCode"), tenantUuid);
        UUID refundGlId = glCodeIdForApplication(str(m, "refundGlAccountCode"), tenantUuid);
        UUID glSourceTypeId = luGlSourceTypeId(str(m, "sourceType"));
        UUID glTxnTypeId = luGlTransactionTypeId(str(m, "transactionType"));
        UUID lobId = luLineOfBusinessId(str(m, "lineOfBusiness"));
        UUID journalTypeId = luJournalTypeId(str(m, "journalType"));
        UUID itemCatId = itemCategoryIdByCode(str(m, "itemCategory"), tenantUuid);
        UUID payMethodId = paymentMethodTypeIdByCode(str(m, "paymentMethodCode"));
        UUID payCurId = paymentCurrencyTypeIdByCode(str(m, "currencyCode"));
        if (isUpdate) {
            int u = jdbc.update("""
                    UPDATE reconciliation.gl_mapping_rule SET
                        mapping_name = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                        refund_type = ?, auto_post = ?, requires_approval = ?, reversal_allowed = ?,
                        match_rank = ?, dimensions_json = ?::jsonb,
                        debit_gl_code_id = ?, credit_gl_code_id = ?, tax_gl_code_id = ?, refund_gl_code_id = ?,
                        gl_source_type_id = ?, gl_transaction_type_id = ?, line_of_business_id = ?, journal_type_id = ?,
                        item_category_id = ?, payment_method_type_id = ?, payment_currency_type_id = ?,
                        modified_on = now(), modified_by = ?
                    WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, name, bool(m, "active", true), str(m, "effectiveFrom"), str(m, "effectiveTo"),
                    str(m, "refundType"),
                    bool(m, "autoPost", false), bool(m, "requiresApproval", true), bool(m, "reversalAllowed", true),
                    rank,
                    toJson(m.get("dimensionsJson") == null ? Map.of() : m.get("dimensionsJson")),
                    debitGlId, creditGlId, taxGlId, refundGlId,
                    glSourceTypeId, glTxnTypeId, lobId, journalTypeId,
                    itemCatId, payMethodId, payCurId,
                    actor, id, tenantUuid);
            if (u == 0) {
                return Map.of("updated", false, "glMappingRuleId", idOrNull);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.gl_mapping_rule (
                        gl_mapping_rule_id, application_id, mapping_name, mapping_code, is_active, effective_from, effective_to,
                        refund_type, auto_post, requires_approval, reversal_allowed, match_rank, dimensions_json,
                        debit_gl_code_id, credit_gl_code_id, tax_gl_code_id, refund_gl_code_id,
                        gl_source_type_id, gl_transaction_type_id, line_of_business_id, journal_type_id,
                        item_category_id, payment_method_type_id, payment_currency_type_id, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?::date, ?::date, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, id, tenantUuid, name, code, bool(m, "active", true), str(m, "effectiveFrom"), str(m, "effectiveTo"),
                    str(m, "refundType"),
                    bool(m, "autoPost", false), bool(m, "requiresApproval", true), bool(m, "reversalAllowed", true),
                    rank,
                    toJson(m.get("dimensionsJson") == null ? Map.of() : m.get("dimensionsJson")),
                    debitGlId, creditGlId, taxGlId, refundGlId,
                    glSourceTypeId, glTxnTypeId, lobId, journalTypeId,
                    itemCatId, payMethodId, payCurId, actor);
        }
        writeAudit(tenantUuid, actor, isUpdate ? "GL_MAPPING_UPDATED" : "GL_MAPPING_CREATED", "gl_mapping_rule", id.toString(), Map.of("mappingCode", code));
        return Map.of("glMappingRuleId", id.toString(), "updated", true);
    }

    public Map<String, Object> patchGlMappingActive(String tenantId, String id, boolean active) {
        UUID gid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.gl_mapping_rule SET is_active = ?, modified_on = now() WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                active, gid, tenantUuid);
        return Map.of("updated", n > 0, "glMappingRuleId", id, "active", active);
    }

    public Map<String, Object> softDeleteGlMappingRule(String tenantId, String id) {
        UUID gid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.gl_mapping_rule SET deleted_on = now() WHERE gl_mapping_rule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
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
        Map<String, Object> exec = new LinkedHashMap<>();
        Object ej = m.get("executionJson");
        if (ej instanceof Map<?, ?> mm) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cast = (Map<String, Object>) mm;
            exec.putAll(cast);
        }
        if (str(m, "locationId") != null && !str(m, "locationId").isBlank()) {
            exec.put("locationId", str(m, "locationId"));
        }
        if (str(m, "gatewayCode") != null) {
            exec.put("gatewayCode", str(m, "gatewayCode"));
        }
        exec.put("includeChildLocations", bool(m, "includeChildLocations", true));
        String execJson = toJson(exec);
        UUID levelId = parseOptionalUuid(str(m, "locationLevelId"));
        UUID paymentGatewayId = parseOptionalUuid(str(m, "paymentGatewayId"));
        if (isUpdate) {
            int u = jdbc.update("""
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
                    bool(m, "notifyOnFailure", true), bool(m, "notifyOnCompletion", false),
                    toJson(m.get("notificationChannelsJson") == null ? List.of() : m.get("notificationChannelsJson")),
                    execJson,
                    actor, id, tenantUuid);
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
                    bool(m, "notifyOnFailure", true), bool(m, "notifyOnCompletion", false),
                    toJson(m.get("notificationChannelsJson") == null ? List.of() : m.get("notificationChannelsJson")),
                    execJson, actor);
        }
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_SCHEDULE_UPDATED" : "RECO_SCHEDULE_CREATED", "reco_schedule", id.toString(), Map.of("scheduleCode", code));
        return Map.of("scheduleId", id.toString(), "updated", true);
    }

    public Map<String, Object> patchScheduleActive(String tenantId, String id, boolean active) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.reco_schedule SET is_active = ?, modified_on = now() WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                active, sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id, "active", active);
    }

    public Map<String, Object> softDeleteSchedule(String tenantId, String id) {
        UUID sid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("UPDATE reconciliation.reco_schedule SET deleted_on = now() WHERE reco_schedule_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?",
                sid, tenantUuid);
        return Map.of("updated", n > 0, "scheduleId", id);
    }

    // --- Enterprise thresholds (relational) — see docs/RECONCILIATION_THRESHOLDS_ENTERPRISE_API.md ---

    private static String normalizeThresholdKind(String kind) {
        if (kind == null || kind.isBlank()) {
            throw new IllegalArgumentException("kind is required");
        }
        return switch (kind.trim().toLowerCase()) {
            case "variance", "sla", "operational", "escalation" -> kind.trim().toLowerCase();
            default -> throw new IllegalArgumentException("Unsupported kind: " + kind);
        };
    }

    private Map<String, Object> thresholdKpisStub(String kind, UUID tenantUuid) {
        Map<String, Object> k = new HashMap<>();
        if (tenantUuid == null) {
            k.put("activeThresholds", 0L);
            k.put("thresholdViolationsToday", 0L);
            k.put("escalatedExceptions", 0L);
            k.put("slaBreaches", 0L);
            k.put("duplicateEventsBlocked", 0L);
            k.put("autoResolvedExceptions", 0L);
            return k;
        }
        long active = switch (kind) {
            case "variance" -> countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_variance_threshold t
                    WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? AND t.is_active = true
                    """, tenantUuid);
            case "sla" -> countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_sla_threshold t
                    WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? AND t.is_active = true
                    """, tenantUuid);
            case "operational" -> countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_operational_threshold t
                    WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? AND t.is_active = true
                    """, tenantUuid);
            case "escalation" -> countOrZero("""
                    SELECT COUNT(1) FROM reconciliation.reco_escalation_policy t
                    WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? AND t.is_active = true
                    """, tenantUuid);
            default -> 0L;
        };
        k.put("activeThresholds", active);
        k.put("thresholdViolationsToday", 0L);
        k.put("escalatedExceptions", 0L);
        k.put("slaBreaches", 0L);
        k.put("duplicateEventsBlocked", 0L);
        k.put("autoResolvedExceptions", 0L);
        return k;
    }

    private long countOrZero(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0L : n;
    }

    private List<Map<String, Object>> varianceConflictWarnings(UUID tenantUuid) {
        if (tenantUuid == null) {
            return List.of();
        }
        List<Map<String, Object>> dupes = jdbc.queryForList("""
                SELECT d.code AS "recoDomain", sev.code AS "severity", COUNT(*)::bigint AS "cnt",
                       string_agg(CAST(v.reco_variance_threshold_id AS text), ',' ORDER BY v.threshold_code) AS "ids"
                FROM reconciliation.reco_variance_threshold v
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = v.reco_domain_id
                JOIN reconciliation.lu_severity sev ON sev.severity_id = v.severity_id
                WHERE v.deleted_on IS NULL AND v.application_id IS NOT DISTINCT FROM ? AND v.is_active = true
                GROUP BY v.reco_domain_id, v.severity_id, d.code, sev.code
                HAVING COUNT(*) > 1
                """, tenantUuid);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> row : dupes) {
            String ids = Objects.toString(row.get("ids"), "");
            String[] parts = ids.split(",", 3);
            Map<String, Object> w = new HashMap<>();
            w.put("message", "Multiple ACTIVE variance thresholds share recoDomain="
                    + row.get("recoDomain") + " and severity=" + row.get("severity") + " — review priority / effective dates.");
            w.put("thresholdIds", List.of(parts));
            w.put("winnerThresholdId", parts.length > 0 ? parts[0] : null);
            w.put("reason", "FIRST_BY_CODE_ORDER");
            w.put("summary", w.get("message"));
            out.add(w);
        }
        return out;
    }

    private List<Map<String, Object>> thresholdConflictWarnings(String kind, UUID tenantUuid) {
        if (tenantUuid == null) {
            return List.of();
        }
        if ("variance".equals(kind)) {
            return varianceConflictWarnings(tenantUuid);
        }
        return List.of();
    }

    public Map<String, Object> listEnterpriseThresholdsByKind(
            String kindRaw,
            String tenantId,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String applicationId,
            String recoDomain,
            String stage,
            String level,
            String gatewayCode,
            String currencyCode,
            String paymentMethodCode,
            String severity,
            String status,
            String effectiveFrom,
            String effectiveTo
    ) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID appFilter = parseOptionalUuid(applicationId);
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        return switch (kind) {
            case "variance" -> listVarianceThresholdsPaged(tenantUuid, appFilter, activeOnly, search, recoDomain, severity, status, effectiveFrom, effectiveTo, p, ps, offset);
            case "sla" -> listSlaThresholdsPaged(tenantUuid, appFilter, activeOnly, search, status, effectiveFrom, effectiveTo, p, ps, offset);
            case "operational" -> listOperationalThresholdsPaged(tenantUuid, appFilter, activeOnly, search, status, effectiveFrom, effectiveTo, p, ps, offset);
            case "escalation" -> listEscalationPoliciesPaged(tenantUuid, appFilter, activeOnly, search, status, effectiveFrom, effectiveTo, p, ps, offset);
            default -> throw new IllegalArgumentException("Unsupported kind");
        };
    }

    private Map<String, Object> listVarianceThresholdsPaged(
            UUID tenantUuid, UUID appFilter, Boolean activeOnly, String search, String recoDomain, String severity,
            String status, String effectiveFrom, String effectiveTo, int p, int ps, int offset
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE v.deleted_on IS NULL ");
        where.append(" AND v.application_id IS NOT DISTINCT FROM ? ");
        args.add(appFilter != null ? appFilter : tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND v.is_active = true ");
        }
        if (status != null && !status.isBlank()) {
            if ("ACTIVE".equalsIgnoreCase(status.trim())) {
                where.append(" AND v.is_active = true ");
            } else if ("INACTIVE".equalsIgnoreCase(status.trim()) || "DRAFT".equalsIgnoreCase(status.trim())) {
                where.append(" AND v.is_active = false ");
            }
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (v.threshold_name ILIKE ? ESCAPE '\\' OR COALESCE(v.threshold_code,'') ILIKE ? ESCAPE '\\' OR COALESCE(d.code,'') ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
            args.add(q);
        }
        if (recoDomain != null && !recoDomain.isBlank()) {
            where.append(" AND UPPER(COALESCE(d.code,'')) = UPPER(?) ");
            args.add(recoDomain.trim());
        }
        if (severity != null && !severity.isBlank()) {
            where.append(" AND UPPER(sev.code) = UPPER(?) ");
            args.add(severity.trim());
        }
        appendEffectiveOverlap(where, args, effectiveFrom, effectiveTo, "v");
        String from = """
                FROM reconciliation.reco_variance_threshold v
                LEFT JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = v.reco_domain_id
                JOIN reconciliation.lu_severity sev ON sev.severity_id = v.severity_id
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + from, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(v.reco_variance_threshold_id AS text) AS "varianceThresholdId",
                       CAST(v.reco_variance_threshold_id AS text) AS "thresholdId",
                       CAST(v.reco_variance_threshold_id AS text) AS "id",
                       v.threshold_name AS "thresholdName",
                       v.threshold_code AS "thresholdCode",
                       CAST(v.application_id AS text) AS "applicationId",
                       d.code AS "recoDomain",
                       CAST(NULL AS text) AS "stage",
                       CAST(NULL AS text) AS "level",
                       CAST(NULL AS text) AS "gatewayCode",
                       CAST(NULL AS text) AS "currencyCode",
                       CAST(NULL AS text) AS "paymentMethodCode",
                       v.min_amount AS "minVarianceAmount",
                       v.max_amount AS "maxVarianceAmount",
                       CAST(NULL AS text) AS "varianceType",
                       CAST(NULL AS numeric) AS "variancePercentage",
                       CAST(NULL AS int) AS "toleranceWindowHours",
                       sev.code AS "defaultSeverity",
                       v.is_active AS "active",
                       CASE WHEN v.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                       v.effective_from::text AS "effectiveFrom",
                       v.effective_to::text AS "effectiveTo",
                       CAST(NULL AS boolean) AS "autoResolve",
                       CAST(NULL AS boolean) AS "createException",
                       CAST(NULL AS boolean) AS "retryEligible",
                       CAST(NULL AS int) AS "retryCount",
                       CAST(NULL AS text) AS "description"
                """ + from + """
                ORDER BY v.threshold_code NULLS LAST, v.threshold_name
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapThresholdList("variance", items, total, p, ps, tenantUuid);
    }

    private void appendEffectiveOverlap(StringBuilder sql, List<Object> args, String effectiveFrom, String effectiveTo, String alias) {
        if (effectiveFrom != null && !effectiveFrom.isBlank() && effectiveTo != null && !effectiveTo.isBlank()) {
            sql.append(" AND (").append(alias).append(".effective_to IS NULL OR ").append(alias).append(".effective_to::date >= ?::date) ");
            sql.append(" AND (").append(alias).append(".effective_from IS NULL OR ").append(alias).append(".effective_from::date <= ?::date) ");
            args.add(effectiveFrom.trim());
            args.add(effectiveTo.trim());
        } else if (effectiveFrom != null && !effectiveFrom.isBlank()) {
            sql.append(" AND (").append(alias).append(".effective_to IS NULL OR ").append(alias).append(".effective_to::date >= ?::date) ");
            sql.append(" AND (").append(alias).append(".effective_from IS NULL OR ").append(alias).append(".effective_from::date <= ?::date) ");
            args.add(effectiveFrom.trim());
            args.add(effectiveFrom.trim());
        } else if (effectiveTo != null && !effectiveTo.isBlank()) {
            sql.append(" AND (").append(alias).append(".effective_to IS NULL OR ").append(alias).append(".effective_to::date >= ?::date) ");
            sql.append(" AND (").append(alias).append(".effective_from IS NULL OR ").append(alias).append(".effective_from::date <= ?::date) ");
            args.add(effectiveTo.trim());
            args.add(effectiveTo.trim());
        }
    }

    private Map<String, Object> wrapThresholdList(String kind, List<Map<String, Object>> items, Long total, int page, int pageSize, UUID tenantUuid) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("total", total == null ? 0L : total);
        meta.put("page", page);
        meta.put("pageSize", pageSize);
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        out.put("kpis", thresholdKpisStub(kind, tenantUuid));
        out.put("conflictWarnings", thresholdConflictWarnings(kind, tenantUuid));
        return out;
    }

    private Map<String, Object> listSlaThresholdsPaged(
            UUID tenantUuid, UUID appFilter, Boolean activeOnly, String search, String status,
            String effectiveFrom, String effectiveTo, int p, int ps, int offset
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? ");
        args.add(appFilter != null ? appFilter : tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND t.is_active = true ");
        }
        if (status != null && !status.isBlank()) {
            if ("ACTIVE".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = true ");
            } else if ("INACTIVE".equalsIgnoreCase(status.trim()) || "DRAFT".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = false ");
            }
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (t.policy_name ILIKE ? ESCAPE '\\' OR t.policy_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        appendEffectiveOverlap(where, args, effectiveFrom, effectiveTo, "t");
        String from = " FROM reconciliation.reco_sla_threshold t " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + from, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(t.reco_sla_threshold_id AS text) AS "slaThresholdId",
                       CAST(t.reco_sla_threshold_id AS text) AS "thresholdId",
                       CAST(t.reco_sla_threshold_id AS text) AS "id",
                       t.policy_name AS "policyName",
                       t.policy_code AS "policyCode",
                       CAST(t.application_id AS text) AS "applicationId",
                       t.settlement_delay_hours AS "settlementDelayHours",
                       t.refund_sla_hours AS "refundSlaHours",
                       t.exception_aging_hours AS "exceptionAgingHours",
                       CAST(NULL AS int) AS "warningThresholdHours",
                       CAST(NULL AS int) AS "criticalThresholdHours",
                       CAST(NULL AS boolean) AS "autoEscalate",
                       CAST(NULL AS boolean) AS "createException",
                       CAST(NULL AS text) AS "escalationSeverity",
                       CAST(NULL AS text) AS "description",
                       t.is_active AS "active",
                       CASE WHEN t.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                       t.effective_from::text AS "effectiveFrom",
                       t.effective_to::text AS "effectiveTo"
                """ + from + """
                ORDER BY t.policy_code
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapThresholdList("sla", items, total, p, ps, tenantUuid);
    }

    private Map<String, Object> listOperationalThresholdsPaged(
            UUID tenantUuid, UUID appFilter, Boolean activeOnly, String search, String status,
            String effectiveFrom, String effectiveTo, int p, int ps, int offset
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? ");
        args.add(appFilter != null ? appFilter : tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND t.is_active = true ");
        }
        if (status != null && !status.isBlank()) {
            if ("ACTIVE".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = true ");
            } else if ("INACTIVE".equalsIgnoreCase(status.trim()) || "DRAFT".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = false ");
            }
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (t.policy_name ILIKE ? ESCAPE '\\' OR t.policy_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        appendEffectiveOverlap(where, args, effectiveFrom, effectiveTo, "t");
        String from = " FROM reconciliation.reco_operational_threshold t " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + from, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(t.reco_operational_threshold_id AS text) AS "operationalThresholdId",
                       CAST(t.reco_operational_threshold_id AS text) AS "thresholdId",
                       CAST(t.reco_operational_threshold_id AS text) AS "id",
                       t.policy_name AS "policyName",
                       t.policy_code AS "policyCode",
                       CAST(t.application_id AS text) AS "applicationId",
                       t.pending_invoice_timeout_minutes AS "pendingInvoiceTimeoutMinutes",
                       t.max_retry_count AS "maxRetryCount",
                       t.duplicate_event_window_minutes AS "duplicateEventWindowMinutes",
                       CAST(NULL AS int) AS "retryBackoffSeconds",
                       CAST(NULL AS int) AS "orphanTransactionTimeoutMinutes",
                       CAST(NULL AS int) AS "settlementWaitWindowMinutes",
                       CAST(NULL AS boolean) AS "autoRetry",
                       CAST(NULL AS boolean) AS "autoEscalate",
                       CAST(NULL AS boolean) AS "createException",
                       CAST(NULL AS text) AS "defaultSeverity",
                       CAST(NULL AS boolean) AS "deadLetterQueueEnabled",
                       CAST(NULL AS text) AS "description",
                       t.is_active AS "active",
                       CASE WHEN t.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                       t.effective_from::text AS "effectiveFrom",
                       t.effective_to::text AS "effectiveTo"
                """ + from + """
                ORDER BY t.policy_code
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapThresholdList("operational", items, total, p, ps, tenantUuid);
    }

    private Map<String, Object> listEscalationPoliciesPaged(
            UUID tenantUuid, UUID appFilter, Boolean activeOnly, String search, String status,
            String effectiveFrom, String effectiveTo, int p, int ps, int offset
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ? ");
        args.add(appFilter != null ? appFilter : tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND t.is_active = true ");
        }
        if (status != null && !status.isBlank()) {
            if ("ACTIVE".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = true ");
            } else if ("INACTIVE".equalsIgnoreCase(status.trim()) || "DRAFT".equalsIgnoreCase(status.trim())) {
                where.append(" AND t.is_active = false ");
            }
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (t.policy_name ILIKE ? ESCAPE '\\' OR t.policy_code ILIKE ? ESCAPE '\\' OR COALESCE(t.escalation_role_code,'') ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
            args.add(q);
        }
        appendEffectiveOverlap(where, args, effectiveFrom, effectiveTo, "t");
        String from = " FROM reconciliation.reco_escalation_policy t " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + from, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(t.reco_escalation_policy_id AS text) AS "escalationPolicyId",
                       CAST(t.reco_escalation_policy_id AS text) AS "policyId",
                       CAST(t.reco_escalation_policy_id AS text) AS "thresholdId",
                       CAST(t.reco_escalation_policy_id AS text) AS "id",
                       t.policy_name AS "policyName",
                       t.policy_code AS "policyCode",
                       CAST(t.application_id AS text) AS "applicationId",
                       t.auto_escalate AS "autoEscalate",
                       t.escalate_after_hours AS "escalateAfterHours",
                       t.escalation_role_code AS "escalationRole",
                       CAST(NULL AS boolean) AS "autoAssign",
                       CAST(NULL AS int) AS "reassignAfterHours",
                       CAST(NULL AS int) AS "maxEscalationLevel",
                       CAST(NULL AS text) AS "description",
                       CAST(NULL AS boolean) AS "emailNotifications",
                       t.is_active AS "active",
                       CASE WHEN t.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                       t.effective_from::text AS "effectiveFrom",
                       t.effective_to::text AS "effectiveTo"
                """ + from + """
                ORDER BY t.policy_code
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapThresholdList("escalation", items, total, p, ps, tenantUuid);
    }

    private String entityTypeForThresholdKind(String kind) {
        return switch (kind) {
            case "variance" -> "reco_variance_threshold";
            case "sla" -> "reco_sla_threshold";
            case "operational" -> "reco_operational_threshold";
            case "escalation" -> "reco_escalation_policy";
            default -> "reco_threshold";
        };
    }

    public Map<String, Object> getEnterpriseThresholdRow(String kindRaw, String tenantId, String id) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID rid = parseOptionalUuid(id);
        if (rid == null) {
            return Map.of("status", "NOT_FOUND");
        }
        Map<String, Object> list = listEnterpriseThresholdsByKind(kind, tenantId, false, null, 1, 500, null, null, null, null, null, null, null, null, null, null, null);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) list.get("items");
        if (items == null) {
            return Map.of("status", "NOT_FOUND");
        }
        for (Map<String, Object> row : items) {
            if (id.equals(row.get("id")) || id.equals(row.get("varianceThresholdId")) || id.equals(row.get("slaThresholdId"))
                    || id.equals(row.get("operationalThresholdId")) || id.equals(row.get("escalationPolicyId"))) {
                return Map.of("status", "OK", "row", row);
            }
        }
        return Map.of("status", "NOT_FOUND");
    }

    public Map<String, Object> upsertEnterpriseThreshold(String kindRaw, String tenantId, Map<String, Object> row, String idOrNull, String actorUserId) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        return switch (kind) {
            case "variance" -> upsertVarianceThreshold(tenantUuid, row, idOrNull, actor);
            case "sla" -> upsertSlaThreshold(tenantUuid, row, idOrNull, actor);
            case "operational" -> upsertOperationalThreshold(tenantUuid, row, idOrNull, actor);
            case "escalation" -> upsertEscalationPolicy(tenantUuid, row, idOrNull, actor);
            default -> throw new IllegalArgumentException("Unsupported kind");
        };
    }

    private Map<String, Object> upsertVarianceThreshold(UUID tenantUuid, Map<String, Object> m, String idOrNull, UUID actor) {
        boolean isUpdate = parseOptionalUuid(idOrNull) != null;
        UUID id = isUpdate ? parseOptionalUuid(idOrNull) : UUID.randomUUID();
        String name = Objects.requireNonNull(resolveThresholdDisplayName(m), "thresholdName is required");
        String code = optionalTrimmedCode(str(m, "thresholdCode"));
        UUID domainId = domainIdByCode(str(m, "recoDomain"));
        BigDecimal minA = dec(m, "minVarianceAmount");
        BigDecimal maxA = dec(m, "maxVarianceAmount");
        UUID sevId = severityIdByCode(str(m, "defaultSeverity"));
        if (sevId == null) {
            sevId = severityIdByCode(str(m, "severity"));
        }
        if (sevId == null) {
            throw new IllegalArgumentException("defaultSeverity (or severity) is required");
        }
        boolean active = bool(m, "active", true);
        String ef = str(m, "effectiveFrom");
        String et = str(m, "effectiveTo");
        if (isUpdate) {
            int u;
            if (code != null) {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_variance_threshold SET
                            threshold_name = ?, threshold_code = ?, reco_domain_id = ?, min_amount = ?, max_amount = ?,
                            severity_id = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                            modified_on = now(), modified_by = ?
                        WHERE reco_variance_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, code, domainId, minA, maxA, sevId, active, ef, et, actor, id, tenantUuid);
            } else {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_variance_threshold SET
                            threshold_name = ?, reco_domain_id = ?, min_amount = ?, max_amount = ?,
                            severity_id = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                            modified_on = now(), modified_by = ?
                        WHERE reco_variance_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, domainId, minA, maxA, sevId, active, ef, et, actor, id, tenantUuid);
            }
            if (u == 0) {
                return Map.of("updated", false);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_variance_threshold (
                        reco_variance_threshold_id, application_id, threshold_name, threshold_code,
                        reco_domain_id, min_amount, max_amount, severity_id, is_active, effective_from, effective_to, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::date, ?::date, ?)
                    """, id, tenantUuid, name, code, domainId, minA, maxA, sevId, active, ef, et, actor);
        }
        String savedCode = code != null ? code : loadVarianceThresholdCode(id);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_VARIANCE_THRESHOLD_UPDATED" : "RECO_VARIANCE_THRESHOLD_CREATED",
                entityTypeForThresholdKind("variance"), id.toString(), Map.of("thresholdCode", savedCode));
        Map<String, Object> out = new HashMap<>();
        out.put("id", id.toString());
        out.put("varianceThresholdId", id.toString());
        out.put("thresholdCode", savedCode);
        out.put("updated", true);
        return out;
    }

    private Map<String, Object> upsertSlaThreshold(UUID tenantUuid, Map<String, Object> m, String idOrNull, UUID actor) {
        boolean isUpdate = parseOptionalUuid(idOrNull) != null;
        UUID id = isUpdate ? parseOptionalUuid(idOrNull) : UUID.randomUUID();
        String name = Objects.requireNonNull(resolveThresholdDisplayName(m), "policyName is required");
        String code = optionalTrimmedCode(str(m, "policyCode"));
        Integer sd = intVal(m, "settlementDelayHours");
        Integer rh = intVal(m, "refundSlaHours");
        Integer ea = intVal(m, "exceptionAgingHours");
        boolean active = bool(m, "active", true);
        String ef = str(m, "effectiveFrom");
        String et = str(m, "effectiveTo");
        if (isUpdate) {
            int u;
            if (code != null) {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_sla_threshold SET
                            policy_name = ?, policy_code = ?, settlement_delay_hours = ?, refund_sla_hours = ?, exception_aging_hours = ?,
                            is_active = ?, effective_from = ?::date, effective_to = ?::date, modified_on = now(), modified_by = ?
                        WHERE reco_sla_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, code, sd, rh, ea, active, ef, et, actor, id, tenantUuid);
            } else {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_sla_threshold SET
                            policy_name = ?, settlement_delay_hours = ?, refund_sla_hours = ?, exception_aging_hours = ?,
                            is_active = ?, effective_from = ?::date, effective_to = ?::date, modified_on = now(), modified_by = ?
                        WHERE reco_sla_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, sd, rh, ea, active, ef, et, actor, id, tenantUuid);
            }
            if (u == 0) {
                return Map.of("updated", false);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_sla_threshold (
                        reco_sla_threshold_id, application_id, policy_name, policy_code,
                        settlement_delay_hours, refund_sla_hours, exception_aging_hours, is_active, effective_from, effective_to, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::date, ?::date, ?)
                    """, id, tenantUuid, name, code, sd, rh, ea, active, ef, et, actor);
        }
        String savedCode = code != null ? code : loadPolicyCode("reco_sla_threshold", "reco_sla_threshold_id", id);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_SLA_THRESHOLD_UPDATED" : "RECO_SLA_THRESHOLD_CREATED",
                entityTypeForThresholdKind("sla"), id.toString(), Map.of("policyCode", savedCode));
        Map<String, Object> out = new HashMap<>();
        out.put("id", id.toString());
        out.put("slaThresholdId", id.toString());
        out.put("policyCode", savedCode);
        out.put("updated", true);
        return out;
    }

    private Map<String, Object> upsertOperationalThreshold(UUID tenantUuid, Map<String, Object> m, String idOrNull, UUID actor) {
        boolean isUpdate = parseOptionalUuid(idOrNull) != null;
        UUID id = isUpdate ? parseOptionalUuid(idOrNull) : UUID.randomUUID();
        String name = Objects.requireNonNull(resolveThresholdDisplayName(m), "policyName is required");
        String code = optionalTrimmedCode(str(m, "policyCode"));
        Integer pending = intVal(m, "pendingInvoiceTimeoutMinutes");
        Integer maxR = intVal(m, "maxRetryCount");
        Integer dup = intVal(m, "duplicateEventWindowMinutes");
        boolean active = bool(m, "active", true);
        String ef = str(m, "effectiveFrom");
        String et = str(m, "effectiveTo");
        if (isUpdate) {
            int u;
            if (code != null) {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_operational_threshold SET
                            policy_name = ?, policy_code = ?, pending_invoice_timeout_minutes = ?, max_retry_count = ?,
                            duplicate_event_window_minutes = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                            modified_on = now(), modified_by = ?
                        WHERE reco_operational_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, code, pending, maxR, dup, active, ef, et, actor, id, tenantUuid);
            } else {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_operational_threshold SET
                            policy_name = ?, pending_invoice_timeout_minutes = ?, max_retry_count = ?,
                            duplicate_event_window_minutes = ?, is_active = ?, effective_from = ?::date, effective_to = ?::date,
                            modified_on = now(), modified_by = ?
                        WHERE reco_operational_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, pending, maxR, dup, active, ef, et, actor, id, tenantUuid);
            }
            if (u == 0) {
                return Map.of("updated", false);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_operational_threshold (
                        reco_operational_threshold_id, application_id, policy_name, policy_code,
                        pending_invoice_timeout_minutes, max_retry_count, duplicate_event_window_minutes,
                        is_active, effective_from, effective_to, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::date, ?::date, ?)
                    """, id, tenantUuid, name, code, pending, maxR, dup, active, ef, et, actor);
        }
        String savedCode = code != null ? code : loadPolicyCode("reco_operational_threshold", "reco_operational_threshold_id", id);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_OPERATIONAL_THRESHOLD_UPDATED" : "RECO_OPERATIONAL_THRESHOLD_CREATED",
                entityTypeForThresholdKind("operational"), id.toString(), Map.of("policyCode", savedCode));
        Map<String, Object> out = new HashMap<>();
        out.put("id", id.toString());
        out.put("operationalThresholdId", id.toString());
        out.put("policyCode", savedCode);
        out.put("updated", true);
        return out;
    }

    private Map<String, Object> upsertEscalationPolicy(UUID tenantUuid, Map<String, Object> m, String idOrNull, UUID actor) {
        boolean isUpdate = parseOptionalUuid(idOrNull) != null;
        UUID id = isUpdate ? parseOptionalUuid(idOrNull) : UUID.randomUUID();
        String name = Objects.requireNonNull(resolveThresholdDisplayName(m), "policyName is required");
        String code = optionalTrimmedCode(str(m, "policyCode"));
        boolean autoEsc = bool(m, "autoEscalate", false);
        Integer escH = intVal(m, "escalateAfterHours");
        String role = str(m, "escalationRole");
        if (role == null) {
            role = str(m, "escalationRoleCode");
        }
        boolean active = bool(m, "active", true);
        String ef = str(m, "effectiveFrom");
        String et = str(m, "effectiveTo");
        if (isUpdate) {
            int u;
            if (code != null) {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_escalation_policy SET
                            policy_name = ?, policy_code = ?, auto_escalate = ?, escalate_after_hours = ?, escalation_role_code = ?,
                            is_active = ?, effective_from = ?::date, effective_to = ?::date, modified_on = now(), modified_by = ?
                        WHERE reco_escalation_policy_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, code, autoEsc, escH, role, active, ef, et, actor, id, tenantUuid);
            } else {
                u = jdbc.update("""
                        UPDATE reconciliation.reco_escalation_policy SET
                            policy_name = ?, auto_escalate = ?, escalate_after_hours = ?, escalation_role_code = ?,
                            is_active = ?, effective_from = ?::date, effective_to = ?::date, modified_on = now(), modified_by = ?
                        WHERE reco_escalation_policy_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                        """, name, autoEsc, escH, role, active, ef, et, actor, id, tenantUuid);
            }
            if (u == 0) {
                return Map.of("updated", false);
            }
        } else {
            jdbc.update("""
                    INSERT INTO reconciliation.reco_escalation_policy (
                        reco_escalation_policy_id, application_id, policy_name, policy_code,
                        auto_escalate, escalate_after_hours, escalation_role_code, is_active, effective_from, effective_to, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::date, ?::date, ?)
                    """, id, tenantUuid, name, code, autoEsc, escH, role, active, ef, et, actor);
        }
        String savedCode = code != null ? code : loadPolicyCode("reco_escalation_policy", "reco_escalation_policy_id", id);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_ESCALATION_POLICY_UPDATED" : "RECO_ESCALATION_POLICY_CREATED",
                entityTypeForThresholdKind("escalation"), id.toString(), Map.of("policyCode", savedCode));
        Map<String, Object> out = new HashMap<>();
        out.put("id", id.toString());
        out.put("escalationPolicyId", id.toString());
        out.put("policyCode", savedCode);
        out.put("updated", true);
        return out;
    }

    public Map<String, Object> deactivateEnterpriseThreshold(String kindRaw, String tenantId, String id, String actorUserId) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID rid = parseOptionalUuid(id);
        if (rid == null) {
            return Map.of("updated", false);
        }
        int n = switch (kind) {
            case "variance" -> jdbc.update("""
                    UPDATE reconciliation.reco_variance_threshold SET is_active = false, modified_on = now(), modified_by = ?
                    WHERE reco_variance_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, actor, rid, tenantUuid);
            case "sla" -> jdbc.update("""
                    UPDATE reconciliation.reco_sla_threshold SET is_active = false, modified_on = now(), modified_by = ?
                    WHERE reco_sla_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, actor, rid, tenantUuid);
            case "operational" -> jdbc.update("""
                    UPDATE reconciliation.reco_operational_threshold SET is_active = false, modified_on = now(), modified_by = ?
                    WHERE reco_operational_threshold_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, actor, rid, tenantUuid);
            case "escalation" -> jdbc.update("""
                    UPDATE reconciliation.reco_escalation_policy SET is_active = false, modified_on = now(), modified_by = ?
                    WHERE reco_escalation_policy_id = ? AND deleted_on IS NULL AND application_id IS NOT DISTINCT FROM ?
                    """, actor, rid, tenantUuid);
            default -> 0;
        };
        writeAudit(tenantUuid, actor, "RECO_THRESHOLD_DEACTIVATED", entityTypeForThresholdKind(kind), id, Map.of());
        return Map.of("updated", n > 0, "id", id, "status", "INACTIVE", "active", false);
    }

    public Map<String, Object> cloneEnterpriseThreshold(String kindRaw, String tenantId, String id, String actorUserId) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID rid = parseOptionalUuid(id);
        if (rid == null) {
            return Map.of("updated", false);
        }
        UUID newId = UUID.randomUUID();
        int n = switch (kind) {
            case "variance" -> jdbc.update("""
                    INSERT INTO reconciliation.reco_variance_threshold (
                        reco_variance_threshold_id, application_id, threshold_name, threshold_code, reco_domain_id,
                        min_amount, max_amount, severity_id, is_active, effective_from, effective_to, created_by
                    )
                    SELECT ?, v.application_id, v.threshold_name || ' (Copy)',
                           COALESCE(v.threshold_code,'') || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                           v.reco_domain_id, v.min_amount, v.max_amount, v.severity_id, false, v.effective_from, v.effective_to, ?
                    FROM reconciliation.reco_variance_threshold v
                    WHERE v.reco_variance_threshold_id = ? AND v.deleted_on IS NULL AND v.application_id IS NOT DISTINCT FROM ?
                    """, newId, actor, rid, tenantUuid);
            case "sla" -> jdbc.update("""
                    INSERT INTO reconciliation.reco_sla_threshold (
                        reco_sla_threshold_id, application_id, policy_name, policy_code,
                        settlement_delay_hours, refund_sla_hours, exception_aging_hours, is_active, effective_from, effective_to, created_by
                    )
                    SELECT ?, t.application_id, t.policy_name || ' (Copy)',
                           t.policy_code || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                           t.settlement_delay_hours, t.refund_sla_hours, t.exception_aging_hours, false, t.effective_from, t.effective_to, ?
                    FROM reconciliation.reco_sla_threshold t
                    WHERE t.reco_sla_threshold_id = ? AND t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ?
                    """, newId, actor, rid, tenantUuid);
            case "operational" -> jdbc.update("""
                    INSERT INTO reconciliation.reco_operational_threshold (
                        reco_operational_threshold_id, application_id, policy_name, policy_code,
                        pending_invoice_timeout_minutes, max_retry_count, duplicate_event_window_minutes,
                        is_active, effective_from, effective_to, created_by
                    )
                    SELECT ?, t.application_id, t.policy_name || ' (Copy)',
                           t.policy_code || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                           t.pending_invoice_timeout_minutes, t.max_retry_count, t.duplicate_event_window_minutes,
                           false, t.effective_from, t.effective_to, ?
                    FROM reconciliation.reco_operational_threshold t
                    WHERE t.reco_operational_threshold_id = ? AND t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ?
                    """, newId, actor, rid, tenantUuid);
            case "escalation" -> jdbc.update("""
                    INSERT INTO reconciliation.reco_escalation_policy (
                        reco_escalation_policy_id, application_id, policy_name, policy_code,
                        auto_escalate, escalate_after_hours, escalation_role_code, is_active, effective_from, effective_to, created_by
                    )
                    SELECT ?, t.application_id, t.policy_name || ' (Copy)',
                           t.policy_code || '-CL-' || left(replace(gen_random_uuid()::text,'-',''), 8),
                           t.auto_escalate, t.escalate_after_hours, t.escalation_role_code, false, t.effective_from, t.effective_to, ?
                    FROM reconciliation.reco_escalation_policy t
                    WHERE t.reco_escalation_policy_id = ? AND t.deleted_on IS NULL AND t.application_id IS NOT DISTINCT FROM ?
                    """, newId, actor, rid, tenantUuid);
            default -> 0;
        };
        if (n == 0) {
            return Map.of("updated", false);
        }
        writeAudit(tenantUuid, actor, "RECO_THRESHOLD_CLONED", entityTypeForThresholdKind(kind), newId.toString(), Map.of("fromId", id));
        return Map.of("updated", true, "id", newId.toString());
    }

    public Map<String, Object> testEnterpriseThreshold(String kindRaw, String tenantId, Map<String, Object> sample) {
        String kind = normalizeThresholdKind(kindRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (sample == null) {
            sample = Map.of();
        }
        if ("variance".equals(kind)) {
            BigDecimal expected = dec(sample, "expectedAmount");
            BigDecimal actual = dec(sample, "actualAmount");
            if (expected == null) {
                expected = BigDecimal.ZERO;
            }
            if (actual == null) {
                actual = BigDecimal.ZERO;
            }
            BigDecimal variance = expected.subtract(actual).abs();
            String domain = str(sample, "recoDomain");
            UUID domainId = domainIdByCode(domain);
            List<Object> args = new ArrayList<>();
            args.add(tenantUuid);
            StringBuilder w = new StringBuilder("""
                    WHERE v.deleted_on IS NULL AND v.application_id IS NOT DISTINCT FROM ? AND v.is_active = true
                    """);
            if (domainId != null) {
                w.append(" AND (v.reco_domain_id IS NULL OR v.reco_domain_id = ?) ");
                args.add(domainId);
            }
            w.append("""
                     AND (v.effective_from IS NULL OR v.effective_from::date <= CURRENT_DATE)
                     AND (v.effective_to IS NULL OR v.effective_to::date >= CURRENT_DATE)
                    """);
            List<Map<String, Object>> rules = jdbc.queryForList("""
                    SELECT CAST(v.reco_variance_threshold_id AS text) AS "matchedThresholdId",
                           v.threshold_name AS "thresholdName",
                           v.min_amount AS "minVarianceAmount",
                           v.max_amount AS "maxVarianceAmount"
                    FROM reconciliation.reco_variance_threshold v
                    """ + w + """
                    ORDER BY v.threshold_code NULLS LAST, v.threshold_name
                    """, args.toArray());
            Map<String, Object> matched = null;
            for (Map<String, Object> r : rules) {
                BigDecimal min = r.get("minVarianceAmount") instanceof BigDecimal b ? b : null;
                BigDecimal max = r.get("maxVarianceAmount") instanceof BigDecimal b2 ? b2 : null;
                boolean okMin = min == null || variance.compareTo(min) >= 0;
                boolean okMax = max == null || variance.compareTo(max) <= 0;
                if (okMin && okMax) {
                    matched = r;
                    break;
                }
            }
            Map<String, Object> out = new HashMap<>();
            out.put("variance", variance);
            out.put("thresholdMatch", matched != null);
            out.put("matchedThresholdId", matched == null ? null : matched.get("matchedThresholdId"));
            out.put("wouldAutoResolve", false);
            out.put("wouldEscalate", matched == null);
            String sevCode = null;
            if (matched != null) {
                List<Map<String, Object>> sevRows = jdbc.queryForList("""
                        SELECT sev.code AS c FROM reconciliation.reco_variance_threshold v
                        JOIN reconciliation.lu_severity sev ON sev.severity_id = v.severity_id
                        WHERE v.reco_variance_threshold_id = ?::uuid LIMIT 1
                        """, matched.get("matchedThresholdId"));
                if (!sevRows.isEmpty()) {
                    sevCode = Objects.toString(sevRows.get(0).get("c"), null);
                }
            }
            out.put("expectedSeverity", sevCode);
            out.put("evaluationNotes", matched == null
                    ? "No variance threshold band matched the sample for this tenant/domain."
                    : "Matched threshold " + matched.get("thresholdName") + " for |expected-actual|=" + variance);
            return out;
        }
        Map<String, Object> stub = new HashMap<>();
        stub.put("thresholdMatch", false);
        stub.put("evaluationNotes", "Kind-specific evaluation not implemented; sample echoed.");
        stub.put("sample", sample);
        return stub;
    }

    public Map<String, Object> previewEnterpriseThresholdImpact(Map<String, Object> body) {
        String kind = body == null ? "variance" : Objects.toString(body.get("kind"), "variance").trim().toLowerCase();
        try {
            normalizeThresholdKind(kind);
        } catch (IllegalArgumentException e) {
            kind = "variance";
        }
        UUID tenantUuid = body == null ? null : parseOptionalUuid(str(body, "applicationId"));
        if (tenantUuid == null && body != null && body.get("filters") instanceof Map<?, ?> fm) {
            @SuppressWarnings("unchecked")
            Map<String, Object> f = (Map<String, Object>) fm;
            tenantUuid = parseOptionalUuid(str(f, "applicationId"));
        }
        Map<String, Object> out = new HashMap<>();
        out.put("overlaps", thresholdConflictWarnings(kind, tenantUuid));
        out.put("affectedTransactionCount", null);
        out.put("draftVsPublished", Map.of("note", "Publish pipeline not wired for thresholds in this service build."));
        out.put("kind", kind);
        out.put("search", body == null ? null : body.get("search"));
        return out;
    }

    public List<Map<String, Object>> listThresholdAuditEvents(String kindRaw, String id) {
        String kind = normalizeThresholdKind(kindRaw);
        String entityType = entityTypeForThresholdKind(kind);
        return jdbc.queryForList("""
                SELECT CAST(audit_log_id AS text) AS "auditId",
                       action_code AS "action",
                       entity_type AS "entityType",
                       CAST(entity_id AS text) AS "entityId",
                       entity_reference_code AS "entityReferenceCode",
                       details_json AS "details",
                       event_at AS "eventAt"
                FROM reconciliation.audit_log
                WHERE entity_type = ?
                  AND (entity_reference_code = ? OR CAST(entity_id AS text) = ?)
                ORDER BY event_at DESC
                LIMIT 200
                """, entityType, id, id);
    }
}
