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
 * Enterprise reconciliation access control using {@code access.access_role},
 * {@code access.access_application_user}, {@code access.access_user_role_location},
 * and reconciliation scope tables ({@code reco_role_domain_access}, etc.).
 */
@Repository
public class ReconciliationEnterpriseAccessControlRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationEnterpriseAccessControlRepository(
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
                    VALUES (?, ?, ?, 'access_role', ?, ?::jsonb)
                    """, tenantId, actorId, action, entityRef, toJson(clean));
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private static String normalizeView(String view) {
        if (view == null || view.isBlank()) {
            return "roles";
        }
        return switch (view.trim().toLowerCase()) {
            case "roles", "permissions", "domain-access", "stage-access", "financial-period",
                 "location", "approval-matrix", "sod", "user-assignments", "audit" -> view.trim().toLowerCase();
            default -> "roles";
        };
    }

    /** Tenant scope on {@code access.access_role} ({@code application_id}). */
    private static String accessRoleTenantPredicate(String alias) {
        return alias + ".application_id IS NOT DISTINCT FROM ?";
    }

    private static void appendAccessRoleTenantArgs(List<Object> args, UUID tenantUuid) {
        args.add(tenantUuid);
    }

    private static String accessRoleTenantPredicateNoAlias() {
        return "application_id IS NOT DISTINCT FROM ?";
    }

    private static String roleCodeExpr(String alias) {
        return "COALESCE(" + alias + ".role_number, " + alias + ".name)";
    }

    private static String accessAssignmentJoinSql() {
        return """
                JOIN access.access_user_role_location aurl ON aurl.role_id = r.role_id
                JOIN access.access_application_user aau ON aau.application_user_id = aurl.application_user_id
                  AND aau.application_id IS NOT DISTINCT FROM r.application_id
                  AND aau.is_active = true
                """;
    }

    private static String accessAssignmentUserJoinSql() {
        return accessAssignmentJoinSql() + """
                LEFT JOIN access.access_user u ON u.user_id = aau.user_id
                """;
    }

    private static String accessAssignmentActiveSql() {
        return " AND COALESCE(r.is_active, true) = true ";
    }

    private static String accessUserCountSubquery() {
        return """
                (SELECT COUNT(DISTINCT aau_uc.user_id)
                 FROM access.access_user_role_location aurl_uc
                 JOIN access.access_application_user aau_uc ON aau_uc.application_user_id = aurl_uc.application_user_id
                   AND aau_uc.application_id IS NOT DISTINCT FROM r.application_id
                   AND aau_uc.is_active = true
                 WHERE aurl_uc.role_id = r.role_id) AS "userCount"
                """;
    }

    private UUID resolveApplicationUserId(UUID userId, UUID applicationId) {
        UUID appUserId = queryUuidOrNull("""
                SELECT application_user_id FROM access.access_application_user
                WHERE user_id = ? AND application_id IS NOT DISTINCT FROM ? AND is_active = true
                LIMIT 1
                """, userId, applicationId);
        if (appUserId == null) {
            throw new IllegalArgumentException(
                    "User is not registered for this application (access.access_application_user)");
        }
        return appUserId;
    }

    private void verifyRoleForApplication(UUID roleId, UUID applicationId) {
        UUID found = queryUuidOrNull("""
                SELECT role_id FROM access.access_role
                WHERE role_id = ? AND application_id IS NOT DISTINCT FROM ?
                LIMIT 1
                """, roleId, applicationId);
        if (found == null) {
            throw new IllegalArgumentException("Role not found for application");
        }
    }

    private static String actorRef(UUID actor) {
        return actor == null ? "reconciliation-api" : actor.toString();
    }

    private Map<String, Object> wrapList(
            List<Map<String, Object>> items,
            long total,
            int page,
            int pageSize,
            UUID tenantUuid,
            boolean includeKpis,
            List<Map<String, Object>> violations,
            List<Map<String, Object>> riskAlerts,
            String expiringHint
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
            out.put("kpis", accessKpis(tenantUuid));
        }
        if (violations != null && !violations.isEmpty()) {
            out.put("violations", violations);
        }
        if (riskAlerts != null && !riskAlerts.isEmpty()) {
            out.put("riskAlerts", riskAlerts);
        }
        if (expiringHint != null) {
            out.put("expiringAssignmentsHint", expiringHint);
        }
        return out;
    }

    private Map<String, Object> accessKpis(UUID tenantUuid) {
        Map<String, Object> k = new HashMap<>();
        if (tenantUuid == null) {
            k.put("activeRoles", 0L);
            k.put("usersAssigned", 0L);
            k.put("pendingApprovals", 0L);
            k.put("sodViolations", 0L);
            k.put("adminUsers", 0L);
            k.put("expiredAssignments", 0L);
            return k;
        }
        k.put("activeRoles", countOrZero("""
                SELECT COUNT(1) FROM access.access_role r
                WHERE r.is_active = true AND %s
                """.formatted(accessRoleTenantPredicate("r")), tenantUuid));
        k.put("usersAssigned", countOrZero("""
                SELECT COUNT(DISTINCT aau.user_id)
                FROM access.access_role r
                """ + accessAssignmentJoinSql() + """
                WHERE %s
                """.formatted(accessRoleTenantPredicate("r")) + accessAssignmentActiveSql(), tenantUuid));
        k.put("pendingApprovals", 0L);
        k.put("sodViolations", 0L);
        k.put("adminUsers", countOrZero("""
                SELECT COUNT(DISTINCT aau.user_id)
                FROM access.access_role r
                """ + accessAssignmentJoinSql() + """
                WHERE r.name ILIKE '%%ADMIN%%' AND %s
                """.formatted(accessRoleTenantPredicate("r")) + accessAssignmentActiveSql(), tenantUuid));
        k.put("expiredAssignments", 0L);
        return k;
    }

    private String expiringAssignmentsHint(UUID tenantUuid) {
        if (tenantUuid == null) {
            return null;
        }
        // Temporary assignment expiry is managed in access schema (no reco_principal_role).
        return null;
    }

    public Map<String, Object> listEnterpriseAccessControl(
            String tenantId,
            String viewRaw,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String roleId,
            String userId,
            String recoDomain,
            String stage,
            String level,
            String permissionCode,
            String status
    ) {
        String view = normalizeView(viewRaw);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        String expiring = expiringAssignmentsHint(tenantUuid);

        return switch (view) {
            case "permissions" -> listPermissions(tenantUuid, activeOnly, search, p, ps, offset, permissionCode);
            case "domain-access" -> listDomainAccess(tenantUuid, activeOnly, search, p, ps, offset, roleId, recoDomain);
            case "stage-access" -> listStageAccess(tenantUuid, activeOnly, search, p, ps, offset, roleId, stage);
            case "financial-period" -> listFinancialPeriod(tenantUuid, activeOnly, search, p, ps, offset, roleId);
            case "location" -> listLocation(tenantUuid, activeOnly, search, p, ps, offset, roleId, userId, level);
            case "approval-matrix" -> listApprovalMatrix(tenantUuid, activeOnly, search, p, ps, offset);
            case "sod" -> listSod(tenantUuid, activeOnly, search, p, ps, offset);
            case "user-assignments" -> listUserAssignments(tenantUuid, activeOnly, search, p, ps, offset, roleId, userId, status);
            case "audit" -> listAudit(tenantUuid, p, ps, offset, search);
            default -> listRoles(tenantUuid, activeOnly, search, p, ps, offset, roleId, status, expiring);
        };
    }

    private Map<String, Object> listRoles(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String roleId, String status, String expiringHint
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE ").append(accessRoleTenantPredicate("r")).append(" ");
        appendAccessRoleTenantArgs(args, tenantUuid);
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND r.is_active = true ");
        }
        UUID rid = parseOptionalUuid(roleId);
        if (rid != null) {
            where.append(" AND r.role_id = ? ");
            args.add(rid);
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (r.name ILIKE ? ESCAPE '\\' OR ")
                    .append(roleCodeExpr("r")).append(" ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        if (status != null && !status.isBlank()) {
            if ("ACTIVE".equalsIgnoreCase(status)) {
                where.append(" AND r.is_active = true ");
            } else if ("INACTIVE".equalsIgnoreCase(status)) {
                where.append(" AND r.is_active = false ");
            }
        }
        String fromSql = " FROM access.access_role r " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        String selectSql = "SELECT CAST(r.role_id AS text) AS \"roleId\", "
                + "CAST(r.role_id AS text) AS \"id\", "
                + "r.name AS \"roleName\", "
                + roleCodeExpr("r") + " AS \"roleCode\", "
                + "'BUSINESS' AS \"roleType\", "
                + accessUserCountSubquery()
                + ", (SELECT COUNT(1) FROM reconciliation.reco_role_domain_access da WHERE da.role_id = r.role_id) AS \"domainCount\", "
                + "(SELECT COUNT(1) FROM reconciliation.reco_role_stage_access sa WHERE sa.role_id = r.role_id) AS \"stageCount\", "
                + "r.is_active AS \"active\", "
                + "CASE WHEN r.is_active THEN 'ACTIVE' ELSE 'INACTIVE' END AS \"status\" "
                + fromSql + " ORDER BY " + roleCodeExpr("r") + " LIMIT ? OFFSET ?";
        List<Map<String, Object>> items = jdbc.queryForList(selectSql, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, true, null, null, expiringHint);
    }

    private Map<String, Object> listPermissions(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset, String permissionCode
    ) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE a.application_id IS NOT DISTINCT FROM ? ");
        if (Boolean.TRUE.equals(activeOnly)) {
            where.append(" AND COALESCE(a.is_active, true) = true ");
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (a.name ILIKE ? ESCAPE '\\' OR COALESCE(a.description, '') ILIKE ? ESCAPE '\\') ");
            args.add(q);
            args.add(q);
        }
        if (permissionCode != null && !permissionCode.isBlank()) {
            where.append(" AND UPPER(a.name) = UPPER(?) ");
            args.add(permissionCode.trim());
        }
        String fromSql = " FROM access.access a " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        String permSql = "SELECT CAST(a.access_id AS text) AS \"accessId\", "
                + "a.name AS \"permissionCode\", NULL AS \"moduleCode\", a.description AS \"description\", "
                + "false AS \"sensitive\", 'OPERATIONAL' AS \"category\" "
                + fromSql + " ORDER BY a.name LIMIT ? OFFSET ?";
        List<Map<String, Object>> items = jdbc.queryForList(permSql, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
    }

    private Map<String, Object> listDomainAccess(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String roleId, String recoDomain
    ) {
        return listScopedAccess(tenantUuid, search, p, ps, offset, roleId, recoDomain, "domain");
    }

    private Map<String, Object> listStageAccess(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String roleId, String stage
    ) {
        return listScopedAccess(tenantUuid, search, p, ps, offset, roleId, stage, "stage");
    }

    private Map<String, Object> listScopedAccess(
            UUID tenantUuid, String search, int p, int ps, int offset,
            String roleId, String scopeCode, String kind
    ) {
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder(" WHERE ").append(accessRoleTenantPredicate("r")).append(" ");
            appendAccessRoleTenantArgs(args, tenantUuid);
            UUID rid = parseOptionalUuid(roleId);
            if (rid != null) {
                where.append(" AND r.role_id = ? ");
                args.add(rid);
            }
            if (search != null && !search.isBlank()) {
                String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
                where.append(" AND (r.name ILIKE ? ESCAPE '\\') ");
                args.add(q);
            }
            String sql;
            if ("domain".equals(kind)) {
                if (scopeCode != null && !scopeCode.isBlank()) {
                    where.append(" AND UPPER(d.code) = UPPER(?) ");
                    args.add(scopeCode.trim());
                }
                sql = """
                        SELECT r.name AS "roleName", d.code AS "recoDomain",
                               da.can_view AS "canView", da.can_manage AS "canManage",
                               da.can_manage AS "canExecute", false AS "canExport"
                        FROM reconciliation.reco_role_domain_access da
                        JOIN access.access_role r ON r.role_id = da.role_id
                        JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = da.reco_domain_id
                        """ + where;
            } else {
                if (scopeCode != null && !scopeCode.isBlank()) {
                    where.append(" AND UPPER(stg.code) = UPPER(?) ");
                    args.add(scopeCode.trim());
                }
                sql = """
                        SELECT r.name AS "roleName", stg.code AS "stage",
                               sa.can_view AS "canView", sa.can_manage AS "canExecute",
                               false AS "canResolve", false AS "canApprove"
                        FROM reconciliation.reco_role_stage_access sa
                        JOIN access.access_role r ON r.role_id = sa.role_id
                        JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = sa.reconciliation_stage_id
                        """ + where;
            }
            Long total = jdbc.queryForObject("SELECT COUNT(1) FROM (" + sql + ") x", Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList(sql + " ORDER BY r.name LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
        } catch (Exception e) {
            return wrapList(List.of(), 0L, p, ps, tenantUuid, false, null, null, null);
        }
    }

    private Map<String, Object> listFinancialPeriod(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset, String roleId
    ) {
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder(" WHERE ").append(accessRoleTenantPredicate("r")).append(" ");
            appendAccessRoleTenantArgs(args, tenantUuid);
            UUID rid = parseOptionalUuid(roleId);
            if (rid != null) {
                where.append(" AND r.role_id = ? ");
                args.add(rid);
            }
            String fromSql = """
                    FROM reconciliation.reco_role_financial_period_access fpa
                    JOIN access.access_role r ON r.role_id = fpa.role_id
                    """ + where;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT r.name AS "roleName",
                           fpa.access_mode AS "accessMode",
                           CAST(fpa.financial_period_id AS text) AS "periodLabel",
                           fpa.can_run_reconciliation AS "canRunRecon",
                           fpa.can_close_period AS "canClose",
                           fpa.can_reopen_period AS "canReopen",
                           fpa.can_view AS "canExport"
                    """ + fromSql + " ORDER BY r.name LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
        } catch (Exception e) {
            return wrapList(List.of(), 0L, p, ps, tenantUuid, false, null, null, null);
        }
    }

    private Map<String, Object> listLocation(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String roleId, String userId, String level
    ) {
        List<Map<String, Object>> items = new ArrayList<>();
        try {
            List<Object> args = new ArrayList<>();
            StringBuilder where = new StringBuilder(" WHERE ").append(accessRoleTenantPredicate("r")).append(" ");
            appendAccessRoleTenantArgs(args, tenantUuid);
            UUID rid = parseOptionalUuid(roleId);
            if (rid != null) {
                where.append(" AND r.role_id = ? ");
                args.add(rid);
            }
            UUID uid = parseOptionalUuid(userId);
            if (uid != null) {
                where.append(" AND aau.user_id = ? ");
                args.add(uid);
            }
            UUID lvl = parseOptionalUuid(level);
            if (lvl != null) {
                where.append(" AND aurl.location_id = ? ");
                args.add(lvl);
            }
            String fromSql = """
                    FROM access.access_role r
                    """ + accessAssignmentUserJoinSql() + """
                    LEFT JOIN locations.location loc ON loc.location_id = aurl.location_id
                    """ + where + accessAssignmentActiveSql();
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            items = jdbc.queryForList("""
                    SELECT COALESCE(NULLIF(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), ''), CAST(aau.user_id AS text)) AS "userName",
                           r.name AS "roleName",
                           COALESCE(loc.name, CAST(aurl.location_id AS text), 'Global') AS "locationName",
                           CASE WHEN aurl.location_id IS NULL THEN 'GLOBAL' ELSE 'DIRECT' END AS "scopeType",
                           false AS "inherited",
                           true AS "active"
                    """ + fromSql + " ORDER BY r.name LIMIT ? OFFSET ?", pageArgs.toArray());
            Map<String, Object> out = wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
            out.put("locationTree", List.of());
            return out;
        } catch (Exception e) {
            Map<String, Object> out = wrapList(items, 0L, p, ps, tenantUuid, false, null, null, null);
            out.put("locationTree", List.of());
            return out;
        }
    }

    private Map<String, Object> listApprovalMatrix(UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset) {
        try {
            List<Object> args = new ArrayList<>();
            args.add(tenantUuid);
            StringBuilder where = new StringBuilder(" WHERE m.deleted_on IS NULL AND m.application_id IS NOT DISTINCT FROM ? ");
            if (Boolean.TRUE.equals(activeOnly)) {
                where.append(" AND m.is_active = true ");
            }
            String fromSql = " FROM reconciliation.reco_approval_matrix m " + where;
            Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            List<Map<String, Object>> items = jdbc.queryForList("""
                    SELECT m.approval_action AS "approvalAction",
                           m.requestor_role_code AS "requestorRole",
                           m.approver_role_code AS "approverRole",
                           m.dual_approval AS "dualApproval",
                           m.escalation_role_code AS "escalationRole"
                    """ + fromSql + " ORDER BY m.approval_action LIMIT ? OFFSET ?", pageArgs.toArray());
            return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
        } catch (Exception e) {
            return wrapList(List.of(), 0L, p, ps, tenantUuid, false, null, null, null);
        }
    }

    private Map<String, Object> listSod(UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset) {
        List<Map<String, Object>> violations = List.of();
        List<Map<String, Object>> riskAlerts = List.of();
        List<Map<String, Object>> items = List.of();
        long total = 0;
        try {
            List<Object> args = new ArrayList<>();
            args.add(tenantUuid);
            StringBuilder where = new StringBuilder(" WHERE s.deleted_on IS NULL AND s.application_id IS NOT DISTINCT FROM ? ");
            if (Boolean.TRUE.equals(activeOnly)) {
                where.append(" AND s.is_active = true ");
            }
            String fromSql = " FROM reconciliation.reco_sod_rule s " + where;
            total = countOrZero("SELECT COUNT(1) " + fromSql, args.toArray());
            List<Object> pageArgs = new ArrayList<>(args);
            pageArgs.add(ps);
            pageArgs.add(offset);
            items = jdbc.queryForList("""
                    SELECT CAST(s.reco_sod_rule_id AS text) AS "sodRuleId",
                           s.rule_name AS "ruleName",
                           s.conflicting_roles_display AS "conflictingRoles",
                           s.severity_code AS "severity",
                           s.is_active AS "active"
                    """ + fromSql + " ORDER BY s.rule_name LIMIT ? OFFSET ?", pageArgs.toArray());
        } catch (Exception ignored) {
            // tables optional
        }
        return wrapList(items, total, p, ps, tenantUuid, false, violations, riskAlerts, null);
    }

    private Map<String, Object> listUserAssignments(
            UUID tenantUuid, Boolean activeOnly, String search, int p, int ps, int offset,
            String roleId, String userId, String status
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE ").append(accessRoleTenantPredicate("r")).append(" ");
        appendAccessRoleTenantArgs(args, tenantUuid);
        UUID rid = parseOptionalUuid(roleId);
        if (rid != null) {
            where.append(" AND r.role_id = ? ");
            args.add(rid);
        }
        UUID uid = parseOptionalUuid(userId);
        if (uid != null) {
            where.append(" AND aau.user_id = ? ");
            args.add(uid);
        }
        if (status != null && !status.isBlank()) {
            switch (status.toUpperCase()) {
                case "ACTIVE" -> where.append(accessAssignmentActiveSql());
                case "EXPIRED" -> where.append(" AND aau.is_active = false ");
                case "PENDING" -> where.append(" AND false ");
                default -> { }
            }
        } else {
            where.append(accessAssignmentActiveSql());
        }
        String fromSql = """
                FROM access.access_role r
                """ + accessAssignmentUserJoinSql() + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT CAST(aurl.application_user_id AS text) || ':' || CAST(aurl.role_id AS text)
                       || ':' || COALESCE(CAST(aurl.location_id AS text), '') AS "assignmentId",
                       COALESCE(NULLIF(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), ''), CAST(aau.user_id AS text)) AS "userName",
                       r.name AS "roleName",
                       '—' AS "domainSummary",
                       '—' AS "stageSummary",
                       COALESCE((SELECT loc.name FROM locations.location loc WHERE loc.location_id = aurl.location_id LIMIT 1), 'Global') AS "locationSummary",
                       NULL::date AS "validFrom",
                       NULL::date AS "validTo",
                       CASE WHEN aau.is_active AND COALESCE(r.is_active, true) THEN 'ACTIVE' ELSE 'INACTIVE' END AS "status",
                       false AS "temporary"
                """ + fromSql + " ORDER BY r.name, aau.user_id LIMIT ? OFFSET ?", pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, expiringAssignmentsHint(tenantUuid));
    }

    private Map<String, Object> listAudit(UUID tenantUuid, int p, int ps, int offset, String search) {
        List<Object> args = new ArrayList<>();
        args.add(tenantUuid);
        StringBuilder where = new StringBuilder(" WHERE a.entity_type = 'access_role' AND a.application_id IS NOT DISTINCT FROM ? ");
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append(" AND (a.action_code ILIKE ? ESCAPE '\\') ");
            args.add(q);
        }
        String fromSql = " FROM reconciliation.audit_log a " + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT a.event_at AS "occurredAt",
                       CAST(a.actor_user_id AS text) AS "actorId",
                       a.action_code AS "action",
                       a.entity_reference_code AS "target",
                       COALESCE(a.details_json->>'beforeSummary', '') AS "beforeSummary",
                       COALESCE(a.details_json->>'afterSummary', a.action_code) AS "afterSummary"
                FROM reconciliation.audit_log a
                """ + where + """
                ORDER BY a.event_at DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        return wrapList(items, total == null ? 0L : total, p, ps, tenantUuid, false, null, null, null);
    }

    public Map<String, Object> getRoleDetail(String tenantId, String roleId) {
        UUID rid = parseOptionalUuid(roleId);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (rid == null) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT CAST(r.role_id AS text) AS "roleId",
                       r.name AS "roleName",
                       COALESCE(r.role_number, r.name) AS "roleCode",
                       r.description AS "description",
                       'BUSINESS' AS "roleType",
                       r.is_active AS "active",
                       CAST(r.role_id AS text) AS "accessRoleId"
                FROM access.access_role r
                WHERE r.role_id = ? AND %s
                """.formatted(accessRoleTenantPredicate("r")), rid, tenantUuid);
        if (rows.isEmpty()) {
            return Map.of("status", "NOT_FOUND");
        }
        Map<String, Object> db = rows.get(0);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("roleId", db.get("roleId"));
        row.put("roleName", db.get("roleName"));
        row.put("roleCode", db.get("roleCode"));
        row.put("description", db.get("description"));
        row.put("roleType", db.get("roleType"));
        row.put("active", db.get("active"));
        row.put("applicationCodes", List.of("RECONCILIATION"));
        row.put("domainCodes", loadDomainCodes(rid));
        row.put("stageCodes", loadStageCodes(rid));
        row.put("locationLevelIds", loadLocationIds(rid));
        row.put("financialPeriodAccessMode", loadFinancialPeriodMode(rid, db));
        db.put("applicationId", tenantUuid == null ? null : tenantUuid.toString());
        row.put("permissionMatrix", loadPermissionMatrix(rid, db));
        return Map.of("status", "OK", "row", row);
    }

    private static String displayNameFromRow(Map<String, Object> m) {
        String name = str(m, "roleName");
        if (name == null || name.isBlank()) {
            name = str(m, "displayName");
        }
        return Objects.requireNonNull(name, "roleName is required");
    }

    private List<String> loadDomainCodes(UUID roleId) {
        return jdbc.query("""
                SELECT d.code FROM reconciliation.reco_role_domain_access da
                JOIN reconciliation.lu_reco_domain d ON d.reco_domain_id = da.reco_domain_id
                WHERE da.role_id = ?
                """, (rs, rn) -> rs.getString(1), roleId);
    }

    private List<String> loadStageCodes(UUID roleId) {
        return jdbc.query("""
                SELECT stg.code FROM reconciliation.reco_role_stage_access sa
                JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = sa.reconciliation_stage_id
                WHERE sa.role_id = ?
                """, (rs, rn) -> rs.getString(1), roleId);
    }

    private List<String> loadLocationIds(UUID roleId) {
        try {
            return jdbc.query("""
                    SELECT DISTINCT CAST(aurl.location_id AS text)
                    FROM access.access_user_role_location aurl
                    WHERE aurl.role_id = ? AND aurl.location_id IS NOT NULL
                    """, (rs, rn) -> rs.getString(1), roleId);
        } catch (Exception e) {
            return List.of();
        }
    }

    private String loadFinancialPeriodMode(UUID roleId, Map<String, Object> dbRow) {
        try {
            List<String> modes = jdbc.query("""
                    SELECT access_mode FROM reconciliation.reco_role_financial_period_access
                    WHERE role_id = ? LIMIT 1
                    """, (rs, rn) -> rs.getString(1), roleId);
            if (!modes.isEmpty()) {
                return modes.get(0);
            }
        } catch (Exception ignored) {
            // optional
        }
        return "ALL_PERIODS";
    }

    private List<Map<String, Object>> loadPermissionMatrix(UUID roleId, Map<String, Object> roleRow) {
        UUID applicationId = parseOptionalUuid(String.valueOf(roleRow.getOrDefault("applicationId", "")));
        if (applicationId == null) {
            applicationId = queryUuidOrNull(
                    "SELECT application_id FROM access.access_role WHERE role_id = ? LIMIT 1", roleId);
        }
        try {
            return jdbc.queryForList("""
                    SELECT a.name AS "permissionCode",
                           COALESCE(ara.is_active, false) AS "canView",
                           COALESCE(ara.is_active, false) AS "canExecute",
                           false AS "canApprove",
                           false AS "canResolve",
                           false AS "canPublish",
                           COALESCE(ara.is_active, false) AS "canExport"
                    FROM access.access a
                    LEFT JOIN access.access_role_access ara
                      ON ara.access_id = a.access_id AND ara.role_id = ?
                    WHERE a.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(a.is_active, true) = true
                    ORDER BY a.name
                    """, roleId, applicationId);
        } catch (Exception e) {
            return List.of();
        }
    }

    public Map<String, Object> upsertRole(String tenantId, Map<String, Object> m, String idOrNull, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID parsedId = parseOptionalUuid(idOrNull);
        boolean isUpdate = parsedId != null;
        UUID id = isUpdate ? parsedId : UUID.randomUUID();

        String name = displayNameFromRow(m);
        boolean active = bool(m, "active", true);
        String description = str(m, "description");

        if (isUpdate) {
            int u = jdbc.update("""
                    UPDATE access.access_role SET
                        name = ?, description = ?, is_active = ?, modified_on = now(), modified_by = ?
                    WHERE role_id = ? AND %s
                    """.formatted(accessRoleTenantPredicateNoAlias()),
                    name, description, active, actor, id, tenantUuid);
            if (u == 0) {
                return Map.of("updated", false, "roleId", idOrNull);
            }
        } else {
            jdbc.update("""
                    INSERT INTO access.access_role (role_id, application_id, name, description, is_active, created_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, id, tenantUuid, name, description, active, actor);
        }

        replaceRoleChildren(id, tenantUuid, m, actor);
        writeAudit(tenantUuid, actor, isUpdate ? "RECO_ROLE_UPDATED" : "RECO_ROLE_CREATED", id.toString(),
                Map.of("roleName", name));
        return getRoleDetail(tenantId, id.toString());
    }

    @SuppressWarnings("unchecked")
    private void replaceRoleChildren(UUID roleId, UUID applicationId, Map<String, Object> m, UUID actor) {
        jdbc.update("DELETE FROM reconciliation.reco_role_domain_access WHERE role_id = ?", roleId);
        jdbc.update("DELETE FROM reconciliation.reco_role_stage_access WHERE role_id = ?", roleId);
        jdbc.update("DELETE FROM reconciliation.reco_role_financial_period_access WHERE role_id = ?", roleId);
        try {
            jdbc.update("UPDATE access.access_role_access SET is_active = false, modified_on = now() WHERE role_id = ?", roleId);
        } catch (Exception ignored) {
            // optional
        }

        if (m.get("domainCodes") instanceof List<?> domains) {
            for (Object d : domains) {
                UUID domainId = domainIdByCode(String.valueOf(d));
                if (domainId != null) {
                    jdbc.update("""
                            INSERT INTO reconciliation.reco_role_domain_access (role_id, reco_domain_id, can_view, can_manage, created_by)
                            VALUES (?, ?, true, false, ?)
                            ON CONFLICT (role_id, reco_domain_id) DO NOTHING
                            """, roleId, domainId, actor);
                }
            }
        }
        if (m.get("stageCodes") instanceof List<?> stages) {
            for (Object s : stages) {
                UUID stageId = stageIdByCode(String.valueOf(s));
                if (stageId != null) {
                    jdbc.update("""
                            INSERT INTO reconciliation.reco_role_stage_access (role_id, reconciliation_stage_id, can_view, can_manage, created_by)
                            VALUES (?, ?, true, false, ?)
                            ON CONFLICT (role_id, reconciliation_stage_id) DO NOTHING
                            """, roleId, stageId, actor);
                }
            }
        }
        String fpMode = str(m, "financialPeriodAccessMode");
        if (fpMode != null) {
            boolean canClose = "CLOSED_READ_ONLY".equals(fpMode) || "SPECIFIC_PERIODS".equals(fpMode);
            jdbc.update("""
                    INSERT INTO reconciliation.reco_role_financial_period_access (
                        role_id, access_mode, can_view, can_run_reconciliation, can_close_period, can_reopen_period, created_by
                    ) VALUES (?, ?, true, true, ?, ?, ?)
                    """, roleId, fpMode, canClose, "ALL_PERIODS".equals(fpMode), actor);
        }
        if (m.get("permissionMatrix") instanceof List<?> matrix && applicationId != null) {
            for (Object o : matrix) {
                if (!(o instanceof Map<?, ?> pm)) {
                    continue;
                }
                Map<String, Object> row = (Map<String, Object>) pm;
                String permName = str(row, "permissionCode");
                UUID accessId = queryUuidOrNull(
                        "SELECT access_id FROM access.access WHERE application_id IS NOT DISTINCT FROM ? AND name = ? LIMIT 1",
                        applicationId, permName);
                if (accessId == null) {
                    continue;
                }
                boolean granted = bool(row, "canView", false) || bool(row, "canExecute", false);
                jdbc.update("""
                        INSERT INTO access.access_role_access (role_id, access_id, is_active, created_by)
                        VALUES (?, ?, ?, ?)
                        """, roleId, accessId, granted, actor);
            }
        }
    }

    private UUID domainIdByCode(String code) {
        return queryUuidOrNull("SELECT reco_domain_id FROM reconciliation.lu_reco_domain WHERE code = ? LIMIT 1", code.trim());
    }

    private UUID stageIdByCode(String code) {
        return queryUuidOrNull(
                "SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = ? LIMIT 1",
                code.trim());
    }

    public Map<String, Object> deactivateRole(String tenantId, String id, String actorUserId) {
        UUID rid = parseOptionalUuid(id);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        int n = jdbc.update("""
                UPDATE access.access_role SET is_active = false, modified_on = now()
                WHERE role_id = ? AND %s
                """.formatted(accessRoleTenantPredicateNoAlias()),
                rid, tenantUuid);
        writeAudit(tenantUuid, parseOptionalUuid(actorUserId), "RECO_ROLE_DEACTIVATED", id, Map.of());
        return Map.of("updated", n > 0, "roleId", id, "active", false);
    }

    public Map<String, Object> cloneRole(String tenantId, String id, String actorUserId) {
        Map<String, Object> detail = getRoleDetail(tenantId, id);
        if (!"OK".equals(detail.get("status"))) {
            return Map.of("updated", false);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) detail.get("row");
        row.put("roleName", row.get("roleName") + " (Copy)");
        row.put("roleCode", row.get("roleCode") + "-CL-" + UUID.randomUUID().toString().substring(0, 8));
        row.put("active", false);
        return upsertRole(tenantId, row, null, actorUserId);
    }

    public Map<String, Object> assignUser(String tenantId, Map<String, Object> row, String actorUserId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID actor = parseOptionalUuid(actorUserId);
        UUID userId = parseOptionalUuid(str(row, "userId"));
        UUID roleId = parseOptionalUuid(str(row, "roleId"));
        if (userId == null || roleId == null) {
            throw new IllegalArgumentException("userId and roleId are required");
        }
        if (tenantUuid == null) {
            throw new IllegalArgumentException("application id (X-Tenant-Id) is required");
        }
        verifyRoleForApplication(roleId, tenantUuid);
        UUID appUserId = resolveApplicationUserId(userId, tenantUuid);
        UUID locationId = parseOptionalUuid(str(row, "locationId"));
        String createdBy = actorRef(actor);
        int inserted;
        if (locationId != null) {
            inserted = jdbc.update("""
                    INSERT INTO access.access_user_role_location (application_user_id, role_id, location_id, created_by)
                    SELECT ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM access.access_user_role_location x
                        WHERE x.application_user_id = ? AND x.role_id = ? AND x.location_id IS NOT DISTINCT FROM ?
                    )
                    """, appUserId, roleId, locationId, createdBy, appUserId, roleId, locationId);
        } else {
            inserted = jdbc.update("""
                    INSERT INTO access.access_user_role_location (application_user_id, role_id, created_by)
                    SELECT ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM access.access_user_role_location x
                        WHERE x.application_user_id = ? AND x.role_id = ? AND x.location_id IS NULL
                    )
                    """, appUserId, roleId, createdBy, appUserId, roleId);
        }
        String assignmentId = appUserId + ":" + roleId + ":" + (locationId == null ? "" : locationId);
        writeAudit(tenantUuid, actor, "RECO_USER_ROLE_ASSIGNED", roleId.toString(),
                Map.of("userId", userId.toString(), "roleId", roleId.toString(), "assignmentId", assignmentId));
        return Map.of("assignmentId", assignmentId, "updated", inserted > 0);
    }

    public Map<String, Object> previewEffectiveAccess(String tenantId, Map<String, Object> body) {
        UUID userId = parseOptionalUuid(str(body, "userId"));
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<String> permissions = new ArrayList<>();
        List<String> inherited = new ArrayList<>();
        List<String> restricted = new ArrayList<>();
        List<String> approval = new ArrayList<>();
        if (userId != null && tenantUuid != null) {
            List<Map<String, Object>> grants = jdbc.queryForList("""
                    SELECT DISTINCT a.name AS "code"
                    FROM access.access_application_user aau
                    JOIN access.access_user_role_location aurl ON aurl.application_user_id = aau.application_user_id
                    JOIN access.access_role r ON r.role_id = aurl.role_id
                      AND r.application_id IS NOT DISTINCT FROM aau.application_id
                    JOIN access.access_role_access ara ON ara.role_id = r.role_id AND COALESCE(ara.is_active, false) = true
                    JOIN access.access a ON a.access_id = ara.access_id
                    WHERE aau.user_id = ? AND aau.application_id IS NOT DISTINCT FROM ?
                      AND aau.is_active = true AND COALESCE(r.is_active, true) = true
                    ORDER BY a.name
                    """, userId, tenantUuid);
            for (Map<String, Object> g : grants) {
                permissions.add(String.valueOf(g.get("code")));
            }
        }
        if (permissions.isEmpty()) {
            permissions = List.of("RECO_RUN_VIEW");
        }
        if (str(body, "level") != null) {
            inherited.add("Inherited scope for level " + body.get("level"));
        }
        if (!permissions.contains("RECO_CONFIG_PUBLISH")) {
            restricted.add("RECO_CONFIG_PUBLISH");
        }
        if (approval.isEmpty()) {
            approval = List.of("EXCEPTION_OVERRIDE");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("effectivePermissions", permissions);
        out.put("inheritedAccess", inherited);
        out.put("restrictedOperations", restricted);
        out.put("approvalRights", approval);
        return out;
    }

    public Map<String, Object> previewImpact(String tenantId, Map<String, Object> body) {
        Map<String, Object> row = body != null && body.get("row") instanceof Map<?, ?> rm
                ? castMap(rm) : Map.of();
        String roleId = str(row, "roleId");
        long affectedUsers = 0;
        if (roleId != null) {
            affectedUsers = countOrZero("""
                    SELECT COUNT(DISTINCT aau.user_id)
                    FROM access.access_role r
                    """ + accessAssignmentJoinSql() + """
                    WHERE r.role_id = ?
                    """ + accessAssignmentActiveSql(), parseOptionalUuid(roleId));
        }
        boolean hasGl = false;
        boolean hasApprove = false;
        if (row.get("permissionMatrix") instanceof List<?> matrix) {
            for (Object o : matrix) {
                if (o instanceof Map<?, ?> pm) {
                    String code = str(castMap(pm), "permissionCode");
                    if ("RECO_GL_POST".equals(code) && bool(castMap(pm), "canExecute", false)) {
                        hasGl = true;
                    }
                    if (bool(castMap(pm), "canApprove", false) || bool(castMap(pm), "canPublish", false)) {
                        hasApprove = true;
                    }
                }
            }
        }
        String risk = hasGl && hasApprove ? "CRITICAL" : "LOW";
        String riskMsg = hasGl && hasApprove
                ? "User has both GL posting and reconciliation approval rights."
                : "No critical SoD risk detected for this change.";
        return Map.of(
                "affectedUsers", affectedUsers,
                "affectedDomains", row.get("domainCodes") instanceof List<?> d ? d.size() : 0,
                "affectedSchedules", 0,
                "riskLevel", risk,
                "riskMessage", riskMsg
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    public Map<String, Object> exportAccessMatrix(String tenantId) {
        UUID tenantUuid = parseOptionalUuid(tenantId);
        List<Map<String, Object>> matrix = jdbc.queryForList("""
                SELECT COALESCE(r.role_number, r.name) AS "roleCode", a.name AS "permissionCode",
                       COALESCE(ara.is_active, false) AS "granted"
                FROM access.access_role r
                CROSS JOIN access.access a
                LEFT JOIN access.access_role_access ara
                  ON ara.role_id = r.role_id AND ara.access_id = a.access_id
                WHERE %s AND a.application_id IS NOT DISTINCT FROM r.application_id
                  AND COALESCE(a.is_active, true) = true
                ORDER BY COALESCE(r.role_number, r.name), a.name
                """.formatted(accessRoleTenantPredicate("r")), tenantUuid);
        return Map.of("matrix", matrix, "format", "json");
    }

    public Map<String, Object> acknowledgeSodViolation(String violationId, String actorUserId) {
        return Map.of(
                "updated", false,
                "violationId", violationId == null ? "" : violationId,
                "message", "SoD violation instances are not persisted; configure rules via reco_sod_rule only."
        );
    }

    public List<Map<String, Object>> listRecoPermissionsForLookups(UUID applicationId) {
        if (applicationId == null) {
            return List.of();
        }
        try {
            return jdbc.queryForList("""
                    SELECT a.name AS "code",
                           a.name AS "displayName",
                           NULL AS "module",
                           false AS "sensitive",
                           'OPERATIONAL' AS "category",
                           COALESCE(a.is_active, true) AS "active"
                    FROM access.access a
                    WHERE a.application_id IS NOT DISTINCT FROM ?
                      AND COALESCE(a.is_active, true) = true
                    ORDER BY a.name
                    """, applicationId);
        } catch (Exception e) {
            return List.of();
        }
    }
}
