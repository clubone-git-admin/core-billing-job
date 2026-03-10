package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM Opportunities: list, get, create, update, delete, bulk operations, contact methods.
 * DDL: crm.opportunity, crm.lu_opportunity_stage, crm.opportunity_stage_history, crm.client_contact_method, crm.lu_contact_method.
 */
@Repository
public class CrmOpportunityRepository {

    private final JdbcTemplate jdbc;

    public CrmOpportunityRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String LIST_SELECT = """
        SELECT o.opportunity_id, o.opportunity_code, o.contact_id, c.contact_code, o.client_id,
               COALESCE(o.full_name, '') AS name, NULL::text AS client_status,
               loc.name AS club, o.opportunity_stage_id, os.code AS stage_code, os.display_name AS stage_display_name,
               o.lead_type_id, lt.display_name AS type_display_name, o.created_on, o.owner_user_id,
               TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_display_name,
               0::double precision AS amount, NULL::date AS expected_close_date, c.full_name AS contact_name
        FROM crm.opportunity o
        LEFT JOIN crm.contact c ON c.contact_id = o.contact_id AND c.org_client_id = o.org_client_id
        LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id
        LEFT JOIN crm.lu_lead_type lt ON lt.lead_type_id = o.lead_type_id AND lt.org_client_id = o.org_client_id
        LEFT JOIN "access".access_user u ON u.user_id = o.owner_user_id
        LEFT JOIN locations."location" loc ON loc.location_id = o.home_location_id
        WHERE o.org_client_id = ?
        """;

    public List<Map<String, Object>> listOpportunities(UUID orgClientId, String search, UUID opportunityStageId,
                                                       String stageCode, UUID ownerUserId, UUID leadTypeId,
                                                       int limit, int offset) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (o.opportunity_code ILIKE ? OR o.full_name ILIKE ? OR o.email ILIKE ? OR c.full_name ILIKE ? OR c.contact_code ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (opportunityStageId != null) { sql.append(" AND o.opportunity_stage_id = ? "); params.add(opportunityStageId); }
        if (stageCode != null && !stageCode.isBlank()) {
            sql.append(" AND os.code = ? ");
            params.add(stageCode.trim());
        }
        if (ownerUserId != null) { sql.append(" AND o.owner_user_id = ? "); params.add(ownerUserId); }
        if (leadTypeId != null) { sql.append(" AND o.lead_type_id = ? "); params.add(leadTypeId); }
        sql.append(" ORDER BY o.modified_on DESC NULLS LAST, o.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countOpportunities(UUID orgClientId, String search, UUID opportunityStageId,
                                   String stageCode, UUID ownerUserId, UUID leadTypeId) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*) FROM crm.opportunity o
            LEFT JOIN crm.contact c ON c.contact_id = o.contact_id AND c.org_client_id = o.org_client_id
            LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id
            WHERE o.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (o.opportunity_code ILIKE ? OR o.full_name ILIKE ? OR o.email ILIKE ? OR c.full_name ILIKE ? OR c.contact_code ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (opportunityStageId != null) { sql.append(" AND o.opportunity_stage_id = ? "); params.add(opportunityStageId); }
        if (stageCode != null && !stageCode.isBlank()) { sql.append(" AND os.code = ? "); params.add(stageCode.trim()); }
        if (ownerUserId != null) { sql.append(" AND o.owner_user_id = ? "); params.add(ownerUserId); }
        if (leadTypeId != null) { sql.append(" AND o.lead_type_id = ? "); params.add(leadTypeId); }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String DETAIL_SELECT = """
        SELECT o.opportunity_id, o.opportunity_code, o.full_name, o.contact_id, c.full_name AS contact_display_name,
               o.client_id, NULL::text AS client_status, o.home_location_id, loc.name AS home_location_name,
               o.opportunity_stage_id, os.display_name AS stage_display_name, o.probability,
               o.owner_user_id, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_display_name,
               o.salutation_id, o.first_name, o.last_name, o.email, o.phone,
               o.lead_type_id, lt.display_name AS lead_type_display_name, o.gender_id, o.referred_by_contact_id,
               o.created_on, o.modified_on, o.created_by, o.modified_by
        FROM crm.opportunity o
        LEFT JOIN crm.contact c ON c.contact_id = o.contact_id AND c.org_client_id = o.org_client_id
        LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id
        LEFT JOIN crm.lu_lead_type lt ON lt.lead_type_id = o.lead_type_id AND lt.org_client_id = o.org_client_id
        LEFT JOIN "access".access_user u ON u.user_id = o.owner_user_id
        LEFT JOIN locations."location" loc ON loc.location_id = o.home_location_id
        WHERE o.org_client_id = ? AND o.opportunity_id = ?
        """;

    public Map<String, Object> findById(UUID orgClientId, UUID opportunityId) {
        List<Map<String, Object>> list = jdbc.queryForList(DETAIL_SELECT, orgClientId, opportunityId);
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean exists(UUID orgClientId, UUID opportunityId) {
        if (opportunityId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.opportunity WHERE org_client_id = ? AND opportunity_id = ?)",
                Boolean.class, orgClientId, opportunityId);
        return Boolean.TRUE.equals(b);
    }

    public boolean contactExists(UUID orgClientId, UUID contactId) {
        if (contactId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.contact WHERE org_client_id = ? AND contact_id = ?)",
                Boolean.class, orgClientId, contactId);
        return Boolean.TRUE.equals(b);
    }

    public boolean leadTypeExists(UUID orgClientId, UUID leadTypeId) {
        if (leadTypeId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_lead_type WHERE org_client_id = ? AND lead_type_id = ? AND is_active = true)",
                Boolean.class, orgClientId, leadTypeId);
        return Boolean.TRUE.equals(b);
    }

    public boolean contactMethodLookupExists(UUID orgClientId, UUID contactMethodId) {
        if (contactMethodId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_contact_method WHERE org_client_id = ? AND contact_method_id = ?)",
                Boolean.class, orgClientId, contactMethodId);
        return Boolean.TRUE.equals(b);
    }

    public boolean ownerUserExists(UUID ownerUserId) {
        if (ownerUserId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM access.access_user WHERE user_id = ?)",
                Boolean.class, ownerUserId);
        return Boolean.TRUE.equals(b);
    }

    public UUID insert(UUID orgClientId, UUID contactId, String fullName, UUID opportunityStageId, UUID ownerUserId,
                       String firstName, String lastName, String email, String phone, UUID homeLocationId,
                       UUID leadTypeId, Integer probability, UUID createdBy) {
        UUID opportunityId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.opportunity (opportunity_id, org_client_id, contact_id, full_name, opportunity_stage_id, owner_user_id,
                first_name, last_name, email, phone, home_location_id, lead_type_id, probability, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, opportunityId, orgClientId, contactId, fullName != null ? fullName : "", opportunityStageId, ownerUserId,
                firstName, lastName, email, phone, homeLocationId, leadTypeId, probability != null ? probability : 20, createdBy);
        return opportunityId;
    }

    public int update(UUID orgClientId, UUID opportunityId, Map<String, Object> updates) {
        if (updates == null || updates.isEmpty()) return 0;
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (updates.containsKey("full_name")) { setClauses.add("full_name = ?"); params.add(updates.get("full_name")); }
        if (updates.containsKey("contact_id")) { setClauses.add("contact_id = ?"); params.add(updates.get("contact_id")); }
        if (updates.containsKey("opportunity_stage_id")) { setClauses.add("opportunity_stage_id = ?"); params.add(updates.get("opportunity_stage_id")); }
        if (updates.containsKey("owner_user_id")) { setClauses.add("owner_user_id = ?"); params.add(updates.get("owner_user_id")); }
        if (updates.containsKey("first_name")) { setClauses.add("first_name = ?"); params.add(updates.get("first_name")); }
        if (updates.containsKey("last_name")) { setClauses.add("last_name = ?"); params.add(updates.get("last_name")); }
        if (updates.containsKey("email")) { setClauses.add("email = ?"); params.add(updates.get("email")); }
        if (updates.containsKey("phone")) { setClauses.add("phone = ?"); params.add(updates.get("phone")); }
        if (updates.containsKey("home_location_id")) { setClauses.add("home_location_id = ?"); params.add(updates.get("home_location_id")); }
        if (updates.containsKey("lead_type_id")) { setClauses.add("lead_type_id = ?"); params.add(updates.get("lead_type_id")); }
        if (updates.containsKey("probability")) { setClauses.add("probability = ?"); params.add(updates.get("probability")); }
        if (setClauses.isEmpty()) return 0;
        params.add(orgClientId);
        params.add(opportunityId);
        return jdbc.update("UPDATE crm.opportunity SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND opportunity_id = ?", params.toArray());
    }

    public int delete(UUID orgClientId, UUID opportunityId) {
        return jdbc.update("DELETE FROM crm.opportunity WHERE org_client_id = ? AND opportunity_id = ?", orgClientId, opportunityId);
    }

    /** Bulk update stage; optionally close current history row and insert new one. */
    public int bulkUpdateStage(UUID orgClientId, List<UUID> opportunityIds, UUID newStageId, UUID changedBy, String changeReason, String notes) {
        if (opportunityIds == null || opportunityIds.isEmpty()) return 0;
        int n = 0;
        for (UUID oppId : opportunityIds) {
            Map<String, Object> row = findById(orgClientId, oppId);
            if (row == null) continue;
            Object prevStageObj = row.get("opportunity_stage_id");
            int up = jdbc.update("UPDATE crm.opportunity SET opportunity_stage_id = ? WHERE org_client_id = ? AND opportunity_id = ?",
                    newStageId, orgClientId, oppId);
            if (up > 0) {
                n += up;
                jdbc.update("""
                    UPDATE crm.opportunity_stage_history SET exited_on = CURRENT_TIMESTAMP
                    WHERE org_client_id = ? AND opportunity_id = ? AND exited_on IS NULL
                    """, orgClientId, oppId);
                jdbc.update("""
                    INSERT INTO crm.opportunity_stage_history (org_client_id, opportunity_id, opportunity_stage_id, previous_opportunity_stage_id, entered_on, changed_by_user_id, change_reason, notes, created_by)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
                    """, orgClientId, oppId, newStageId, prevStageObj, changedBy, changeReason, notes, changedBy);
            }
        }
        return n;
    }

    public int bulkChangeOwner(UUID orgClientId, List<UUID> opportunityIds, UUID ownerUserId) {
        if (opportunityIds == null || opportunityIds.isEmpty()) return 0;
        String placeholders = String.join(",", opportunityIds.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(ownerUserId);
        params.add(orgClientId);
        params.addAll(opportunityIds);
        return jdbc.update("UPDATE crm.opportunity SET owner_user_id = ? WHERE org_client_id = ? AND opportunity_id IN (" + placeholders + ")",
                params.toArray());
    }

    // ---------- Contact methods ----------

    public List<Map<String, Object>> listContactMethods(UUID orgClientId, UUID opportunityId) {
        return jdbc.queryForList("""
            SELECT ccm.client_contact_method_id, ccm.opportunity_id, ccm.contact_method_id, cm.contact_method_name, ccm.is_primary, ccm.created_on
            FROM crm.client_contact_method ccm
            INNER JOIN crm.opportunity o ON o.opportunity_id = ccm.opportunity_id AND o.org_client_id = ?
            INNER JOIN crm.lu_contact_method cm ON cm.contact_method_id = ccm.contact_method_id
            WHERE ccm.opportunity_id = ?
            ORDER BY ccm.is_primary DESC NULLS LAST, ccm.created_on
            """, orgClientId, opportunityId);
    }

    public UUID addContactMethod(UUID opportunityId, UUID contactMethodId, boolean isPrimary, UUID createdBy) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.client_contact_method (client_contact_method_id, opportunity_id, contact_method_id, is_primary, created_by)
            VALUES (?, ?, ?, ?, ?)
            """, id, opportunityId, contactMethodId, isPrimary, createdBy);
        return id;
    }

    public int removeContactMethod(UUID orgClientId, UUID opportunityId, UUID clientContactMethodId) {
        return jdbc.update("""
            DELETE FROM crm.client_contact_method ccm
            USING crm.opportunity o
            WHERE ccm.opportunity_id = o.opportunity_id AND o.org_client_id = ?
            AND ccm.opportunity_id = ? AND ccm.client_contact_method_id = ?
            """, orgClientId, opportunityId, clientContactMethodId);
    }

    public boolean contactMethodExists(UUID opportunityId, UUID clientContactMethodId) {
        Long n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM crm.client_contact_method WHERE opportunity_id = ? AND client_contact_method_id = ?",
                Long.class, opportunityId, clientContactMethodId);
        return n != null && n > 0;
    }

    public UUID resolveDefaultOpportunityStageId(UUID orgClientId) {
        List<UUID> list = jdbc.query(
                "SELECT opportunity_stage_id FROM crm.lu_opportunity_stage WHERE org_client_id = ? AND is_active = true ORDER BY display_order, display_name LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("opportunity_stage_id"), orgClientId);
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * Placeholder API: brand name (org), opportunity first/last/display name, sales advisor name and title.
     * Returns null if opportunity does not exist.
     */
    public Map<String, Object> findOpportunityPlaceholderData(UUID opportunityId, UUID salesAdvisorId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT
                oc.name AS brand_name,
                o.first_name,
                o.last_name,
                COALESCE(TRIM(o.full_name), NULLIF(TRIM(COALESCE(o.first_name, '') || ' ' || COALESCE(o.last_name, '')), '')) AS display_name,
                TRIM(COALESCE(u.first_name, '') || ' ' || COALESCE(u.last_name, '')) AS sales_advisor_name,
                (SELECT r.name
                 FROM "access".access_application_user aau
                 JOIN "access".access_user_role_location aurl ON aurl.application_user_id = aau.application_user_id
                 JOIN "access".access_role r ON r.role_id = aurl.role_id AND r.is_active = true
                 WHERE aau.user_id = ?
                   AND aau.is_active = true
                 LIMIT 1) AS sales_advisor_title
            FROM crm.opportunity o
            JOIN common.organization_client oc ON oc.org_client_id = o.org_client_id
            LEFT JOIN "access".access_user u ON u.user_id = ?
            WHERE o.opportunity_id = ?
            """, salesAdvisorId, salesAdvisorId, opportunityId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Linked tab: contact associated with the opportunity, with account_name if contact has an account.
     */
    public Map<String, Object> findLinkedContactByOpportunity(UUID orgClientId, UUID opportunityId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT c.contact_id, c.full_name, c.email, c.phone, acc.account_name
            FROM crm.opportunity o
            INNER JOIN crm.contact c ON c.contact_id = o.contact_id AND c.org_client_id = o.org_client_id
            LEFT JOIN crm.account acc ON acc.account_id = c.account_id AND acc.org_client_id = c.org_client_id
            WHERE o.org_client_id = ? AND o.opportunity_id = ?
            """, orgClientId, opportunityId);
        return rows.isEmpty() ? null : rows.get(0);
    }
}
