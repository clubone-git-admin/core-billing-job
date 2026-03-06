package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class CrmContactRepository {

    private final JdbcTemplate jdbc;

    public CrmContactRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean contactExists(UUID orgClientId, UUID contactId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.contact WHERE org_client_id = ? AND contact_id = ?)",
                Boolean.class, orgClientId, contactId);
        return Boolean.TRUE.equals(exists);
    }

    /** Map view to lifecycle_code when applicable. lifecycle_code param overrides. */
    public List<Map<String, Object>> listContacts(UUID orgClientId, String view, String lifecycleCode,
                                                   String search, UUID ownerId, int limit, int offset) {
        String lifeCode = (lifecycleCode != null && !lifecycleCode.isBlank()) ? lifecycleCode
                : mapViewToLifecycleCode(view);
        StringBuilder sql = new StringBuilder("""
            SELECT
                c.contact_id,
                c.full_name,
                c.contact_code,
                cr.role_id   as client_id,
                c.email,
                c.phone,
                acc.account_name,
                cl.code AS lifecycle_code,
                cl.display_name AS lifecycle_display_name,
                loc.display_name AS home_club_name,
                TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                c.created_by AS owner_id,
                c.created_on
            FROM crm.contact c
            LEFT JOIN crm.lu_contact_lifecycle cl ON cl.contact_lifecycle_id = c.contact_lifecycle_id
            LEFT JOIN crm.account acc ON acc.account_id = c.account_id AND acc.org_client_id = c.org_client_id
            LEFT JOIN locations."location" loc ON loc.location_id = c.home_location_id
            LEFT JOIN "access".access_user u ON u.user_id = c.owner_user_id
            LEFT JOIN clients.client_role cr ON cr.client_role_id = c.client_id
            WHERE c.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (lifeCode != null && !lifeCode.isBlank()) {
            sql.append(" AND cl.code = ? ");
            params.add(lifeCode);
        }
        if (ownerId != null) {
            sql.append(" AND c.created_by = ? ");
            params.add(ownerId);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.full_name ILIKE ? OR c.email ILIKE ? OR c.phone ILIKE ? OR c.contact_code ILIKE ? OR acc.account_name ILIKE ? OR c.client_id::text ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        sql.append(" ORDER BY c.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countContacts(UUID orgClientId, String view, String lifecycleCode, String search, UUID ownerId) {
        String lifeCode = (lifecycleCode != null && !lifecycleCode.isBlank()) ? lifecycleCode
                : mapViewToLifecycleCode(view);
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*)
            FROM crm.contact c
            LEFT JOIN crm.lu_contact_lifecycle cl ON cl.contact_lifecycle_id = c.contact_lifecycle_id
            LEFT JOIN crm.account acc ON acc.account_id = c.account_id AND acc.org_client_id = c.org_client_id
            WHERE c.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (lifeCode != null && !lifeCode.isBlank()) {
            sql.append(" AND cl.code = ? ");
            params.add(lifeCode);
        }
        if (ownerId != null) {
            sql.append(" AND c.created_by = ? ");
            params.add(ownerId);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.full_name ILIKE ? OR c.email ILIKE ? OR c.phone ILIKE ? OR c.contact_code ILIKE ? OR acc.account_name ILIKE ? OR c.client_id::text ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static String mapViewToLifecycleCode(String view) {
        if (view == null || view.isBlank() || "all".equalsIgnoreCase(view)) return null;
        return switch (view.toUpperCase()) {
            case "MEMBERS" -> "MEMBER";
            case "PROSPECTS" -> "PROSPECT";
            case "EX_MEMBERS" -> "EX_MEMBER";
            default -> null;
        };
    }

    public Map<String, Object> findContactById(UUID orgClientId, UUID contactId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT
                c.contact_id,
                c.full_name,
                c.first_name,
                c.last_name,
                c.contact_code,
                cr.role_id as  client_id,
                c.email,
                c.phone,
                cl.code AS lifecycle_code,
                cl.display_name AS lifecycle_display_name,
                c.account_id,
                acc.account_name,
                c.home_location_id,
                loc.display_name AS home_club_name,
                c.created_by AS owner_id,
                TRIM(COALESCE(own.first_name,'') || ' ' || COALESCE(own.last_name,'')) AS owner_name,
                sal.display_name AS salutation_display_name,
                g.display_name AS gender_display_name,
                c.date_of_birth,
                c.consent_to_contact,
                c.consent_to_marketing,
                c.has_opt_out_sms,
                c.has_opt_out_email,
                c.created_on,
                c.created_by,
                c.modified_on,
                c.modified_by,
                c.tags
            FROM crm.contact c
            LEFT JOIN crm.lu_contact_lifecycle cl ON cl.contact_lifecycle_id = c.contact_lifecycle_id
            LEFT JOIN crm.account acc ON acc.account_id = c.account_id AND acc.org_client_id = c.org_client_id
            LEFT JOIN locations."location" loc ON loc.location_id = c.home_location_id
            LEFT JOIN "access".access_user own ON own.user_id = c.created_by
            LEFT JOIN crm.lu_salutation sal ON sal.salutation_id = c.salutation_id
            LEFT JOIN crm.lu_gender g ON g.gender_id = c.gender_id
            LEFT JOIN clients.client_role cr ON cr.client_role_id = c.client_id
            WHERE c.org_client_id = ? AND c.contact_id = ?
            """, orgClientId, contactId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void updateContact(UUID orgClientId, UUID contactId, Map<String, Object> updates, UUID modifiedBy) {
        if (updates == null || updates.isEmpty()) return;
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (updates.containsKey("first_name")) { setClauses.add("first_name = ?"); params.add(updates.get("first_name")); }
        if (updates.containsKey("last_name")) { setClauses.add("last_name = ?"); params.add(updates.get("last_name")); }
        if (updates.containsKey("email")) { setClauses.add("email = ?"); params.add(updates.get("email")); }
        if (updates.containsKey("phone")) { setClauses.add("phone = ?"); params.add(updates.get("phone")); }
        if (updates.containsKey("contact_lifecycle_id")) { setClauses.add("contact_lifecycle_id = ?"); params.add(updates.get("contact_lifecycle_id")); }
        if (updates.containsKey("account_id")) { setClauses.add("account_id = ?"); params.add(updates.get("account_id")); }
        if (updates.containsKey("home_location_id")) { setClauses.add("home_location_id = ?"); params.add(updates.get("home_location_id")); }
        if (updates.containsKey("salutation_id")) { setClauses.add("salutation_id = ?"); params.add(updates.get("salutation_id")); }
        if (updates.containsKey("gender_id")) { setClauses.add("gender_id = ?"); params.add(updates.get("gender_id")); }
        if (updates.containsKey("date_of_birth")) { setClauses.add("date_of_birth = ?"); params.add(updates.get("date_of_birth")); }
        if (updates.containsKey("consent_to_contact")) { setClauses.add("consent_to_contact = ?"); params.add(updates.get("consent_to_contact")); }
        if (updates.containsKey("consent_to_marketing")) { setClauses.add("consent_to_marketing = ?"); params.add(updates.get("consent_to_marketing")); }
        if (updates.containsKey("has_opt_out_sms")) { setClauses.add("has_opt_out_sms = ?"); params.add(updates.get("has_opt_out_sms")); }
        if (updates.containsKey("has_opt_out_email")) { setClauses.add("has_opt_out_email = ?"); params.add(updates.get("has_opt_out_email")); }
        if (updates.containsKey("tags")) { setClauses.add("tags = ?::jsonb"); params.add(updates.get("tags")); }
        if (setClauses.isEmpty()) return;
        setClauses.add("modified_on = CURRENT_TIMESTAMP");
        setClauses.add("modified_by = ?");
        params.add(modifiedBy);
        params.add(orgClientId);
        params.add(contactId);
        String sql = "UPDATE crm.contact SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND contact_id = ?";
        jdbc.update(sql, params.toArray());
    }

    public void updateContactOwner(UUID orgClientId, UUID contactId, UUID ownerId, UUID modifiedBy) {
        jdbc.update("""
            UPDATE crm.contact SET created_by = ?, modified_on = CURRENT_TIMESTAMP, modified_by = ?
            WHERE org_client_id = ? AND contact_id = ?
            """, ownerId, modifiedBy, orgClientId, contactId);
    }

    public int bulkUpdateOwner(UUID orgClientId, List<UUID> contactIds, UUID ownerId, UUID modifiedBy) {
        if (contactIds == null || contactIds.isEmpty()) return 0;
        String placeholders = String.join(",", contactIds.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(ownerId);
        params.add(modifiedBy);
        params.add(orgClientId);
        params.addAll(contactIds);
        return jdbc.update("""
            UPDATE crm.contact SET created_by = ?, modified_on = CURRENT_TIMESTAMP, modified_by = ?
            WHERE org_client_id = ? AND contact_id IN (""" + placeholders + ")",
                params.toArray());
    }

    public UUID resolveEntityTypeIdForContact(UUID orgClientId) {
        List<UUID> ids = jdbc.query("""
            SELECT entity_type_id FROM crm.lu_entity_type
            WHERE org_client_id = ? AND UPPER(TRIM(code)) = 'CONTACT' LIMIT 1
            """, (rs, i) -> (UUID) rs.getObject("entity_type_id"), orgClientId);
        return ids.isEmpty() ? null : ids.get(0);
    }

    public List<Map<String, Object>> listNotes(UUID orgClientId, UUID entityTypeId, UUID contactId) {
        return jdbc.queryForList("""
            SELECT n.note_id, n.entity_id AS contact_id, n.created_on,
                   COALESCE(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), 'Unknown') AS author,
                   n.content
            FROM crm.note n
            LEFT JOIN "access".access_user u ON u.user_id = n.created_by
            WHERE n.org_client_id = ? AND n.entity_type_id = ? AND n.entity_id = ? AND COALESCE(n.is_deleted, false) = false
            ORDER BY n.created_on DESC
            """, orgClientId, entityTypeId, contactId);
    }

    public Map<String, Object> insertNote(UUID orgClientId, UUID entityTypeId, UUID contactId, UUID createdBy, String body) {
        UUID noteId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.note (note_id, org_client_id, entity_type_id, entity_id, content, is_rich_text, visibility, created_by)
            VALUES (?, ?, ?, ?, ?, false, 'INTERNAL', ?)
            """, noteId, orgClientId, entityTypeId, contactId, body, createdBy);
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT n.note_id, n.entity_id AS contact_id, n.created_on, n.created_by::text, n.content
            FROM crm.note n WHERE n.org_client_id = ? AND n.note_id = ?
            """, orgClientId, noteId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> findRelatedAccountByContact(UUID orgClientId, UUID contactId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT acc.account_id, acc.account_name, at.display_name AS type_display_name, acc.phone, NULL AS address
            FROM crm.contact c
            JOIN crm.account acc ON acc.account_id = c.account_id AND acc.org_client_id = c.org_client_id
            LEFT JOIN crm.lu_account_type at ON at.account_type_id = acc.account_type_id
            WHERE c.org_client_id = ? AND c.contact_id = ? AND c.account_id IS NOT NULL
            """, orgClientId, contactId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> findOpportunitiesByContact(UUID orgClientId, UUID contactId) {
        return jdbc.queryForList("""
            SELECT o.opportunity_id, o.full_name AS opportunity_name,
                   st.code AS stage_code, st.display_name AS stage_display_name,
                   NULL::float8 AS amount, NULL::date AS expected_close_date,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name
            FROM crm.opportunity o
            LEFT JOIN crm.lu_opportunity_stage st ON st.opportunity_stage_id = o.opportunity_stage_id
            LEFT JOIN "access".access_user u ON u.user_id = o.owner_user_id
            WHERE o.org_client_id = ? AND o.contact_id = ?
            ORDER BY o.modified_on DESC NULLS LAST
            """, orgClientId, contactId);
    }

    public List<Map<String, Object>> findCasesByContact(UUID orgClientId, UUID contactId, String search, String statusCode, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT c.case_id, c.case_number, c.subject,
                   ct.display_name AS case_type_display_name,
                   cs.display_name AS case_status_display_name,
                   cp.display_name AS case_priority_display_name,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                   c.created_on, c.sla_resolve_due_at
            FROM crm.cases c
            LEFT JOIN crm.lu_case_type ct ON ct.case_type_id = c.case_type_id
            LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
            LEFT JOIN crm.lu_case_priority cp ON cp.case_priority_id = c.case_priority_id
            LEFT JOIN "access".access_user u ON u.user_id = c.owner_user_id
            WHERE c.org_client_id = ? AND c.contact_id = ? AND COALESCE(c.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(contactId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.subject ILIKE ? OR c.case_number ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        if (statusCode != null && !statusCode.isBlank()) {
            sql.append(" AND cs.code = ? ");
            params.add(statusCode);
        }
        sql.append(" ORDER BY c.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countCasesByContact(UUID orgClientId, UUID contactId, String search, String statusCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*) FROM crm.cases c
            LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
            WHERE c.org_client_id = ? AND c.contact_id = ? AND COALESCE(c.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(contactId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.subject ILIKE ? OR c.case_number ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        if (statusCode != null && !statusCode.isBlank()) {
            sql.append(" AND cs.code = ? ");
            params.add(statusCode);
        }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String CASE_SELECT_FROM = """
        SELECT c.case_id, c.case_number, c.subject,
               ct.display_name AS case_type_display_name,
               cs.display_name AS case_status_display_name,
               cp.display_name AS case_priority_display_name,
               TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
               c.created_on, c.sla_resolve_due_at
        FROM crm.cases c
        LEFT JOIN crm.lu_case_type ct ON ct.case_type_id = c.case_type_id
        LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
        LEFT JOIN crm.lu_case_priority cp ON cp.case_priority_id = c.case_priority_id
        LEFT JOIN "access".access_user u ON u.user_id = c.owner_user_id
        WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
        """;

    /** Returns the case row in same shape as findCasesByContact, or null if not found. */
    public Map<String, Object> findCaseById(UUID orgClientId, UUID caseId) {
        List<Map<String, Object>> list = jdbc.queryForList(
                CASE_SELECT_FROM + " AND c.case_id = ?", orgClientId, caseId);
        return list.isEmpty() ? null : list.get(0);
    }

    private static final String CASE_DETAIL_SELECT = """
        SELECT c.case_id, c.case_number, c.subject,
               c.case_type_id, ct.display_name AS case_type_display_name,
               c.case_status_id, cs.display_name AS case_status_display_name,
               c.case_priority_id, cp.display_name AS case_priority_display_name,
               c.owner_user_id, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
               c.created_on, c.sla_resolve_due_at, c.description
        FROM crm.cases c
        LEFT JOIN crm.lu_case_type ct ON ct.case_type_id = c.case_type_id
        LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
        LEFT JOIN crm.lu_case_priority cp ON cp.case_priority_id = c.case_priority_id
        LEFT JOIN "access".access_user u ON u.user_id = c.owner_user_id
        WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false AND c.case_id = ?
        """;

    /** Returns case detail with IDs for operations (GET /api/crm/cases/{caseId}). */
    public Map<String, Object> findCaseDetailById(UUID orgClientId, UUID caseId) {
        List<Map<String, Object>> list = jdbc.queryForList(CASE_DETAIL_SELECT, orgClientId, caseId);
        return list.isEmpty() ? null : list.get(0);
    }

    /** Same as findCaseById but filters by contact_id (for created case response). */
    public Map<String, Object> findCaseByIdAndContact(UUID orgClientId, UUID contactId, UUID caseId) {
        List<Map<String, Object>> list = jdbc.queryForList(
                CASE_SELECT_FROM + " AND c.contact_id = ? AND c.case_id = ?", orgClientId, contactId, caseId);
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean caseTypeExists(UUID orgClientId, UUID caseTypeId) {
        if (caseTypeId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_case_type WHERE org_client_id = ? AND case_type_id = ? AND is_active = true)",
                Boolean.class, orgClientId, caseTypeId);
        return Boolean.TRUE.equals(b);
    }

    public boolean caseStatusExists(UUID orgClientId, UUID caseStatusId) {
        if (caseStatusId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_case_status WHERE org_client_id = ? AND case_status_id = ? AND is_active = true)",
                Boolean.class, orgClientId, caseStatusId);
        return Boolean.TRUE.equals(b);
    }

    public boolean casePriorityExists(UUID orgClientId, UUID casePriorityId) {
        if (casePriorityId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_case_priority WHERE org_client_id = ? AND case_priority_id = ? AND is_active = true)",
                Boolean.class, orgClientId, casePriorityId);
        return Boolean.TRUE.equals(b);
    }

    /** Resolve default case status by code (e.g. NEW or OPEN). Returns null if not found. */
    public UUID resolveDefaultCaseStatusIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) return null;
        List<UUID> list = jdbc.query(
                "SELECT case_status_id FROM crm.lu_case_status WHERE org_client_id = ? AND UPPER(code) = UPPER(?) AND is_active = true",
                (rs, i) -> (UUID) rs.getObject("case_status_id"), orgClientId, code.trim());
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * Insert a case. contact_id is set from parameter; case_number is generated by DB trigger.
     * Returns the generated case_id.
     */
    public UUID insertCase(UUID orgClientId, UUID contactId, UUID caseTypeId, UUID caseStatusId, UUID casePriorityId,
                           UUID accountId, UUID opportunityId, UUID ownerUserId, String channelCode,
                           String subject, String description, String internalNotes, UUID createdBy) {
        UUID caseId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.cases (case_id, org_client_id, contact_id, case_type_id, case_status_id, case_priority_id,
                account_id, opportunity_id, owner_user_id, channel_code, subject, description, internal_notes, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                caseId, orgClientId, contactId, caseTypeId, caseStatusId, casePriorityId,
                accountId, opportunityId, ownerUserId, channelCode, subject, description, internalNotes, createdBy);
        return caseId;
    }

    /**
     * Update case fields (case_status_id, case_priority_id, owner_user_id). Only provided keys are updated.
     * Status history is maintained by trigger trigger_case_status_history.
     */
    public int updateCase(UUID orgClientId, UUID caseId, Map<String, Object> updates, UUID modifiedBy) {
        if (updates == null || updates.isEmpty()) return 0;
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (updates.containsKey("case_status_id")) { setClauses.add("case_status_id = ?"); params.add(updates.get("case_status_id")); }
        if (updates.containsKey("case_priority_id")) { setClauses.add("case_priority_id = ?"); params.add(updates.get("case_priority_id")); }
        if (updates.containsKey("owner_user_id")) { setClauses.add("owner_user_id = ?"); params.add(updates.get("owner_user_id")); }
        if (setClauses.isEmpty()) return 0;
        setClauses.add("modified_by = ?");
        params.add(modifiedBy);
        params.add(orgClientId);
        params.add(caseId);
        String sql = "UPDATE crm.cases SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND case_id = ?";
        return jdbc.update(sql, params.toArray());
    }

    public static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime odt) return odt.toString();
        if (value instanceof Timestamp ts) return ts.toInstant().atOffset(ZoneOffset.UTC).toString();
        return value.toString();
    }
}
