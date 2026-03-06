package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM cases (list across org, not tied to a single contact).
 */
@Repository
public class CrmCaseRepository {

    private final JdbcTemplate jdbc;

    public CrmCaseRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String LIST_SELECT = """
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
        LEFT JOIN crm.contact cont ON cont.contact_id = c.contact_id AND cont.org_client_id = c.org_client_id
        LEFT JOIN crm.account acc ON acc.account_id = c.account_id
        WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
        """;

    public List<Map<String, Object>> listCases(UUID orgClientId, UUID ownerUserId, String search,
                                               UUID caseTypeId, UUID caseStatusId, UUID casePriorityId,
                                               int limit, int offset) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (ownerUserId != null) {
            sql.append(" AND c.owner_user_id = ? ");
            params.add(ownerUserId);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.subject ILIKE ? OR c.case_number ILIKE ? OR cont.full_name ILIKE ? OR acc.account_name ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (caseTypeId != null) {
            sql.append(" AND c.case_type_id = ? ");
            params.add(caseTypeId);
        }
        if (caseStatusId != null) {
            sql.append(" AND c.case_status_id = ? ");
            params.add(caseStatusId);
        }
        if (casePriorityId != null) {
            sql.append(" AND c.case_priority_id = ? ");
            params.add(casePriorityId);
        }
        sql.append(" ORDER BY c.modified_on DESC NULLS LAST, c.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countCases(UUID orgClientId, UUID ownerUserId, String search,
                           UUID caseTypeId, UUID caseStatusId, UUID casePriorityId) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*) FROM crm.cases c
            LEFT JOIN crm.contact cont ON cont.contact_id = c.contact_id AND cont.org_client_id = c.org_client_id
            LEFT JOIN crm.account acc ON acc.account_id = c.account_id
            WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (ownerUserId != null) {
            sql.append(" AND c.owner_user_id = ? ");
            params.add(ownerUserId);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.subject ILIKE ? OR c.case_number ILIKE ? OR cont.full_name ILIKE ? OR acc.account_name ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (caseTypeId != null) {
            sql.append(" AND c.case_type_id = ? ");
            params.add(caseTypeId);
        }
        if (caseStatusId != null) {
            sql.append(" AND c.case_status_id = ? ");
            params.add(caseStatusId);
        }
        if (casePriorityId != null) {
            sql.append(" AND c.case_priority_id = ? ");
            params.add(casePriorityId);
        }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String HISTORY_SELECT = """
        SELECT h.history_id, h.case_status_id, h.previous_case_status_id,
               cs.display_name AS status_display_name,
               pcs.display_name AS previous_status_display_name,
               h.entered_on, h.exited_on, h.duration_seconds,
               TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS changed_by_display_name,
               h.change_reason, h.change_source, h.notes
        FROM crm.case_status_history h
        LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = h.case_status_id
        LEFT JOIN crm.lu_case_status pcs ON pcs.case_status_id = h.previous_case_status_id
        LEFT JOIN "access".access_user u ON u.user_id = h.changed_by_user_id
        WHERE h.org_client_id = ? AND h.case_id = ?
        ORDER BY h.entered_on DESC
        """;

    public List<Map<String, Object>> listCaseStatusHistory(UUID orgClientId, UUID caseId) {
        return jdbc.queryForList(HISTORY_SELECT, orgClientId, caseId);
    }
}
