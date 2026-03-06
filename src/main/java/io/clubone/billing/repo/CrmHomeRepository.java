package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM Home dashboard: pipeline summary, key deals, today's events, today's tasks, items to approve.
 */
@Repository
public class CrmHomeRepository {

    private final JdbcTemplate jdbc;

    public CrmHomeRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Pipeline summary: opportunity counts by stage. crm.opportunity has opportunity_stage_id only (no amount column).
     */
    public List<Map<String, Object>> getPipelineSummary(UUID orgClientId) {
        String sql = """
            SELECT COALESCE(os.code, 'UNKNOWN') AS stage_code,
                   COALESCE(os.display_name, 'Unknown') AS stage_name,
                   COUNT(*)::int AS count,
                   0::double precision AS total_amount
            FROM crm.opportunity o
            LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id AND os.is_active = true
            WHERE o.org_client_id = ?
            GROUP BY os.opportunity_stage_id, os.code, os.display_name, os.display_order
            ORDER BY MIN(os.display_order) NULLS LAST, os.code NULLS LAST
            """;
        return jdbc.queryForList(sql, orgClientId);
    }

    /**
     * Key deals: opportunities with filter my (owner_user_id = currentUser) / team / all. Limit 20.
     * crm.opportunity has full_name, opportunity_stage_id, contact_id, owner_user_id; no amount or expected_close_date.
     */
    public List<Map<String, Object>> getKeyDeals(UUID orgClientId, UUID currentUserId, String filter, int limit) {
        String sql = """
            SELECT o.opportunity_id, COALESCE(o.full_name, '') AS opportunity_name,
                   os.code AS stage_code, os.display_name AS stage_display_name,
                   0::double precision AS amount, NULL::date AS expected_close_date,
                   c.full_name AS contact_name, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name
            FROM crm.opportunity o
            LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id
            LEFT JOIN crm.contact c ON c.contact_id = o.contact_id AND c.org_client_id = o.org_client_id
            LEFT JOIN "access".access_user u ON u.user_id = o.owner_user_id
            WHERE o.org_client_id = ?
            """;
        List<Object> params = new java.util.ArrayList<>();
        params.add(orgClientId);
        if ("my".equalsIgnoreCase(filter) && currentUserId != null) {
            sql += " AND o.owner_user_id = ? ";
            params.add(currentUserId);
        }
        sql += " ORDER BY o.modified_on DESC NULLS LAST LIMIT ? ";
        params.add(limit);
        return jdbc.queryForList(sql, params.toArray());
    }

    /**
     * Today's events: activities of type EVENT (lu_activity_type.code = 'EVENT') where start_date_time::date = date. Limit 20.
     */
    public List<Map<String, Object>> getTodaysEvents(UUID orgClientId, String dateYyyyMmDd, int limit) {
        String sql = """
            SELECT a.activity_id AS event_id, a.subject,
                   a.start_date_time AS start_time, a.end_date_time AS end_time,
                   COALESCE(
                     (SELECT l.full_name FROM crm.leads l WHERE l.lead_id = a.entity_id AND l.org_client_id = a.org_client_id LIMIT 1),
                     (SELECT c.full_name FROM crm.contact c WHERE c.contact_id = a.entity_id AND c.org_client_id = a.org_client_id LIMIT 1),
                     (SELECT o.full_name FROM crm.opportunity o WHERE o.opportunity_id = a.entity_id AND o.org_client_id = a.org_client_id LIMIT 1)
                   ) AS related_entity,
                   COALESCE(es.display_name, 'Scheduled') AS status, aev.event_place AS location
            FROM crm.activity a
            INNER JOIN crm.activity_event aev ON aev.activity_id = a.activity_id AND aev.org_client_id = a.org_client_id
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id AND at.org_client_id = a.org_client_id AND UPPER(TRIM(at.code)) = 'EVENT' AND at.is_active = true
            LEFT JOIN crm.lu_event_status es ON es.event_status_id = aev.event_status_id AND es.org_client_id = aev.org_client_id
            WHERE a.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
            AND a.start_date_time::date = ?::date
            ORDER BY a.start_date_time ASC
            LIMIT ?
            """;
        return jdbc.queryForList(sql, orgClientId, dateYyyyMmDd, limit);
    }

    /**
     * Pending approval requests where current user is an approver (approval_step with decision = 'PENDING').
     * crm.approval_request uses approval_status_id (FK to lu_approval_status), not status_code.
     */
    public List<Map<String, Object>> getItemsToApprove(UUID orgClientId, UUID currentUserId, int limit) {
        String sql = """
            SELECT ar.approval_request_id AS approval_id, ar.request_type,
                   COALESCE(ar.entity_id::text, ar.request_type) AS entity_ref,
                   (COALESCE((ar.payload_json->>'amount')::numeric, (ar.payload_json->>'discountAmount')::numeric, 0))::double precision AS amount,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS requester_name,
                   ar.requested_on AS requested_on
            FROM crm.approval_request ar
            INNER JOIN crm.lu_approval_status las ON las.approval_status_id = ar.approval_status_id AND las.org_client_id = ar.org_client_id AND UPPER(TRIM(las.code)) = 'PENDING'
            INNER JOIN crm.approval_step ast ON ast.approval_request_id = ar.approval_request_id AND ast.org_client_id = ar.org_client_id
             AND (ast.decision IS NULL OR UPPER(TRIM(ast.decision)) = 'PENDING') AND ast.approver_user_id = ?
            LEFT JOIN "access".access_user u ON u.user_id = ar.requested_by_user_id
            WHERE ar.org_client_id = ?
            ORDER BY ar.requested_on DESC NULLS LAST
            LIMIT ?
            """;
        return jdbc.queryForList(sql, currentUserId, orgClientId, limit);
    }

    /** Approve: set step decision = APPROVED and set request approval_status_id to APPROVED (from lu_approval_status). */
    public boolean approve(UUID orgClientId, UUID approvalId, UUID userId) {
        int step = jdbc.update("""
            UPDATE crm.approval_step SET decision = 'APPROVED', decided_by_user_id = ?, decided_on = CURRENT_TIMESTAMP
            WHERE org_client_id = ? AND approval_request_id = ? AND approver_user_id = ? AND (decision IS NULL OR UPPER(TRIM(decision)) = 'PENDING')
            """, userId, orgClientId, approvalId, userId);
        if (step > 0) {
            jdbc.update("""
                UPDATE crm.approval_request SET approval_status_id = (
                    SELECT approval_status_id FROM crm.lu_approval_status WHERE org_client_id = ? AND UPPER(TRIM(code)) = 'APPROVED' AND is_active = true LIMIT 1
                ), resolved_by_user_id = ?, resolved_on = CURRENT_TIMESTAMP
                WHERE org_client_id = ? AND approval_request_id = ?
                """, orgClientId, userId, orgClientId, approvalId);
        }
        return step > 0;
    }

    /** Reject: set step decision = REJECTED and set request approval_status_id to REJECTED (from lu_approval_status). */
    public boolean reject(UUID orgClientId, UUID approvalId, UUID userId) {
        int step = jdbc.update("""
            UPDATE crm.approval_step SET decision = 'REJECTED', decided_by_user_id = ?, decided_on = CURRENT_TIMESTAMP
            WHERE org_client_id = ? AND approval_request_id = ? AND approver_user_id = ? AND (decision IS NULL OR UPPER(TRIM(decision)) = 'PENDING')
            """, userId, orgClientId, approvalId, userId);
        if (step > 0) {
            jdbc.update("""
                UPDATE crm.approval_request SET approval_status_id = (
                    SELECT approval_status_id FROM crm.lu_approval_status WHERE org_client_id = ? AND UPPER(TRIM(code)) = 'REJECTED' AND is_active = true LIMIT 1
                ), resolved_by_user_id = ?, resolved_on = CURRENT_TIMESTAMP
                WHERE org_client_id = ? AND approval_request_id = ?
                """, orgClientId, userId, orgClientId, approvalId);
        }
        return step > 0;
    }
}
