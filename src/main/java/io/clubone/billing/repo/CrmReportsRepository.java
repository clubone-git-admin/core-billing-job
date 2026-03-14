package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM report queries. Reports are location(s)-specific via location_ids (list).
 * DDL-aligned: crm.leads has no is_deleted; crm.opportunity has no amount column.
 */
@Repository
public class CrmReportsRepository {

    private final JdbcTemplate jdbc;

    public CrmReportsRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Append AND home_location_id IN (?,?,...) when locationIds is non-null and non-empty. */
    private static void appendLocationFilter(String columnQualifier, List<UUID> locationIds, StringBuilder sql, List<Object> params) {
        if (locationIds != null && !locationIds.isEmpty()) {
            sql.append(" AND ").append(columnQualifier).append(" IN (");
            for (int i = 0; i < locationIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
                params.add(locationIds.get(i));
            }
            sql.append(") ");
        }
    }

    /** 1. Location-specific leads (location_ids = null or empty = all locations). */
    public List<Map<String, Object>> getLeadsByLocation(UUID orgClientId, List<UUID> locationIds, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT l.lead_id, l.full_name, l.email, l.phone, ls.code AS status_code, ls.display_name AS status_display_name,
                   src.display_name AS source_display_name, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                   l.created_on, loc.name AS location_name
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            LEFT JOIN crm.lu_lead_source src ON src.lead_source_id = l.lead_source_id
            LEFT JOIN "access".access_user u ON u.user_id = l.owner_user_id
            LEFT JOIN locations."location" loc ON loc.location_id = l.home_location_id
            WHERE l.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        sql.append(" ORDER BY l.created_on DESC ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ? ");
                params.add(offset);
            }
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countLeadsByLocation(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM crm.leads l WHERE l.org_client_id = ? ");
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    /** 2. Location-specific opportunities. */
    public List<Map<String, Object>> getOpportunitiesByLocation(UUID orgClientId, List<UUID> locationIds, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT o.opportunity_id, o.opportunity_code, o.full_name, os.code AS stage_code, os.display_name AS stage_display_name,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name, o.created_on, loc.name AS location_name
            FROM crm.opportunity o
            LEFT JOIN crm.lu_opportunity_stage os ON os.opportunity_stage_id = o.opportunity_stage_id AND os.org_client_id = o.org_client_id
            LEFT JOIN "access".access_user u ON u.user_id = o.owner_user_id
            LEFT JOIN locations."location" loc ON loc.location_id = o.home_location_id
            WHERE o.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("o.home_location_id", locationIds, sql, params);
        sql.append(" ORDER BY o.modified_on DESC NULLS LAST, o.created_on DESC ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ? ");
                params.add(offset);
            }
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countOpportunitiesByLocation(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM crm.opportunity o WHERE o.org_client_id = ? ");
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("o.home_location_id", locationIds, sql, params);
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    /** 3. Lead Source Report – count by lead source. Location filter in JOIN so all sources returned. */
    public List<Map<String, Object>> getLeadSourceReport(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("""
            SELECT src.lead_source_id, src.code AS source_code, src.display_name AS source_display_name,
                   COUNT(l.lead_id) AS lead_count
            FROM crm.lu_lead_source src
            LEFT JOIN crm.leads l ON l.lead_source_id = src.lead_source_id AND l.org_client_id = src.org_client_id
            """);
        List<Object> params = new ArrayList<>();
        if (locationIds != null && !locationIds.isEmpty()) {
            sql.append(" AND l.home_location_id IN (");
            for (int i = 0; i < locationIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
                params.add(locationIds.get(i));
            }
            sql.append(") ");
        }
        sql.append(" WHERE src.org_client_id = ? AND src.is_active = true ");
        params.add(orgClientId);
        sql.append(" GROUP BY src.lead_source_id, src.code, src.display_name, src.display_order ORDER BY src.display_order, src.display_name ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 4. Lead Conversion Report – converted vs not. DDL: crm.leads has no is_deleted. */
    public List<Map<String, Object>> getLeadConversionReport(UUID orgClientId, List<UUID> locationIds, String dateFrom, String dateTo) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                CASE WHEN l.converted_contact_id IS NOT NULL THEN 'CONVERTED' ELSE 'NOT_CONVERTED' END AS conversion_status,
                src.display_name AS source_display_name,
                COUNT(*) AS lead_count
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_source src ON src.lead_source_id = l.lead_source_id
            WHERE l.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND l.created_on::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND l.created_on::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" GROUP BY CASE WHEN l.converted_contact_id IS NOT NULL THEN 'CONVERTED' ELSE 'NOT_CONVERTED' END, src.display_name ORDER BY conversion_status, source_display_name ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 5. Lead Aging Report – count by status and age bucket. DDL: crm.leads has no is_deleted. */
    public List<Map<String, Object>> getLeadAgingReport(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("""
            SELECT ls.code AS status_code, ls.display_name AS status_display_name,
                   CASE
                     WHEN (CURRENT_DATE - l.created_on::date) <= 7 THEN '0-7 days'
                     WHEN (CURRENT_DATE - l.created_on::date) <= 30 THEN '8-30 days'
                     WHEN (CURRENT_DATE - l.created_on::date) <= 90 THEN '31-90 days'
                     ELSE '90+ days'
                   END AS age_bucket,
                   COUNT(*) AS lead_count
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            WHERE l.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        sql.append("""
            GROUP BY ls.code, ls.display_name, ls.display_order,
                   CASE
                     WHEN (CURRENT_DATE - l.created_on::date) <= 7 THEN '0-7 days'
                     WHEN (CURRENT_DATE - l.created_on::date) <= 30 THEN '8-30 days'
                     WHEN (CURRENT_DATE - l.created_on::date) <= 90 THEN '31-90 days'
                     ELSE '90+ days'
                   END
            ORDER BY ls.display_order, age_bucket
            """);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 6. Lead Owner Performance. DDL: crm.leads has no is_deleted. */
    public List<Map<String, Object>> getLeadOwnerPerformanceReport(UUID orgClientId, List<UUID> locationIds, String dateFrom, String dateTo) {
        StringBuilder sql = new StringBuilder("""
            SELECT l.owner_user_id, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                   COUNT(*) AS total_leads,
                   COUNT(l.converted_contact_id) AS converted_count
            FROM crm.leads l
            LEFT JOIN "access".access_user u ON u.user_id = l.owner_user_id
            WHERE l.org_client_id = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND l.created_on::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND l.created_on::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" GROUP BY l.owner_user_id, u.first_name, u.last_name ORDER BY total_leads DESC ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 7. Campaign Performance – campaigns with expected vs actual leads/revenue. */
    public List<Map<String, Object>> getCampaignPerformanceReport(UUID orgClientId, String dateFrom, String dateTo) {
        StringBuilder sql = new StringBuilder("""
            SELECT c.campaign_id, c.campaign_name, ct.display_name AS campaign_type_display_name, cs.display_name AS campaign_status_display_name,
                   c.start_date, c.end_date, c.expected_leads, c.actual_leads, c.expected_revenue, c.actual_revenue, c.conversion_rate
            FROM crm.campaign c
            LEFT JOIN crm.lu_campaign_type ct ON ct.campaign_type_id = c.campaign_type_id
            LEFT JOIN crm.lu_campaign_status cs ON cs.campaign_status_id = c.campaign_status_id
            WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND c.start_date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND (c.end_date IS NULL OR c.end_date <= ?::date) ");
            params.add(dateTo);
        }
        sql.append(" ORDER BY c.start_date DESC NULLS LAST ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 9. Customer Interaction Timeline – recent activities across org (all entity types). */
    public List<Map<String, Object>> getCustomerInteractionTimelineReport(UUID orgClientId, String entityTypeCode, String dateFrom, String dateTo, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT a.activity_id, a.entity_type_id, et.code AS entity_type_code, a.entity_id, a.subject, a.start_date_time, a.created_on,
                   at.code AS activity_type_code, at.display_name AS activity_type_display_name,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name
            FROM crm.activity a
            LEFT JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id AND at.org_client_id = a.org_client_id
            LEFT JOIN crm.lu_entity_type et ON et.entity_type_id = a.entity_type_id AND et.org_client_id = a.org_client_id
            LEFT JOIN "access".access_user u ON u.user_id = a.owner_user_id
            WHERE a.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (entityTypeCode != null && !entityTypeCode.isBlank()) {
            sql.append(" AND UPPER(TRIM(et.code)) = UPPER(TRIM(?)) ");
            params.add(entityTypeCode);
        }
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND COALESCE(a.start_date_time, a.created_on)::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND COALESCE(a.start_date_time, a.created_on)::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" ORDER BY COALESCE(a.start_date_time, a.created_on) DESC NULLS LAST ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ? ");
                params.add(offset);
            }
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 10. Calls Made Report – activities of type CALL with date filter. locationIds reserved for future entity-location filter. */
    public List<Map<String, Object>> getCallsMadeReport(UUID orgClientId, List<UUID> locationIds, String dateFrom, String dateTo, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT a.activity_id, a.start_date_time, a.subject, at.code AS activity_type_code,
                   ac.call_duration_seconds, cd.code AS call_direction_code, cr.code AS call_result_code,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                   et.code AS entity_type_code, a.entity_id
            FROM crm.activity a
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id AND at.org_client_id = a.org_client_id AND UPPER(TRIM(at.code)) = 'CALL'
            LEFT JOIN crm.activity_call ac ON ac.activity_id = a.activity_id AND ac.org_client_id = a.org_client_id
            LEFT JOIN crm.lu_call_direction cd ON cd.call_direction_id = ac.call_direction_id
            LEFT JOIN crm.lu_call_result cr ON cr.call_result_id = ac.call_result_id
            LEFT JOIN "access".access_user u ON u.user_id = a.owner_user_id
            LEFT JOIN crm.lu_entity_type et ON et.entity_type_id = a.entity_type_id AND et.org_client_id = a.org_client_id
            WHERE a.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND a.start_date_time::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND a.start_date_time::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" ORDER BY a.start_date_time DESC NULLS LAST ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ? ");
                params.add(offset);
            }
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 11. Meetings Scheduled – activities of type EVENT. */
    public List<Map<String, Object>> getMeetingsScheduledReport(UUID orgClientId, String dateFrom, String dateTo, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT a.activity_id, a.start_date_time, a.end_date_time, a.subject, at.code AS activity_type_code,
                   TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS owner_name,
                   et.code AS entity_type_code, a.entity_id
            FROM crm.activity a
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id AND at.org_client_id = a.org_client_id AND UPPER(TRIM(at.code)) = 'EVENT'
            LEFT JOIN "access".access_user u ON u.user_id = a.owner_user_id
            LEFT JOIN crm.lu_entity_type et ON et.entity_type_id = a.entity_type_id AND et.org_client_id = a.org_client_id
            WHERE a.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND a.start_date_time::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND a.start_date_time::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" ORDER BY a.start_date_time DESC NULLS LAST ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ? ");
                params.add(offset);
            }
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 12. Case Tickets by Category – count by case_type. */
    public List<Map<String, Object>> getCaseTicketsByCategoryReport(UUID orgClientId, String dateFrom, String dateTo) {
        StringBuilder sql = new StringBuilder("""
            SELECT ct.case_type_id, ct.code AS case_type_code, ct.display_name AS case_type_display_name,
                   cs.display_name AS case_status_display_name, COUNT(c.case_id) AS case_count
            FROM crm.lu_case_type ct
            LEFT JOIN crm.cases c ON c.case_type_id = ct.case_type_id AND c.org_client_id = ct.org_client_id AND COALESCE(c.is_deleted, false) = false
            LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
            WHERE ct.org_client_id = ? AND ct.is_active = true
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND c.created_on::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND c.created_on::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" GROUP BY ct.case_type_id, ct.code, ct.display_name, ct.display_order, cs.display_name, cs.display_order ORDER BY ct.display_order, case_count DESC ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 13. SLA Compliance – cases with sla_resolve_due_at; compliant if resolved on or before due (or status closed). */
    public List<Map<String, Object>> getSlaComplianceReport(UUID orgClientId, String dateFrom, String dateTo) {
        String sql = """
            SELECT c.case_id, c.case_number, c.subject, ct.display_name AS case_type_display_name, cp.display_name AS case_priority_display_name,
                   c.created_on, c.sla_resolve_due_at, c.case_status_id, cs.display_name AS case_status_display_name,
                   (CASE WHEN c.sla_resolve_due_at IS NOT NULL AND (c.sla_resolve_due_at < CURRENT_TIMESTAMP AND cs.code NOT IN ('CLOSED','RESOLVED')) THEN 'BREACHED' ELSE 'OK' END) AS sla_status
            FROM crm.cases c
            LEFT JOIN crm.lu_case_type ct ON ct.case_type_id = c.case_type_id
            LEFT JOIN crm.lu_case_priority cp ON cp.case_priority_id = c.case_priority_id
            LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
            WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
            """;
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        StringBuilder full = new StringBuilder(sql);
        if (dateFrom != null && !dateFrom.isBlank()) {
            full.append(" AND c.created_on::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            full.append(" AND c.created_on::date <= ?::date ");
            params.add(dateTo);
        }
        full.append(" ORDER BY c.sla_resolve_due_at ASC NULLS LAST ");
        return jdbc.queryForList(full.toString(), params.toArray());
    }

    /** 14. Sales Forecast – pipeline by stage. DDL: crm.opportunity has no amount column; total_amount and weighted_amount returned as 0. */
    public List<Map<String, Object>> getSalesForecastReport(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("""
            SELECT os.code AS stage_code, os.display_name AS stage_display_name, COALESCE(os.default_probability, 0) AS probability,
                   COUNT(o.opportunity_id) AS opportunity_count,
                   0::numeric AS total_amount,
                   0::numeric AS weighted_amount
            FROM crm.lu_opportunity_stage os
            LEFT JOIN crm.opportunity o ON o.opportunity_stage_id = os.opportunity_stage_id AND o.org_client_id = os.org_client_id
            """);
        List<Object> params = new ArrayList<>();
        if (locationIds != null && !locationIds.isEmpty()) {
            sql.append(" AND o.home_location_id IN (");
            for (int i = 0; i < locationIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
                params.add(locationIds.get(i));
            }
            sql.append(") ");
        }
        sql.append(" WHERE os.org_client_id = ? GROUP BY os.opportunity_stage_id, os.code, os.display_name, os.display_order, os.default_probability ORDER BY os.display_order ");
        params.add(orgClientId);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 15. Lead Growth Trends. DDL: crm.leads has no is_deleted. */
    public List<Map<String, Object>> getLeadGrowthTrendsReport(UUID orgClientId, List<UUID> locationIds, String period, String dateFrom, String dateTo, Integer limit) {
        String periodArg = period != null && period.equalsIgnoreCase("week") ? "week" : period != null && period.equalsIgnoreCase("month") ? "month" : "day";
        String groupBy = "date_trunc('" + periodArg + "', l.created_on)";
        StringBuilder sql = new StringBuilder("SELECT ").append(groupBy).append(" AS period_start, COUNT(*) AS lead_count FROM crm.leads l WHERE l.org_client_id = ? ");
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        appendLocationFilter("l.home_location_id", locationIds, sql, params);
        if (dateFrom != null && !dateFrom.isBlank()) {
            sql.append(" AND l.created_on::date >= ?::date ");
            params.add(dateFrom);
        }
        if (dateTo != null && !dateTo.isBlank()) {
            sql.append(" AND l.created_on::date <= ?::date ");
            params.add(dateTo);
        }
        sql.append(" GROUP BY ").append(groupBy).append(" ORDER BY period_start DESC ");
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ? ");
            params.add(limit);
        }
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** 16. Opportunity pipeline by stage. DDL: crm.opportunity has no amount column; total_amount = 0. */
    public List<Map<String, Object>> getOpportunityPipelineByStageReport(UUID orgClientId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("""
            SELECT os.code AS stage_code, os.display_name AS stage_display_name, COUNT(o.opportunity_id) AS opportunity_count,
                   0::numeric AS total_amount
            FROM crm.lu_opportunity_stage os
            LEFT JOIN crm.opportunity o ON o.opportunity_stage_id = os.opportunity_stage_id AND o.org_client_id = os.org_client_id
            """);
        List<Object> params = new ArrayList<>();
        if (locationIds != null && !locationIds.isEmpty()) {
            sql.append(" AND o.home_location_id IN (");
            for (int i = 0; i < locationIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
                params.add(locationIds.get(i));
            }
            sql.append(") ");
        }
        sql.append(" WHERE os.org_client_id = ? AND os.is_active = true GROUP BY os.opportunity_stage_id, os.code, os.display_name, os.display_order ORDER BY os.display_order ");
        params.add(orgClientId);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }
}
