package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Repository for dashboard operations.
 */
@Repository
public class DashboardRepository {

    /**
     * Billable schedule status codes (aligned with
     * {@link io.clubone.billing.repo.InvoiceGenerationRepository} open-for-invoice schedules).
     */
    private static final String SCHEDULE_STATUS_OPEN_SQL = "('PENDING', 'DUE', 'PLANNED')";

    private final JdbcTemplate jdbc;

    public DashboardRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
    }

    /**
     * Get summary statistics for dashboard KPIs.
     */
    public Map<String, Object> getSummaryStats(UUID locationId, LocalDate dateFrom) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                COUNT(DISTINCT br.billing_run_id) AS total_runs,
                COUNT(DISTINCT CASE WHEN brs.status_code = 'RUNNING' THEN br.billing_run_id END) AS active_runs,
                COUNT(DISTINCT CASE WHEN brs.status_code IN ('COMPLETED_SUCCESS', 'COMPLETED_WITH_FAILURES') THEN br.billing_run_id END) AS completed_runs,
                COUNT(DISTINCT CASE WHEN brs.status_code IN ('FAILED_SYSTEM', 'CANCELLED') THEN br.billing_run_id END) AS failed_runs,
                COUNT(DISTINCT sbh.invoice_id) AS total_invoices,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.subscription_billing_history_id END) AS successful_invoices,
                COUNT(DISTINCT sbh.subscription_billing_history_id) AS total_attempts
            FROM client_subscription_billing.billing_run br
            LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE br.application_id = ?::uuid
              AND br.created_on >= ?
            """);

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        params.add(dateFrom.atStartOfDay().atOffset(java.time.ZoneOffset.UTC));

        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        return jdbc.queryForMap(sql.toString(), params.toArray());
    }

    /**
     * Get last run by stage.
     */
    public List<Map<String, Object>> getLastRunsByStage(UUID locationId, LocalDate dateFrom) {
        String sql = """
            SELECT DISTINCT ON (bsc.stage_code)
                br.billing_run_id, br.billing_run_code, br.due_date,
                brs.status_code AS status_code, bsc.stage_code AS current_stage_code,
                br.started_on, br.ended_on,
                COUNT(DISTINCT sbh.invoice_id) AS invoices_count,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount
            FROM client_subscription_billing.billing_run br
            JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            WHERE br.application_id = ?::uuid
              AND br.created_on >= ?
            """;

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        params.add(dateFrom.atStartOfDay().atOffset(java.time.ZoneOffset.UTC));

        if (locationId != null) {
            sql += " AND br.location_id = ?::uuid";
            params.add(locationId.toString());
        }

        sql += """
            GROUP BY br.billing_run_id, br.billing_run_code, br.due_date, brs.status_code,
                     bsc.stage_code, br.started_on, br.ended_on
            ORDER BY bsc.stage_code, br.created_on DESC
            """;

        return jdbc.queryForList(sql, params.toArray());
    }

    /**
     * Get forecast data for dashboard.
     */
    public Map<String, Object> getForecastData(UUID locationId) {
        String sql = """
            SELECT 
                COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN sbs.billing_schedule_id END) AS due_7_days_count,
                COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_7_days_amount,
                COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN sbs.billing_schedule_id END) AS due_30_days_count,
                COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_30_days_amount,
                COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN sbs.billing_schedule_id END) AS due_90_days_count,
                COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_90_days_amount
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id
            WHERE sbs.application_id = ?::uuid
              AND bss.status_code IN """
                    + SCHEDULE_STATUS_OPEN_SQL
                    + """
            """;

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (locationId != null) {
            sql += """
                AND EXISTS (
                    SELECT 1 FROM client_subscription_billing.subscription_plan sp
                    JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                    JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id
                    WHERE sp.subscription_plan_id = si.subscription_plan_id
                    AND al.level_id IN (
                        SELECT level_id FROM locations.levels WHERE reference_entity_id = ?::uuid
                    )
                )
                """;
            params.add(locationId.toString());
        }

        return jdbc.queryForMap(sql, params.toArray());
    }

    /**
     * Get DLQ summary.
     */
    public Map<String, Object> getDLQSummary(UUID locationId) {
        String sql = """
            SELECT 
                COUNT(CASE WHEN dlq.resolved = false THEN 1 END) AS unresolved_count,
                COUNT(CASE WHEN dlq.resolved = true AND DATE(dlq.resolved_on) = CURRENT_DATE THEN 1 END) AS resolved_today,
                COUNT(CASE WHEN dlq.resolved = false AND ft.failure_type_code = 'SOFT' THEN 1 END) AS soft_count,
                COUNT(CASE WHEN dlq.resolved = false AND ft.failure_type_code = 'HARD' THEN 1 END) AS hard_count,
                COUNT(CASE WHEN dlq.resolved = false AND ft.failure_type_code = 'TRANSIENT' THEN 1 END) AS transient_count
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            WHERE dlq.application_id = ?::uuid
            """;

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (locationId != null) {
            sql += " AND br.location_id = ?::uuid";
            params.add(locationId.toString());
        }

        return jdbc.queryForMap(sql, params.toArray());
    }

    /**
     * Get recent activity.
     */
    public List<Map<String, Object>> getRecentActivity(UUID locationId, int limit) {
        String sql = """
            SELECT 
                bal.event_type, bal.entity_type, bal.entity_id,
                br.billing_run_code, bal.user_id, bal.created_on
            FROM client_subscription_billing.billing_audit_log bal
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bal.entity_id::uuid
            WHERE br.application_id = ?::uuid
              AND bal.entity_type = 'BILLING_RUN'
            """;

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (locationId != null) {
            sql += " AND br.location_id = ?::uuid";
            params.add(locationId.toString());
        }

        sql += " ORDER BY bal.created_on DESC LIMIT ?";
        params.add(limit);

        return jdbc.queryForList(sql, params.toArray());
    }

    /**
     * Get trends data.
     */
    public List<Map<String, Object>> getTrends(String metric, LocalDate dateFrom, LocalDate dateTo, String groupBy, UUID locationId) {
        String dateFormat = "day".equals(groupBy) ? "YYYY-MM-DD" : "YYYY-MM";
        
        StringBuilder sql = new StringBuilder();
        if ("amount".equals(metric)) {
            sql.append("""
                SELECT 
                    TO_CHAR(br.created_on, ?) AS date,
                    COALESCE(SUM(sbh.invoice_total_amount), 0) AS value,
                    COUNT(DISTINCT sbh.invoice_id) AS count
                FROM client_subscription_billing.billing_run br
                LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
                WHERE br.created_on >= ? AND br.created_on <= ?
                """);
        } else {
            sql.append("""
                SELECT 
                    TO_CHAR(br.created_on, ?) AS date,
                    COUNT(DISTINCT br.billing_run_id) AS value,
                    COUNT(DISTINCT br.billing_run_id) AS count
                FROM client_subscription_billing.billing_run br
                WHERE br.created_on >= ? AND br.created_on <= ?
                """);
        }

        List<Object> params = new ArrayList<>();
        params.add(dateFormat);
        params.add(dateFrom.atStartOfDay().atOffset(java.time.ZoneOffset.UTC));
        params.add(dateTo.atTime(23, 59, 59).atOffset(java.time.ZoneOffset.UTC));
        sql.append(" AND br.application_id = ?::uuid");
        params.add(requireAppIdStr());

        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        sql.append(" GROUP BY TO_CHAR(br.created_on, ?) ORDER BY date");
        params.add(dateFormat);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public Map<String, Object> getOverviewSummary(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> pMain = new ArrayList<>();
        String whereMain =
                buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, pMain);
        List<Object> pDlq = new ArrayList<>();
        String whereDlq =
                buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, pDlq);
        String sql =
                "WITH latest_attempt AS ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh.*, "
                        + "      ROW_NUMBER() OVER (PARTITION BY COALESCE(CAST(sbh.invoice_id AS TEXT), CAST(sbh.subscription_instance_id AS TEXT)) "
                        + "      ORDER BY sbh.billing_attempt_on DESC NULLS LAST, sbh.created_on DESC NULLS LAST, sbh.subscription_billing_history_id DESC) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh "
                        + "  ) t WHERE rn = 1 "
                        + "), unresolved_dlq AS ( "
                        + "  SELECT COUNT(1) AS unresolved_count "
                        + "  FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "  JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + whereDlq.replace("WHERE ", "WHERE COALESCE(dlq.resolved, false) = false AND ")
                        + "), per_run AS ( "
                        + "  SELECT br.billing_run_id, UPPER(brs.status_code) AS status_code, br.summary_json, "
                        + "         COALESCE(COUNT(DISTINCT la.invoice_id) FILTER (WHERE la.invoice_id IS NOT NULL AND COALESCE(la.is_mock, false) = false), 0) AS la_invoice_count, "
                        + "         COALESCE(SUM(CASE WHEN COALESCE(la.is_mock, false) = false THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS la_total_amount, "
                        + "         COALESCE(SUM(CASE WHEN COALESCE(la.is_mock, false) = false AND bs.is_success = true THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS la_collected_amount, "
                        + "         COALESCE(SUM(CASE WHEN COALESCE(la.is_mock, false) = false AND bs.is_success IS NULL THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS la_in_flight_amount, "
                        + "         COALESCE(SUM(CASE WHEN COALESCE(la.is_mock, false) = false AND bs.is_failure = true THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS la_at_risk_amount, "
                        + "         COUNT(1) FILTER (WHERE COALESCE(la.is_mock, false) = false AND bs.is_failure = true) AS la_failure_count, "
                        + "         stg.stg_json, "
                        + "         due_prev.due_json "
                        + "  FROM client_subscription_billing.billing_run br "
                        + "  JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "  LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "  LEFT JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "  LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + "  LEFT JOIN LATERAL ( "
                        + "    SELECT bsr.summary_json AS stg_json "
                        + "    FROM client_subscription_billing.billing_stage_run bsr "
                        + "    JOIN billing_config.billing_stage_code bsc2 ON bsc2.billing_stage_code_id = bsr.stage_code_id "
                        + "    WHERE bsr.billing_run_id = br.billing_run_id "
                        + "      AND bsc.stage_code IS NOT NULL "
                        + "      AND bsc2.stage_code = bsc.stage_code "
                        + "    ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC "
                        + "    LIMIT 1 "
                        + "  ) stg ON true "
                        + "  LEFT JOIN LATERAL ( "
                        + "    SELECT bsr.summary_json AS due_json "
                        + "    FROM client_subscription_billing.billing_stage_run bsr "
                        + "    JOIN billing_config.billing_stage_code bsc3 ON bsc3.billing_stage_code_id = bsr.stage_code_id "
                        + "    WHERE bsr.billing_run_id = br.billing_run_id "
                        + "      AND bsc3.stage_code = 'DUE_PREVIEW' "
                        + "    ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC "
                        + "    LIMIT 1 "
                        + "  ) due_prev ON true "
                        + whereMain
                        + "  GROUP BY br.billing_run_id, brs.status_code, br.summary_json, stg.stg_json, due_prev.due_json "
                        + ") "
                        + "SELECT "
                        + "COUNT(1) AS total_runs, "
                        + "COALESCE(SUM(CASE "
                        + "  WHEN pr.la_invoice_count > 0 THEN pr.la_invoice_count "
                        + "  ELSE COALESCE("
                        + "    NULLIF(TRIM(pr.summary_json->>'invoices_count'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'invoices_count'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'invoicesCreated'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'total_instances'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'eligible_count'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'candidateRows'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'pendingCount'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'total_candidates'), '')::int, "
                        + "    NULLIF(TRIM(pr.stg_json->>'totalSelected'), '')::int, "
                        + "    NULLIF(TRIM(pr.due_json->>'total_instances'), '')::int, "
                        + "    NULLIF(TRIM(pr.due_json->>'eligible_count'), '')::int, "
                        + "    0) "
                        + "END), 0) AS invoice_count, "
                        + "COUNT(1) FILTER (WHERE pr.status_code LIKE 'COMPLETED%') AS completed_runs, "
                        + "COUNT(1) FILTER (WHERE pr.status_code IN ('RUNNING','INITIATED','IN_PROGRESS','SCHEDULED','WAITING','IDLE')) AS running_runs, "
                        + "COUNT(1) FILTER (WHERE pr.status_code LIKE 'FAILED%' OR pr.status_code = 'FAILED_SYSTEM') AS failed_runs, "
                        + "COALESCE(SUM(pr.la_total_amount), 0) AS total_amount, "
                        + "COALESCE(SUM(pr.la_collected_amount),0) AS collected_amount, "
                        + "COALESCE(SUM(pr.la_in_flight_amount),0) AS in_flight_amount, "
                        + "COALESCE(SUM(pr.la_at_risk_amount),0) AS at_risk_amount, "
                        + "(SELECT unresolved_count FROM unresolved_dlq) AS unresolved_dlq_count, "
                        + "COALESCE(SUM(pr.la_failure_count),0) AS failure_count "
                        + "FROM per_run pr";
        List<Object> p = new ArrayList<>(pDlq.size() + pMain.size());
        p.addAll(pDlq);
        p.addAll(pMain);
        return jdbc.queryForMap(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewRunHealth(
            LocalDate dueDateFrom, LocalDate dueDateTo, List<UUID> locationIds) {
        List<Object> p = new ArrayList<>();
        String whereBr2 = buildRunWhere("br2", dueDateFrom, dueDateTo, null, null, locationIds, null, null, p);
        String lateralWhere = whereBr2.replaceFirst("WHERE 1=1 ", "WHERE bsc.stage_code = stages.stage_code ");
        String sql =
                "SELECT stages.stage_code, br.billing_run_id, br.billing_run_code, brs.status_code, br.due_date, "
                        + "COALESCE("
                        + "  NULLIF(agg.distinct_invoices, 0), "
                        + "  NULLIF(TRIM(br.summary_json->>'invoices_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'invoices_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'invoicesCreated'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'total_instances'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'eligible_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'candidateRows'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'pendingCount'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'total_candidates'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'totalSelected'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'records_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'total_records'), '')::int, "
                        + "  NULLIF(TRIM(due_prev.due_json->>'total_instances'), '')::int, "
                        + "  NULLIF(TRIM(due_prev.due_json->>'eligible_count'), '')::int, "
                        + "  0) AS invoices_count, "
                        + "COALESCE("
                        + "  NULLIF(agg.failure_count, 0), "
                        + "  NULLIF(TRIM(br.summary_json->>'failure_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'failure_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'failureCount'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'failed_count'), '')::int, "
                        + "  0) AS failure_count, "
                        + "COALESCE("
                        + "  NULLIF(agg.success_count, 0), "
                        + "  NULLIF(TRIM(ls.stg_json->>'success_count'), '')::int, "
                        + "  NULLIF(TRIM(ls.stg_json->>'successCount'), '')::int, "
                        + "  0) AS success_count, "
                        + "COALESCE("
                        + "  NULLIF(agg.total_amount, 0), "
                        + "  NULLIF(TRIM(br.summary_json->>'total_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(br.summary_json->>'eligible_total_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'total_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'totalAmount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'eligible_total_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'eligible_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'pendingAmount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'selectedAmount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'charged_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(ls.stg_json->>'successAmount'), '')::numeric, "
                        + "  NULLIF(TRIM(due_prev.due_json->>'total_amount'), '')::numeric, "
                        + "  NULLIF(TRIM(due_prev.due_json->>'eligible_total_amount'), '')::numeric, "
                        + "  0) AS total_amount "
                        + "FROM unnest(ARRAY['DUE_PREVIEW','INVOICE_GENERATION','MOCK_CHARGE','ACTUAL_CHARGE']::text[]) AS stages(stage_code) "
                        + "JOIN LATERAL ( "
                        + "  SELECT bsr.billing_run_id, bsr.summary_json AS stg_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsr "
                        + "  JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "  JOIN client_subscription_billing.billing_run br2 ON br2.billing_run_id = bsr.billing_run_id "
                        + lateralWhere
                        + "  ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, "
                        + "           bsr.stage_run_id DESC "
                        + "  LIMIT 1 "
                        + ") ls ON true "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = ls.billing_run_id "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT bsr.summary_json AS due_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsr "
                        + "  JOIN billing_config.billing_stage_code bsc3 ON bsc3.billing_stage_code_id = bsr.stage_code_id "
                        + "  WHERE bsr.billing_run_id = br.billing_run_id "
                        + "    AND bsc3.stage_code = 'DUE_PREVIEW' "
                        + "  ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, "
                        + "           bsr.stage_run_id DESC "
                        + "  LIMIT 1 "
                        + ") due_prev ON true "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT COUNT(DISTINCT sbh.invoice_id) FILTER (WHERE sbh.invoice_id IS NOT NULL) AS distinct_invoices, "
                        + "         COALESCE(SUM(COALESCE(sbh.invoice_total_amount, 0)), 0) AS total_amount, "
                        + "         COUNT(*) FILTER (WHERE bs.is_success = true) AS success_count, "
                        + "         COUNT(*) FILTER (WHERE bs.is_failure = true) AS failure_count "
                        + "  FROM client_subscription_billing.subscription_billing_history sbh "
                        + "  LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "  WHERE sbh.billing_run_id = br.billing_run_id "
                        + ") agg ON true ";
        return jdbc.queryForList(sql, p.toArray());
    }

    public long getOverviewSchedulesDueCount(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder(
                "SELECT COUNT(DISTINCT sbs.billing_schedule_id) "
                        + "FROM client_subscription_billing.subscription_billing_schedule sbs "
                        + "JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id "
                        + "JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id "
                        + "WHERE sbs.application_id = ?::uuid "
                        + "AND sbs.billing_date >= ?::date AND sbs.billing_date <= ?::date "
                        + "AND sbs.invoice_id IS NULL "
                        + "AND bss.status_code IN "
                        + SCHEDULE_STATUS_OPEN_SQL
                        + " ");
        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        params.add(dueDateFrom);
        params.add(dueDateTo);
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql.append(
                    "AND EXISTS ( "
                            + "SELECT 1 FROM client_subscription_billing.subscription_plan sp "
                            + "JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id "
                            + "JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id "
                            + "WHERE sp.subscription_plan_id = si.subscription_plan_id "
                            + "AND al.level_id IN (SELECT level_id FROM locations.levels WHERE reference_entity_id IN (")
                    .append(in)
                    .append("))) ");
            for (UUID u : locationIds) {
                params.add(u.toString());
            }
        }
        Long n = jdbc.queryForObject(sql.toString(), params.toArray(), Long.class);
        return n == null ? 0L : n;
    }

    public Map<String, Object> getOverviewForecast(List<UUID> locationIds) {
        return getForecastDataForLocations(locationIds);
    }

    /**
     * Forecast buckets (7/30/90 days from today) scoped to any of the given physical locations
     * (agreement/role location tree), or global when {@code locationIds} is null or empty.
     */
    public Map<String, Object> getForecastDataForLocations(List<UUID> locationIds) {
        String sql =
                """
                SELECT
                    COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN sbs.billing_schedule_id END) AS due_7_days_count,
                    COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_7_days_amount,
                    COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN sbs.billing_schedule_id END) AS due_30_days_count,
                    COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_30_days_amount,
                    COUNT(DISTINCT CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN sbs.billing_schedule_id END) AS due_90_days_count,
                    COALESCE(SUM(CASE WHEN sbs.billing_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_90_days_amount
                FROM client_subscription_billing.subscription_billing_schedule sbs
                JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
                JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id
                WHERE sbs.application_id = ?::uuid
                  AND bss.status_code IN """
                    + SCHEDULE_STATUS_OPEN_SQL
                    + """
                """;

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql +=
                    """
                     AND EXISTS (
                        SELECT 1 FROM client_subscription_billing.subscription_plan sp
                        JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                        JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id
                        WHERE sp.subscription_plan_id = si.subscription_plan_id
                          AND al.level_id IN (
                              SELECT level_id FROM locations.levels WHERE reference_entity_id IN ("""
                            + in
                            + "))) ";
            for (UUID u : locationIds) {
                params.add(u.toString());
            }
        }

        return jdbc.queryForMap(sql, params.toArray());
    }

    public List<Map<String, Object>> getOverviewRunStarts7d(LocalDate dateFrom, LocalDate dateTo, List<UUID> locationIds) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dateFrom, dateTo, null, null, locationIds, null, null, p);
        String sql =
                "SELECT TO_CHAR(br.created_on::date, 'YYYY-MM-DD') AS date, COUNT(1) AS value "
                        + "FROM client_subscription_billing.billing_run br "
                        + where
                        + " GROUP BY br.created_on::date ORDER BY br.created_on::date";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewRealization7d(LocalDate dateFrom, LocalDate dateTo, List<UUID> locationIds) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dateFrom, dateTo, null, null, locationIds, null, null, p);
        String sql =
                "WITH latest_attempt AS ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh.*, ROW_NUMBER() OVER (PARTITION BY COALESCE(CAST(sbh.invoice_id AS TEXT), CAST(sbh.subscription_instance_id AS TEXT)) "
                        + "      ORDER BY sbh.billing_attempt_on DESC NULLS LAST, sbh.created_on DESC NULLS LAST, sbh.subscription_billing_history_id DESC) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh "
                        + "  ) t WHERE rn = 1 "
                        + ") "
                        + "SELECT TO_CHAR(COALESCE(la.billing_attempt_on::date, br.created_on::date), 'YYYY-MM-DD') AS date, "
                        + "COALESCE(SUM(CASE WHEN bs.is_success = true THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "LEFT JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + where
                        + " GROUP BY COALESCE(la.billing_attempt_on::date, br.created_on::date) "
                        + " ORDER BY COALESCE(la.billing_attempt_on::date, br.created_on::date)";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewStageDistribution(
            LocalDate dueDateFrom, LocalDate dueDateTo, List<UUID> locationIds, String status, String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, null, null, locationIds, status, currentStage, p);
        String sql =
                "SELECT COALESCE(bsc.stage_code, 'UNKNOWN') AS stage, COUNT(1) AS count "
                        + "FROM client_subscription_billing.billing_run br "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + where
                        + " GROUP BY COALESCE(bsc.stage_code, 'UNKNOWN') ORDER BY count DESC";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewTopRevenueLocations(
            LocalDate dueDateFrom, LocalDate dueDateTo, List<UUID> locationIds, int limit) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, null, null, locationIds, null, null, p);
        String sql =
                "WITH latest_attempt AS ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh.*, ROW_NUMBER() OVER (PARTITION BY COALESCE(CAST(sbh.invoice_id AS TEXT), CAST(sbh.subscription_instance_id AS TEXT)) "
                        + "      ORDER BY sbh.billing_attempt_on DESC NULLS LAST, sbh.created_on DESC NULLS LAST, sbh.subscription_billing_history_id DESC) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh "
                        + "  ) t WHERE rn = 1 "
                        + "), run_loc AS ( "
                        + "  SELECT br.billing_run_id, "
                        + "         COALESCE(loc.location_id::text, br.location_id::text, 'UNKNOWN') AS location_id, "
                        + "         COALESCE(loc.name, br_loc.name, 'Unknown') AS location_name, "
                        + "         COUNT(DISTINCT la.invoice_id) FILTER (WHERE la.invoice_id IS NOT NULL AND COALESCE(la.is_mock, false) = false) AS actual_invoice_count, "
                        + "         COALESCE(SUM(CASE WHEN COALESCE(la.is_mock, false) = false THEN COALESCE(la.invoice_total_amount,0) ELSE 0 END),0) AS actual_amount, "
                        + "         stg.stg_json, due_prev.due_json "
                        + "  FROM client_subscription_billing.billing_run br "
                        + "  JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "  LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "  LEFT JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "  LEFT JOIN transactions.invoice i ON i.invoice_id = la.invoice_id "
                        + "  LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = i.client_agreement_id "
                        + "  LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "  LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "  LEFT JOIN locations.location br_loc ON br_loc.location_id = br.location_id "
                        + "  LEFT JOIN LATERAL ( "
                        + "    SELECT bsr.summary_json AS stg_json "
                        + "    FROM client_subscription_billing.billing_stage_run bsr "
                        + "    JOIN billing_config.billing_stage_code bsc2 ON bsc2.billing_stage_code_id = bsr.stage_code_id "
                        + "    WHERE bsr.billing_run_id = br.billing_run_id "
                        + "      AND bsc.stage_code IS NOT NULL "
                        + "      AND bsc2.stage_code = bsc.stage_code "
                        + "    ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC "
                        + "    LIMIT 1 "
                        + "  ) stg ON true "
                        + "  LEFT JOIN LATERAL ( "
                        + "    SELECT bsr.summary_json AS due_json "
                        + "    FROM client_subscription_billing.billing_stage_run bsr "
                        + "    JOIN billing_config.billing_stage_code bsc3 ON bsc3.billing_stage_code_id = bsr.stage_code_id "
                        + "    WHERE bsr.billing_run_id = br.billing_run_id "
                        + "      AND bsc3.stage_code = 'DUE_PREVIEW' "
                        + "    ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC "
                        + "    LIMIT 1 "
                        + "  ) due_prev ON true "
                        + where
                        + "  GROUP BY br.billing_run_id, COALESCE(loc.location_id::text, br.location_id::text, 'UNKNOWN'), "
                        + "           COALESCE(loc.name, br_loc.name, 'Unknown'), stg.stg_json, due_prev.due_json "
                        + ") "
                        + "SELECT location_id, location_name, "
                        + "COALESCE(SUM(CASE "
                        + "  WHEN actual_amount > 0 THEN actual_amount "
                        + "  ELSE COALESCE("
                        + "    NULLIF(TRIM(stg_json->>'total_amount'), '')::numeric, "
                        + "    NULLIF(TRIM(stg_json->>'totalAmount'), '')::numeric, "
                        + "    NULLIF(TRIM(stg_json->>'eligible_total_amount'), '')::numeric, "
                        + "    NULLIF(TRIM(stg_json->>'eligible_amount'), '')::numeric, "
                        + "    NULLIF(TRIM(stg_json->>'pendingAmount'), '')::numeric, "
                        + "    NULLIF(TRIM(stg_json->>'selectedAmount'), '')::numeric, "
                        + "    NULLIF(TRIM(due_json->>'total_amount'), '')::numeric, "
                        + "    NULLIF(TRIM(due_json->>'eligible_total_amount'), '')::numeric, "
                        + "    0) "
                        + "END),0) AS amount, "
                        + "COALESCE(SUM(CASE "
                        + "  WHEN actual_invoice_count > 0 THEN actual_invoice_count "
                        + "  ELSE COALESCE("
                        + "    NULLIF(TRIM(stg_json->>'invoices_count'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'invoicesCreated'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'total_instances'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'eligible_count'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'candidateRows'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'pendingCount'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'total_candidates'), '')::int, "
                        + "    NULLIF(TRIM(stg_json->>'totalSelected'), '')::int, "
                        + "    NULLIF(TRIM(due_json->>'total_instances'), '')::int, "
                        + "    NULLIF(TRIM(due_json->>'eligible_count'), '')::int, "
                        + "    0) "
                        + "END),0) AS invoices, "
                        + "COUNT(DISTINCT billing_run_id) AS runs "
                        + "FROM run_loc "
                        + "GROUP BY location_id, location_name "
                        + " ORDER BY amount DESC NULLS LAST LIMIT ?";
        p.add(Math.max(1, limit));
        return jdbc.queryForList(sql, p.toArray());
    }

    public Map<String, Object> getOverviewContracts(List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COUNT(1) AS total_contracts, "
                        + "COUNT(1) FILTER (WHERE UPPER(COALESCE(lcas.name,'')) = 'ACTIVE') AS active_contracts, "
                        + "COUNT(1) FILTER (WHERE UPPER(COALESCE(lcas.name,'')) = 'EXPIRED') AS expired_contracts, "
                        + "0 AS expiring_in_30_days "
                        + "FROM client_agreements.client_agreement ca "
                        + "LEFT JOIN client_agreements.lu_client_agreement_status lcas ON lcas.client_agreement_status_id = ca.client_agreement_status_id "
                        + "WHERE EXISTS ( "
                        + "SELECT 1 FROM client_subscription_billing.subscription_plan sp "
                        + "WHERE sp.client_agreement_id = ca.client_agreement_id "
                        + "AND sp.application_id = ?::uuid) ";
        List<Object> p = new ArrayList<>();
        p.add(requireAppIdStr());
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql += "AND ca.client_role_id IN (SELECT cr.client_role_id FROM clients.client_role cr WHERE cr.location_id IN (" + in + ")) ";
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        return jdbc.queryForMap(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewFrequencyMix(List<UUID> locationIds) {
        // Keep stable structure; source model does not expose a canonical billing frequency column in current schema.
        return List.of();
    }

    private static final String LATEST_ATTEMPT_CTE =
            "WITH latest_attempt AS ( "
                    + "  SELECT * FROM ( "
                    + "    SELECT sbh.*, "
                    + "      ROW_NUMBER() OVER (PARTITION BY COALESCE(CAST(sbh.invoice_id AS TEXT), CAST(sbh.subscription_instance_id AS TEXT)) "
                    + "      ORDER BY sbh.billing_attempt_on DESC NULLS LAST, sbh.created_on DESC NULLS LAST, sbh.subscription_billing_history_id DESC) AS rn "
                    + "    FROM client_subscription_billing.subscription_billing_history sbh "
                    + "  ) t WHERE rn = 1 "
                    + ") ";

    /**
     * Daily billed vs collected for runs/invoices in scope (same filters as overview summary).
     */
    public List<Map<String, Object>> getOverviewBilledCollectedDaily(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                LATEST_ATTEMPT_CTE
                        + ", sbh_by_run AS ( "
                        + "  SELECT br.billing_run_id, br.due_date, br.summary_json, UPPER(brs.status_code) AS run_status, "
                        + "         COALESCE(SUM(la.invoice_total_amount), 0) AS from_la_billed, "
                        + "         COALESCE(SUM(CASE WHEN bs.is_success = true THEN la.invoice_total_amount ELSE 0 END), 0) AS from_la_collected "
                        + "  FROM client_subscription_billing.billing_run br "
                        + "  JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "  LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "  LEFT JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "  LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + where
                        + "  GROUP BY br.billing_run_id, br.due_date, br.summary_json, brs.status_code "
                        + ") "
                        + "SELECT TO_CHAR(due_date, 'YYYY-MM-DD') AS date, "
                        + "COALESCE(SUM(CASE "
                        + "  WHEN from_la_billed > 0 THEN from_la_billed "
                        + "  ELSE COALESCE(NULLIF(TRIM(summary_json->>'total_amount'), '')::numeric, "
                        + "                NULLIF(TRIM(summary_json->>'eligible_total_amount'), '')::numeric, 0) "
                        + "END), 0) AS billed, "
                        + "COALESCE(SUM(CASE "
                        + "  WHEN from_la_collected > 0 THEN from_la_collected "
                        + "  WHEN from_la_billed <= 0 AND run_status LIKE 'COMPLETED%' THEN "
                        + "       COALESCE(NULLIF(TRIM(summary_json->>'total_amount'), '')::numeric, "
                        + "                NULLIF(TRIM(summary_json->>'eligible_total_amount'), '')::numeric, 0) "
                        + "  ELSE 0 END), 0) AS collected "
                        + "FROM sbh_by_run "
                        + "GROUP BY due_date ORDER BY due_date";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewPaymentMethodSegments(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        String dim = BillingReportSql.SBH_TO_PAYMENT_DIMENSIONS.replace("sbh.", "la.");
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                LATEST_ATTEMPT_CTE
                        + "SELECT COALESCE(NULLIF(TRIM(pt.method_type_name), ''), NULLIF(TRIM(pgw.name), ''), 'Unknown') AS label, "
                        + "COALESCE(SUM(la.invoice_total_amount), 0) AS amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + dim
                        + " "
                        + where
                        + " AND COALESCE(la.is_mock, false) = false "
                        + " GROUP BY COALESCE(NULLIF(TRIM(pt.method_type_name), ''), NULLIF(TRIM(pgw.name), ''), 'Unknown') "
                        + "ORDER BY amount DESC NULLS LAST";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewGatewaySegments(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        String dim = BillingReportSql.SBH_TO_PAYMENT_DIMENSIONS.replace("sbh.", "la.");
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                LATEST_ATTEMPT_CTE
                        + "SELECT COALESCE(NULLIF(TRIM(pgw.name), ''), 'Unknown') AS label, "
                        + "COALESCE(SUM(la.invoice_total_amount), 0) AS amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + dim
                        + " "
                        + where
                        + " AND COALESCE(la.is_mock, false) = false "
                        + " GROUP BY COALESCE(NULLIF(TRIM(pgw.name), ''), 'Unknown') "
                        + "ORDER BY amount DESC NULLS LAST";
        return jdbc.queryForList(sql, p.toArray());
    }

    public Map<String, Object> getOverviewFunnelAggregate(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                "SELECT "
                        + "COALESCE(SUM(COALESCE(NULLIF(TRIM(due_prev.due_json->>'total_instances'), '')::int, NULLIF(TRIM(due_prev.due_json->>'eligible_count'), '')::int, 0)),0) AS billing_preview_count, "
                        + "COALESCE(SUM(COALESCE(NULLIF(TRIM(inv_gen.ig_json->>'invoices_count'), '')::int, NULLIF(TRIM(inv_gen.ig_json->>'invoicesCreated'), '')::int, NULLIF(TRIM(inv_gen.ig_json->>'candidateRows'), '')::int, NULLIF(TRIM(inv_gen.ig_json->>'totalSelected'), '')::int, 0)),0) AS invoice_generated_count, "
                        + "COALESCE(SUM(COALESCE(ac_agg.attempted_count, 0)),0) AS payment_attempted_count, "
                        + "COALESCE(SUM(COALESCE(ac_agg.success_count, 0)),0) AS payment_success_count "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT bsr.summary_json AS due_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsr "
                        + "  JOIN billing_config.billing_stage_code sc ON sc.billing_stage_code_id = bsr.stage_code_id "
                        + "  WHERE bsr.billing_run_id = br.billing_run_id AND sc.stage_code = 'DUE_PREVIEW' "
                        + "  ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC LIMIT 1 "
                        + ") due_prev ON true "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT bsr.summary_json AS ig_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsr "
                        + "  JOIN billing_config.billing_stage_code sc ON sc.billing_stage_code_id = bsr.stage_code_id "
                        + "  WHERE bsr.billing_run_id = br.billing_run_id AND sc.stage_code = 'INVOICE_GENERATION' "
                        + "  ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC LIMIT 1 "
                        + ") inv_gen ON true "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT COUNT(DISTINCT sbh.invoice_id) FILTER (WHERE sbh.invoice_id IS NOT NULL AND COALESCE(sbh.is_mock,false)=false) AS attempted_count, "
                        + "         COUNT(DISTINCT sbh.invoice_id) FILTER (WHERE sbh.invoice_id IS NOT NULL AND COALESCE(sbh.is_mock,false)=false AND bs.is_success = true) AS success_count "
                        + "  FROM client_subscription_billing.subscription_billing_history sbh "
                        + "  LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "  WHERE sbh.billing_run_id = br.billing_run_id "
                        + ") ac_agg ON true "
                        + where;
        return jdbc.queryForMap(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewInvoiceStatusSegments(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                LATEST_ATTEMPT_CTE
                        + "SELECT COALESCE(NULLIF(TRIM(invs.status_name), ''), 'Unknown') AS label, "
                        + "COALESCE(SUM(COALESCE(la.invoice_total_amount, 0)), 0) AS amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = la.invoice_id "
                        + "LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id "
                        + where
                        + " GROUP BY COALESCE(NULLIF(TRIM(invs.status_name), ''), 'Unknown') "
                        + "ORDER BY amount DESC NULLS LAST";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewArAgingSegments(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        // sbh has no payment_due_date; use schedule billing_date when billing_schedule_id is set, else attempt/run/invoice dates.
        String ageDays =
                "GREATEST(0, (CURRENT_DATE - COALESCE(sbs.billing_date::date, "
                        + "DATE(la.billing_attempt_on), br.due_date, DATE(i.created_on))))";
        String sql =
                LATEST_ATTEMPT_CTE
                        + "SELECT CASE "
                        + "  WHEN "
                        + ageDays
                        + " <= 30 THEN '0-30 days' "
                        + "  WHEN "
                        + ageDays
                        + " <= 60 THEN '31-60 days' "
                        + "  WHEN "
                        + ageDays
                        + " <= 90 THEN '61-90 days' "
                        + "  ELSE '90+ days' END AS label, "
                        + "COALESCE(SUM(COALESCE(i.total_amount, la.invoice_total_amount, 0)), 0) AS amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "JOIN latest_attempt la ON la.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN client_subscription_billing.subscription_billing_schedule sbs "
                        + "  ON sbs.billing_schedule_id = la.billing_schedule_id "
                        + "LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = la.billing_status_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = la.invoice_id "
                        + "LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id "
                        + where
                        + " AND (bs.is_success IS DISTINCT FROM true) "
                        + " AND (invs.status_name IS NULL OR UPPER(invs.status_name) NOT IN ('PAID', 'VOID', 'CANCELLED')) "
                        + " GROUP BY 1 ORDER BY 1";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewFunnelStages(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql =
                "SELECT COALESCE(NULLIF(TRIM(bsc.display_name), ''), COALESCE(bsc.stage_code, 'UNKNOWN')) AS name, "
                        + "COUNT(1)::double precision AS value "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + where
                        + " GROUP BY bsc.display_name, bsc.stage_code, bsc.stage_sequence "
                        + "ORDER BY COALESCE(bsc.stage_sequence, 999), name";
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> getOverviewFailureReasons(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        // buildRunWhere starts with "WHERE 1=1 " then "AND ..."; do not add another AND here.
        String dlqWhere = where.replaceFirst("WHERE 1=1 ", "WHERE COALESCE(dlq.resolved, false) = false ");
        String sql =
                "SELECT COALESCE(NULLIF(TRIM(dlq.error_type), ''), 'UNKNOWN') AS label, "
                        + "COALESCE(NULLIF(TRIM(dlq.error_type), ''), 'UNKNOWN') AS reason, "
                        + "COUNT(1) AS count, "
                        + "CAST(0 AS NUMERIC) AS amount "
                        + "FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + dlqWhere
                        + " GROUP BY COALESCE(NULLIF(TRIM(dlq.error_type), ''), 'UNKNOWN') "
                        + "ORDER BY count DESC NULLS LAST";
        return jdbc.queryForList(sql, p.toArray());
    }

    public long countOverviewRecentRuns(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        List<Object> p = new ArrayList<>();
        String where = buildRunWhere("br", dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage, p);
        String sql = "SELECT COUNT(1) FROM client_subscription_billing.billing_run br "
                + "LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                + where;
        Long n = jdbc.queryForObject(sql, p.toArray(), Long.class);
        return n == null ? 0L : n;
    }

    private String buildRunWhere(
            String runAlias,
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage,
            List<Object> params) {
        StringBuilder w = new StringBuilder("WHERE 1=1 AND ")
                .append(runAlias)
                .append(".application_id = ?::uuid ");
        params.add(requireAppIdStr());
        if (dueDateFrom != null) {
            w.append("AND ").append(runAlias).append(".due_date >= ?::date ");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            w.append("AND ").append(runAlias).append(".due_date <= ?::date ");
            params.add(dueDateTo);
        }
        if (asOfFrom != null) {
            w.append("AND DATE(").append(runAlias).append(".created_on) >= ?::date ");
            params.add(asOfFrom);
        }
        if (asOfTo != null) {
            w.append("AND DATE(").append(runAlias).append(".created_on) <= ?::date ");
            params.add(asOfTo);
        }
        if (status != null && !status.isBlank()) {
            w.append("AND COALESCE(brs.status_code,'') ILIKE ? ");
            params.add(status + "%");
        }
        if (currentStage != null && !currentStage.isBlank()) {
            w.append("AND COALESCE(bsc.stage_code,'') ILIKE ? ");
            params.add(currentStage + "%");
        }
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            w.append("AND (")
                    .append(runAlias).append(".location_id IN (").append(in).append(") ")
                    .append("OR EXISTS (SELECT 1 FROM client_subscription_billing.billing_run_location j ")
                    .append("WHERE j.billing_run_id = ").append(runAlias).append(".billing_run_id ")
                    .append("AND j.location_id IN (").append(in).append("))) ");
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    params.add(u.toString());
                }
            }
        }
        return w.toString();
    }

    private String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }
}
