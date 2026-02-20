package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for dashboard operations.
 */
@Repository
public class DashboardRepository {

    private final JdbcTemplate jdbc;

    public DashboardRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
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
            LEFT JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE br.created_on >= ?
            """);

        List<Object> params = new ArrayList<>();
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
                brs.status_code, bsc.stage_code AS current_stage_code,
                br.started_on, br.ended_on,
                COUNT(DISTINCT sbh.invoice_id) AS invoices_count,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount
            FROM client_subscription_billing.billing_run br
            JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            WHERE br.created_on >= ?
            """;

        List<Object> params = new ArrayList<>();
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
                COUNT(DISTINCT CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN sis.subscription_invoice_schedule_id END) AS due_7_days_count,
                COALESCE(SUM(CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN i.total_amount END), 0) AS due_7_days_amount,
                COUNT(DISTINCT CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN sis.subscription_invoice_schedule_id END) AS due_30_days_count,
                COALESCE(SUM(CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days' THEN i.total_amount END), 0) AS due_30_days_amount,
                COUNT(DISTINCT CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN sis.subscription_invoice_schedule_id END) AS due_90_days_count,
                COALESCE(SUM(CASE WHEN sis.payment_due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days' THEN i.total_amount END), 0) AS due_90_days_amount
            FROM client_subscription_billing.subscription_invoice_schedule sis
            JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sis.subscription_instance_id
            WHERE sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
            """;

        List<Object> params = new ArrayList<>();
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
            LEFT JOIN client_subscription_billing.lu_failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            WHERE 1=1
            """;

        List<Object> params = new ArrayList<>();
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
            WHERE bal.entity_type = 'BILLING_RUN'
            """;

        List<Object> params = new ArrayList<>();
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

        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        sql.append(" GROUP BY TO_CHAR(br.created_on, ?) ORDER BY date");
        params.add(dateFormat);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }
}
