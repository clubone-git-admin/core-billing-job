package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Repository for metrics operations.
 */
@Repository
public class MetricsRepository {

    private final JdbcTemplate jdbc;

    public MetricsRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get run metrics grouped by date.
     */
    public List<Map<String, Object>> getRunMetrics(LocalDate fromDate, LocalDate toDate, String groupBy, UUID locationId) {
        String dateFormat = "day".equals(groupBy) ? "YYYY-MM-DD" : "YYYY-MM";
        
        StringBuilder sql = new StringBuilder("""
            SELECT 
                TO_CHAR(br.created_on, ?) AS group_date,
                COUNT(DISTINCT br.billing_run_id) AS total_runs,
                COUNT(DISTINCT CASE WHEN brs.status_code IN ('COMPLETED_SUCCESS', 'COMPLETED_WITH_FAILURES') THEN br.billing_run_id END) AS successful_runs,
                COUNT(DISTINCT CASE WHEN brs.status_code IN ('FAILED_SYSTEM', 'CANCELLED') THEN br.billing_run_id END) AS failed_runs
            FROM client_subscription_billing.billing_run br
            JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            """);

        List<Object> params = new ArrayList<>();
        params.add(dateFormat);
        params.add(fromDate);
        params.add(toDate);

        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        sql.append(" GROUP BY TO_CHAR(br.created_on, ?) ORDER BY group_date");
        params.add(dateFormat);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Get stage metrics.
     */
    public List<Map<String, Object>> getStageMetrics(String stageCode, LocalDate fromDate, LocalDate toDate) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                bsc.stage_code,
                bsc.display_name AS stage_display_name,
                COUNT(DISTINCT bsr.stage_run_id) AS total_runs,
                COUNT(DISTINCT CASE WHEN srs.status_code = 'COMPLETED' THEN bsr.stage_run_id END) AS successful_runs,
                COUNT(DISTINCT CASE WHEN srs.status_code = 'FAILED' THEN bsr.stage_run_id END) AS failed_runs,
                AVG(EXTRACT(EPOCH FROM (bsr.ended_on - bsr.started_on))) AS avg_duration_seconds,
                MIN(EXTRACT(EPOCH FROM (bsr.ended_on - bsr.started_on))) AS min_duration_seconds,
                MAX(EXTRACT(EPOCH FROM (bsr.ended_on - bsr.started_on))) AS max_duration_seconds
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN client_subscription_billing.lu_stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            AND bsr.ended_on IS NOT NULL
            """);

        List<Object> params = new ArrayList<>();
        params.add(fromDate);
        params.add(toDate);

        if (stageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(stageCode);
        }

        sql.append(" GROUP BY bsc.stage_code, bsc.display_name");

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Get charge metrics grouped by failure type.
     */
    public Map<String, Object> getChargeMetricsByFailureType(LocalDate fromDate, LocalDate toDate) {
        String sql = """
            SELECT 
                ft.failure_type_code,
                COUNT(DISTINCT dlq.dlq_id) AS count,
                COALESCE(SUM((dlq.work_item_json->>'amount')::numeric), 0) AS amount
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            JOIN client_subscription_billing.lu_failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            GROUP BY ft.failure_type_code
            """;

        return Map.of("by_failure_type", jdbc.queryForList(sql, fromDate, toDate));
    }

    /**
     * Get charge metrics summary.
     */
    public Map<String, Object> getChargeMetricsSummary(LocalDate fromDate, LocalDate toDate) {
        String sql = """
            SELECT 
                COUNT(DISTINCT sbh.subscription_billing_history_id) AS total_charges,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.subscription_billing_history_id END) AS successful_charges,
                COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.subscription_billing_history_id END) AS failed_charges,
                COALESCE(SUM(CASE WHEN bs.is_success = true THEN sbh.invoice_total_amount END), 0) AS total_amount_charged,
                COALESCE(SUM(CASE WHEN bs.is_failure = true THEN sbh.invoice_total_amount END), 0) AS total_amount_failed
            FROM client_subscription_billing.subscription_billing_history sbh
            JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            """;

        return jdbc.queryForMap(sql, fromDate, toDate);
    }

    /**
     * Get charge metrics by error type.
     */
    public List<Map<String, Object>> getChargeMetricsByErrorType(LocalDate fromDate, LocalDate toDate) {
        String sql = """
            SELECT 
                dlq.error_type,
                COUNT(DISTINCT dlq.dlq_id) AS count,
                COALESCE(SUM((dlq.work_item_json->>'amount')::numeric), 0) AS amount
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            GROUP BY dlq.error_type
            ORDER BY count DESC
            """;

        return jdbc.queryForList(sql, fromDate, toDate);
    }

    /**
     * Get executive summary.
     */
    public Map<String, Object> getExecutiveSummary(LocalDate fromDate, LocalDate toDate, UUID locationId) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                COUNT(DISTINCT br.billing_run_id) AS total_runs,
                COUNT(DISTINCT sbh.invoice_id) AS total_invoices,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS successful_invoices,
                COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.invoice_id END) AS failed_invoices,
                AVG(EXTRACT(EPOCH FROM (br.ended_on - br.started_on)) / 3600.0) AS avg_processing_time_hours,
                MIN(EXTRACT(EPOCH FROM (br.ended_on - br.started_on)) / 60.0) AS fastest_run_minutes,
                MAX(EXTRACT(EPOCH FROM (br.ended_on - br.started_on)) / 60.0) AS slowest_run_minutes,
                AVG(EXTRACT(EPOCH FROM (br.ended_on - br.started_on)) / 60.0) AS avg_run_minutes
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE DATE(br.created_on) >= ? AND DATE(br.created_on) <= ?
            AND br.ended_on IS NOT NULL
            """);

        List<Object> params = new ArrayList<>();
        params.add(fromDate);
        params.add(toDate);

        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        return jdbc.queryForMap(sql.toString(), params.toArray());
    }
}
