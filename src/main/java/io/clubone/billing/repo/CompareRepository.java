package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for compare operations.
 */
@Repository
public class CompareRepository {

    private final JdbcTemplate jdbc;

    public CompareRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get billing run summary for comparison.
     */
    public Map<String, Object> getBillingRunSummary(UUID billingRunId) {
        String sql = """
            SELECT 
                br.billing_run_id, br.billing_run_code, br.due_date,
                COUNT(DISTINCT sbh.invoice_id) AS invoices_count,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS success_count,
                COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.invoice_id END) AS failure_count
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE br.billing_run_id = ?::uuid
            GROUP BY br.billing_run_id, br.billing_run_code, br.due_date
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, billingRunId.toString());
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Get invoice differences between two runs.
     */
    public List<Map<String, Object>> getInvoiceDifferences(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                COALESCE(a.subscription_instance_id, b.subscription_instance_id) AS subscription_instance_id,
                a.invoice_id AS invoice_id_a,
                b.invoice_id AS invoice_id_b,
                COALESCE(a.invoice_total_amount, 0) AS total_a,
                COALESCE(b.invoice_total_amount, 0) AS total_b,
                COALESCE(b.invoice_total_amount, 0) - COALESCE(a.invoice_total_amount, 0) AS delta_total,
                a_status.status_code AS status_a,
                b_status.status_code AS status_b,
                a.failure_reason AS failure_reason_a,
                b.failure_reason AS failure_reason_b
            FROM (
                SELECT sbh.subscription_instance_id, sbh.invoice_id, sbh.invoice_total_amount,
                       sbh.billing_status_id, sbh.failure_reason
                FROM client_subscription_billing.subscription_billing_history sbh
                WHERE sbh.billing_run_id = ?::uuid
            ) a
            FULL OUTER JOIN (
                SELECT sbh.subscription_instance_id, sbh.invoice_id, sbh.invoice_total_amount,
                       sbh.billing_status_id, sbh.failure_reason
                FROM client_subscription_billing.subscription_billing_history sbh
                WHERE sbh.billing_run_id = ?::uuid
            ) b ON a.subscription_instance_id = b.subscription_instance_id
            LEFT JOIN client_subscription_billing.lu_billing_status a_status ON a_status.billing_status_id = a.billing_status_id
            LEFT JOIN client_subscription_billing.lu_billing_status b_status ON b_status.billing_status_id = b.billing_status_id
            WHERE a.invoice_id IS DISTINCT FROM b.invoice_id
               OR ABS(COALESCE(a.invoice_total_amount, 0) - COALESCE(b.invoice_total_amount, 0)) > 0.01
               OR a_status.status_code IS DISTINCT FROM b_status.status_code
            """;

        return jdbc.queryForList(sql, runA.toString(), runB.toString());
    }

    /**
     * Get invoices only in run A.
     */
    public List<Map<String, Object>> getOnlyInRunA(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                a.subscription_instance_id,
                a.invoice_id,
                a.invoice_total_amount AS total_amount,
                bs.status_code AS status
            FROM client_subscription_billing.subscription_billing_history a
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = a.billing_status_id
            WHERE a.billing_run_id = ?::uuid
            AND NOT EXISTS (
                SELECT 1 FROM client_subscription_billing.subscription_billing_history b
                WHERE b.billing_run_id = ?::uuid
                AND b.subscription_instance_id = a.subscription_instance_id
            )
            """;

        return jdbc.queryForList(sql, runA.toString(), runB.toString());
    }

    /**
     * Get invoices only in run B.
     */
    public List<Map<String, Object>> getOnlyInRunB(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                b.subscription_instance_id,
                b.invoice_id,
                b.invoice_total_amount AS total_amount,
                bs.status_code AS status
            FROM client_subscription_billing.subscription_billing_history b
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = b.billing_status_id
            WHERE b.billing_run_id = ?::uuid
            AND NOT EXISTS (
                SELECT 1 FROM client_subscription_billing.subscription_billing_history a
                WHERE a.billing_run_id = ?::uuid
                AND a.subscription_instance_id = b.subscription_instance_id
            )
            """;

        return jdbc.queryForList(sql, runB.toString(), runA.toString());
    }

    /**
     * Find previous billing run for a given run.
     */
    public UUID findPreviousBillingRun(UUID billingRunId) {
        String sql = """
            SELECT br.billing_run_id
            FROM client_subscription_billing.billing_run br
            WHERE br.due_date < (
                SELECT due_date FROM client_subscription_billing.billing_run
                WHERE billing_run_id = ?::uuid
            )
            ORDER BY br.due_date DESC, br.created_on DESC
            LIMIT 1
            """;

        List<UUID> results = jdbc.query(sql, new Object[]{billingRunId.toString()}, 
                (rs, rowNum) -> (UUID) rs.getObject("billing_run_id"));
        
        return results.isEmpty() ? null : results.get(0);
    }
}
