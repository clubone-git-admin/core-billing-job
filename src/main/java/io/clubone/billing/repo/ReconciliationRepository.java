package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for reconciliation operations.
 */
@Repository
public class ReconciliationRepository {

    private final JdbcTemplate jdbc;

    public ReconciliationRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get reconciliation details for an invoice.
     */
    public Map<String, Object> getInvoiceReconciliation(UUID invoiceId) {
        String sql = """
            SELECT 
                i.invoice_id, i.invoice_number, i.total_amount AS invoice_total,
                lis.status_name AS invoice_status, i.created_on AS invoice_created_on,
                sbh.subscription_billing_history_id,
                sbh.billing_run_id, br.billing_run_code,
                sbh.stage_run_id, bsr.stage_run_code,
                bs.status_code AS billing_status_code, bs.display_name AS billing_status_display,
                sbh.billing_attempt_on,
                sbh.invoice_total_amount,
                cpi.client_payment_intent_id, cpi.amount AS intent_amount,
                cpis.payment_intent_status AS intent_status, cpi.created_on AS intent_created_on,
                cpt.client_payment_transaction_id, cpt.amount AS transaction_amount,
                cpt.payment_gateway_order_id AS gateway_order_id,
                pgts.payment_gateway_transaction_status AS transaction_status,
                cpt.created_on AS transaction_created_on
            FROM transactions.invoice i
            LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.invoice_id = i.invoice_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id
            LEFT JOIN client_subscription_billing.billing_stage_run bsr ON bsr.stage_run_id = sbh.stage_run_id
            LEFT JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
            LEFT JOIN client_payments.lu_payment_intent_status cpis ON cpis.payment_intent_status_id = cpi.intent_status_id
            LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
            LEFT JOIN payment_gateway.lu_payment_gateway_transaction_status pgts ON pgts.payment_gateway_transaction_status_id = cpt.payment_gateway_transaction_status_id
            WHERE i.invoice_id = ?::uuid
            ORDER BY sbh.billing_attempt_on DESC
            LIMIT 1
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, invoiceId.toString());
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Get reconciliation summary for a billing run.
     */
    public Map<String, Object> getBillingRunReconciliation(UUID billingRunId) {
        String sql = """
            SELECT 
                COUNT(DISTINCT sbh.invoice_id) AS total_invoices,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS invoiced_amount,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS reconciled_count,
                COUNT(DISTINCT CASE WHEN bs.is_success = false THEN sbh.invoice_id END) AS unreconciled_count,
                COALESCE(SUM(CASE WHEN bs.is_success = true THEN sbh.invoice_total_amount END), 0) AS collected_amount,
                COALESCE(SUM(CASE WHEN bs.is_success = false THEN sbh.invoice_total_amount END), 0) AS failed_amount
            FROM client_subscription_billing.subscription_billing_history sbh
            JOIN client_subscription_billing.lu_billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE sbh.billing_run_id = ?::uuid
            """;

        return jdbc.queryForMap(sql, billingRunId.toString());
    }

    /**
     * Get mismatches for a billing run.
     */
    public List<Map<String, Object>> getMismatches(UUID billingRunId) {
        String sql = """
            SELECT 
                sbh.invoice_id,
                i.invoice_number,
                sbh.invoice_total_amount AS invoice_amount,
                cpt.amount AS transaction_amount,
                CASE 
                    WHEN sbh.invoice_total_amount != cpt.amount THEN 'AMOUNT_MISMATCH'
                    WHEN cpt.client_payment_transaction_id IS NULL THEN 'MISSING_TRANSACTION'
                    ELSE 'NONE'
                END AS mismatch_type,
                CASE 
                    WHEN ABS(sbh.invoice_total_amount - COALESCE(cpt.amount, 0)) > 0.01 THEN 'HIGH'
                    WHEN cpt.client_payment_transaction_id IS NULL THEN 'MEDIUM'
                    ELSE 'NONE'
                END AS severity
            FROM client_subscription_billing.subscription_billing_history sbh
            JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id
            LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
            LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
            WHERE sbh.billing_run_id = ?::uuid
            AND (
                ABS(sbh.invoice_total_amount - COALESCE(cpt.amount, 0)) > 0.01
                OR cpt.client_payment_transaction_id IS NULL
            )
            """;

        return jdbc.queryForList(sql, billingRunId.toString());
    }

    /**
     * Update reconciliation status for an invoice.
     */
    public void reconcileInvoice(UUID invoiceId, UUID reconciledBy, String notes) {
        // This would typically update a reconciliation status table
        // For now, we'll just log it in audit log
        // In production, you might have a reconciliation_status column or table
        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_audit_log
            (event_type, entity_type, entity_id, action, user_id, details, created_on)
            VALUES ('INVOICE_RECONCILED', 'INVOICE', ?::uuid, 'RECONCILE', ?, ?::jsonb, now())
            """,
                invoiceId.toString(), reconciledBy != null ? reconciledBy.toString() : null,
                notes != null ? "{\"notes\":\"" + notes + "\"}" : "{}");
    }
}
