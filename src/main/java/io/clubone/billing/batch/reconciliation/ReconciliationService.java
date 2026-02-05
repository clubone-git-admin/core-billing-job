package io.clubone.billing.batch.reconciliation;

import io.clubone.billing.batch.audit.AuditService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Comprehensive reconciliation service for billing operations.
 * Provides reconciliation reports, variance detection, financial reporting, and audit trail.
 * All reconciliation activities are audited for compliance and troubleshooting.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final JdbcTemplate jdbc;
    private final AuditService auditService;

    public ReconciliationService(JdbcTemplate jdbc, AuditService auditService) {
        this.jdbc = jdbc;
        this.auditService = auditService;
    }

    /**
     * Generate daily reconciliation report with audit logging.
     * 
     * @param date Date for reconciliation report
     * @param userId User who requested the report
     * @return Reconciliation report with summary by status, totals, and failed invoices
     */
    public Map<String, Object> generateDailyReconciliation(LocalDate date, String userId) {
        UUID reconciliationId = UUID.randomUUID();
        String period = "DAILY:" + date.toString();
        
        // Audit: Reconciliation started
        auditService.logReconciliationStarted(reconciliationId, period, userId);
        
        try {
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("reconciliationId", reconciliationId.toString());
            report.put("date", date.toString());
            report.put("generatedAt", LocalDateTime.now().toString());
            report.put("type", "DAILY");

            // Summary by status
            List<Map<String, Object>> byStatus = jdbc.queryForList("""
                SELECT s.status_code, COUNT(1) AS count, 
                       COALESCE(SUM(h.invoice_total_amount), 0) AS total_amount
                FROM client_subscription_billing.subscription_billing_history h
                JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id
                WHERE DATE(h.billing_attempt_on) = ?::date
                GROUP BY s.status_code
                ORDER BY s.status_code
                """,
                date.toString()
            );
            report.put("summaryByStatus", byStatus);

            // Total amounts
            List<Map<String, Object>> totalsList = jdbc.queryForList("""
                SELECT 
                    COUNT(1) AS total_invoices,
                    COALESCE(SUM(invoice_total_amount), 0) AS total_amount,
                    COALESCE(SUM(CASE WHEN is_mock = false THEN invoice_total_amount ELSE 0 END), 0) AS live_amount,
                    COALESCE(SUM(CASE WHEN is_mock = true THEN invoice_total_amount ELSE 0 END), 0) AS mock_amount
                FROM client_subscription_billing.subscription_billing_history
                WHERE DATE(billing_attempt_on) = ?::date
                """,
                date.toString()
            );
            Map<String, Object> totals = totalsList.isEmpty() ? new LinkedHashMap<>() : totalsList.get(0);
            report.put("totals", totals);

            // Failed invoices
            List<Map<String, Object>> failed = jdbc.queryForList("""
                SELECT h.invoice_id, h.failure_reason, s.status_code, h.invoice_total_amount
                FROM client_subscription_billing.subscription_billing_history h
                JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id
                WHERE DATE(h.billing_attempt_on) = ?::date
                  AND s.status_code IN ('LIVE_PAYMENT_FAILED', 'LIVE_ERROR', 'MOCK_ERROR')
                ORDER BY h.billing_attempt_on DESC
                LIMIT 100
                """,
                date.toString()
            );
            report.put("failedInvoices", failed);
            
            // Calculate variance metrics
            int failedCount = failed.size();
            int totalInvoices = totals.get("total_invoices") != null ? 
                ((Number) totals.get("total_invoices")).intValue() : 0;
            double successRate = totalInvoices > 0 ? 
                ((double)(totalInvoices - failedCount) / totalInvoices) * 100 : 0;
            
            report.put("metrics", Map.of(
                "totalInvoices", totalInvoices,
                "failedCount", failedCount,
                "successRate", String.format("%.2f%%", successRate)
            ));

            log.info("Generated daily reconciliation report: reconciliationId={} date={} userId={}", 
                reconciliationId, date, userId);
            
            // Audit: Reconciliation completed
            Map<String, Object> auditDetails = new HashMap<>();
            auditDetails.put("date", date.toString());
            auditDetails.put("totalInvoices", totalInvoices);
            auditDetails.put("failedCount", failedCount);
            auditDetails.put("successRate", successRate);
            auditService.logReconciliationCompleted(reconciliationId, auditDetails, userId);
            
            return report;
        } catch (Exception e) {
            log.error("Failed to generate daily reconciliation report: date={} userId={}", date, userId, e);
            // Audit: Reconciliation failed
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("date", date.toString());
            errorDetails.put("error", e.getMessage());
            auditService.logReconciliationCompleted(reconciliationId, errorDetails, userId);
            throw e;
        }
    }
    
    /**
     * Generate daily reconciliation report (backward compatibility - uses "system" as userId).
     */
    public Map<String, Object> generateDailyReconciliation(LocalDate date) {
        return generateDailyReconciliation(date, "system");
    }

    /**
     * Detect variances between expected and actual billing with audit logging.
     * 
     * @param startDate Start date for variance detection
     * @param endDate End date for variance detection
     * @param userId User who requested variance detection
     * @return Variance report comparing scheduled vs actual billing
     */
    public Map<String, Object> detectVariances(LocalDate startDate, LocalDate endDate, String userId) {
        UUID reconciliationId = UUID.randomUUID();
        String period = "VARIANCES:" + startDate.toString() + " to " + endDate.toString();
        
        // Audit: Reconciliation started
        auditService.logReconciliationStarted(reconciliationId, period, userId);
        
        try {
            Map<String, Object> variances = new LinkedHashMap<>();
            variances.put("reconciliationId", reconciliationId.toString());
            variances.put("period", Map.of("start", startDate.toString(), "end", endDate.toString()));
            variances.put("generatedAt", LocalDateTime.now().toString());
            variances.put("type", "VARIANCES");

            // Compare scheduled vs actual
            List<Map<String, Object>> comparison = jdbc.queryForList("""
                SELECT 
                    DATE(sis.payment_due_date) AS due_date,
                    COUNT(DISTINCT sis.invoice_id) AS scheduled_count,
                    COUNT(DISTINCT h.invoice_id) AS billed_count,
                    COUNT(DISTINCT sis.invoice_id) - COUNT(DISTINCT h.invoice_id) AS variance_count
                FROM client_subscription_billing.subscription_invoice_schedule sis
                LEFT JOIN client_subscription_billing.subscription_billing_history h 
                    ON h.invoice_id = sis.invoice_id 
                    AND DATE(h.billing_attempt_on) BETWEEN ?::date AND ?::date
                WHERE sis.payment_due_date BETWEEN ?::date AND ?::date
                GROUP BY DATE(sis.payment_due_date)
                HAVING COUNT(DISTINCT sis.invoice_id) != COUNT(DISTINCT h.invoice_id)
                ORDER BY due_date
                """,
                startDate.toString(), endDate.toString(), startDate.toString(), endDate.toString()
            );
            variances.put("variances", comparison);
            
            // Calculate summary metrics
            int totalVariances = comparison.size();
            int totalVarianceCount = comparison.stream()
                .mapToInt(v -> ((Number) v.getOrDefault("variance_count", 0)).intValue())
                .sum();
            
            variances.put("summary", Map.of(
                "totalVarianceDays", totalVariances,
                "totalVarianceCount", totalVarianceCount,
                "hasVariances", totalVariances > 0
            ));

            log.info("Detected variances: reconciliationId={} period={} to {} userId={} varianceDays={}", 
                reconciliationId, startDate, endDate, userId, totalVariances);
            
            // Audit: Reconciliation completed
            Map<String, Object> auditDetails = new HashMap<>();
            auditDetails.put("startDate", startDate.toString());
            auditDetails.put("endDate", endDate.toString());
            auditDetails.put("totalVarianceDays", totalVariances);
            auditDetails.put("totalVarianceCount", totalVarianceCount);
            auditService.logReconciliationCompleted(reconciliationId, auditDetails, userId);
            
            return variances;
        } catch (Exception e) {
            log.error("Failed to detect variances: startDate={} endDate={} userId={}", 
                startDate, endDate, userId, e);
            // Audit: Reconciliation failed
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("startDate", startDate.toString());
            errorDetails.put("endDate", endDate.toString());
            errorDetails.put("error", e.getMessage());
            auditService.logReconciliationCompleted(reconciliationId, errorDetails, userId);
            throw e;
        }
    }
    
    /**
     * Detect variances (backward compatibility - uses "system" as userId).
     */
    public Map<String, Object> detectVariances(LocalDate startDate, LocalDate endDate) {
        return detectVariances(startDate, endDate, "system");
    }

    /**
     * Get financial summary for a date range with audit logging.
     * 
     * @param startDate Start date for financial summary
     * @param endDate End date for financial summary
     * @param userId User who requested the financial summary
     * @return Financial summary with revenue, transactions, taxes, discounts
     */
    public Map<String, Object> getFinancialSummary(LocalDate startDate, LocalDate endDate, String userId) {
        UUID reconciliationId = UUID.randomUUID();
        String period = "FINANCIAL:" + startDate.toString() + " to " + endDate.toString();
        
        // Audit: Reconciliation started
        auditService.logReconciliationStarted(reconciliationId, period, userId);
        
        try {
            List<Map<String, Object>> results = jdbc.queryForList("""
                SELECT 
                    COUNT(1) AS total_transactions,
                    COALESCE(SUM(invoice_total_amount), 0) AS total_revenue,
                    COALESCE(SUM(invoice_sub_total), 0) AS total_subtotal,
                    COALESCE(SUM(invoice_tax_amount), 0) AS total_tax,
                    COALESCE(SUM(invoice_discount_amount), 0) AS total_discount,
                    COUNT(CASE WHEN is_mock = false THEN 1 END) AS live_transactions,
                    COUNT(CASE WHEN is_mock = true THEN 1 END) AS mock_transactions
                FROM client_subscription_billing.subscription_billing_history
                WHERE DATE(billing_attempt_on) BETWEEN ?::date AND ?::date
                """,
                startDate.toString(), endDate.toString()
            );
            
            Map<String, Object> summary = results.isEmpty() ? new LinkedHashMap<>() : results.get(0);
            summary.put("reconciliationId", reconciliationId.toString());
            summary.put("period", Map.of("start", startDate.toString(), "end", endDate.toString()));
            summary.put("generatedAt", LocalDateTime.now().toString());
            summary.put("type", "FINANCIAL");
            
            log.info("Generated financial summary: reconciliationId={} period={} to {} userId={}", 
                reconciliationId, startDate, endDate, userId);
            
            // Audit: Reconciliation completed
            Map<String, Object> auditDetails = new HashMap<>(summary);
            auditDetails.remove("reconciliationId"); // Don't duplicate in audit
            auditService.logReconciliationCompleted(reconciliationId, auditDetails, userId);
            
            return summary;
        } catch (Exception e) {
            log.error("Failed to generate financial summary: startDate={} endDate={} userId={}", 
                startDate, endDate, userId, e);
            // Audit: Reconciliation failed
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("startDate", startDate.toString());
            errorDetails.put("endDate", endDate.toString());
            errorDetails.put("error", e.getMessage());
            auditService.logReconciliationCompleted(reconciliationId, errorDetails, userId);
            throw e;
        }
    }
    
    /**
     * Get financial summary (backward compatibility - uses "system" as userId).
     */
    public Map<String, Object> getFinancialSummary(LocalDate startDate, LocalDate endDate) {
        return getFinancialSummary(startDate, endDate, "system");
    }
    
    /**
     * Generate comprehensive reconciliation report for a billing run.
     * 
     * @param billingRunId Billing run ID to reconcile
     * @param userId User who requested the reconciliation
     * @return Comprehensive reconciliation report for the billing run
     */
    public Map<String, Object> reconcileBillingRun(UUID billingRunId, String userId) {
        UUID reconciliationId = UUID.randomUUID();
        String period = "BILLING_RUN:" + billingRunId.toString();
        
        // Audit: Reconciliation started
        auditService.logReconciliationStarted(reconciliationId, period, userId);
        
        try {
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("reconciliationId", reconciliationId.toString());
            report.put("billingRunId", billingRunId.toString());
            report.put("generatedAt", LocalDateTime.now().toString());
            report.put("type", "BILLING_RUN");

            // Get billing run details
            List<Map<String, Object>> runDetails = jdbc.queryForList("""
                SELECT billing_run_id, run_mode, as_of_date, started_on, ended_on, status, summary_json
                FROM client_subscription_billing.billing_run
                WHERE billing_run_id = ?::uuid
                """,
                billingRunId.toString()
            );
            report.put("billingRun", runDetails.isEmpty() ? Map.of() : runDetails.get(0));

            // Summary by status for this run
            List<Map<String, Object>> byStatus = jdbc.queryForList("""
                SELECT s.status_code, COUNT(1) AS count, 
                       COALESCE(SUM(h.invoice_total_amount), 0) AS total_amount
                FROM client_subscription_billing.subscription_billing_history h
                JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id
                WHERE h.billing_run_id = ?::uuid
                GROUP BY s.status_code
                ORDER BY s.status_code
                """,
                billingRunId.toString()
            );
            report.put("summaryByStatus", byStatus);

            // Failed invoices for this run
            List<Map<String, Object>> failed = jdbc.queryForList("""
                SELECT h.invoice_id, h.failure_reason, s.status_code, h.invoice_total_amount
                FROM client_subscription_billing.subscription_billing_history h
                JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id
                WHERE h.billing_run_id = ?::uuid
                  AND s.status_code IN ('LIVE_PAYMENT_FAILED', 'LIVE_ERROR', 'MOCK_ERROR')
                ORDER BY h.billing_attempt_on DESC
                """,
                billingRunId.toString()
            );
            report.put("failedInvoices", failed);

            // DLQ entries for this run
            List<Map<String, Object>> dlqEntries = jdbc.queryForList("""
                SELECT dlq_id, invoice_id, error_type, error_message, created_on, resolved
                FROM client_subscription_billing.billing_dead_letter_queue
                WHERE billing_run_id = ?::uuid
                ORDER BY created_on DESC
                """,
                billingRunId.toString()
            );
            report.put("dlqEntries", dlqEntries);

            log.info("Generated billing run reconciliation: reconciliationId={} billingRunId={} userId={}", 
                reconciliationId, billingRunId, userId);
            
            // Audit: Reconciliation completed
            Map<String, Object> auditDetails = new HashMap<>();
            auditDetails.put("billingRunId", billingRunId.toString());
            auditDetails.put("statusCount", byStatus.size());
            auditDetails.put("failedCount", failed.size());
            auditDetails.put("dlqCount", dlqEntries.size());
            auditService.logReconciliationCompleted(reconciliationId, auditDetails, userId);
            
            return report;
        } catch (Exception e) {
            log.error("Failed to reconcile billing run: billingRunId={} userId={}", billingRunId, userId, e);
            // Audit: Reconciliation failed
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("billingRunId", billingRunId.toString());
            errorDetails.put("error", e.getMessage());
            auditService.logReconciliationCompleted(reconciliationId, errorDetails, userId);
            throw e;
        }
    }
}
