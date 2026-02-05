package io.clubone.billing.batch.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Comprehensive audit service for tracking all billing operations.
 * Provides complete audit trail for compliance, troubleshooting, and regulatory requirements.
 * 
 * Event Types:
 * - JOB_EXECUTION: Billing job lifecycle events
 * - INVOICE_PROCESSING: Invoice processing events
 * - PAYMENT: Payment operations
 * - DLQ: Dead Letter Queue operations
 * - RECONCILIATION: Reconciliation activities
 */
@Service
public class AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public AuditService(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Log an audit event.
     * Non-blocking: failures are logged but don't throw exceptions.
     */
    public void logEvent(String eventType, String entityType, UUID entityId, 
                        String action, String userId, String details) {
        try {
            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_audit_log
                (event_type, entity_type, entity_id, action, user_id, details, created_on)
                VALUES (?, ?, ?::uuid, ?, ?, ?::jsonb, ?)
                """,
                eventType,
                entityType,
                entityId != null ? entityId.toString() : null,
                action,
                userId,
                details,
                LocalDateTime.now()
            );
        } catch (DataAccessException e) {
            // Log but don't throw - audit logging failures shouldn't block job execution
            // Common causes: table doesn't exist, permission denied, or table not migrated
            log.warn("Failed to log audit event (non-blocking): eventType={} entityId={} error={}. " +
                    "To enable audit logging, run database migrations and grant INSERT permissions on billing_audit_log table.", 
                eventType, entityId, e.getMessage());
            log.debug("Audit logging failure details", e);
        } catch (Exception e) {
            log.warn("Unexpected error logging audit event (non-blocking): eventType={} entityId={} error={}", 
                eventType, entityId, e.getMessage());
            log.debug("Audit logging failure details", e);
        }
    }

    /**
     * Log job execution audit.
     */
    public void logJobExecution(UUID billingRunId, String action, String userId, Map<String, Object> details) {
        String detailsJson = null;
        if (details != null) {
            try {
                detailsJson = objectMapper.writeValueAsString(details);
            } catch (Exception e) {
                log.warn("Failed to serialize audit details to JSON: billingRunId={}", billingRunId, e);
            }
        }
        logEvent("JOB_EXECUTION", "BILLING_RUN", billingRunId, action, userId, detailsJson);
    }

    /**
     * Log invoice processing audit.
     */
    public void logInvoiceProcessing(UUID invoiceId, String action, String userId, Map<String, Object> details) {
        String detailsJson = serializeDetails(details);
        logEvent("INVOICE_PROCESSING", "INVOICE", invoiceId, action, userId, detailsJson);
    }

    /**
     * Log payment operation audit.
     */
    public void logPayment(UUID invoiceId, UUID paymentIntentId, String action, String userId, Map<String, Object> details) {
        String detailsJson = serializeDetails(details);
        // Use paymentIntentId as entity_id if available, otherwise invoiceId
        UUID entityId = paymentIntentId != null ? paymentIntentId : invoiceId;
        logEvent("PAYMENT", "PAYMENT", entityId, action, userId, detailsJson);
    }

    /**
     * Log Dead Letter Queue operation audit.
     */
    public void logDLQ(UUID dlqId, UUID invoiceId, String action, String userId, Map<String, Object> details) {
        String detailsJson = serializeDetails(details);
        // Use dlqId as entity_id, invoiceId in details
        logEvent("DLQ", "DLQ", dlqId, action, userId, detailsJson);
    }

    /**
     * Log reconciliation activity audit.
     */
    public void logReconciliation(UUID reconciliationId, String action, String userId, Map<String, Object> details) {
        String detailsJson = serializeDetails(details);
        logEvent("RECONCILIATION", "RECONCILIATION", reconciliationId, action, userId, detailsJson);
    }

    /**
     * Helper method to serialize details to JSON.
     */
    private String serializeDetails(Map<String, Object> details) {
        if (details == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(details);
        } catch (Exception e) {
            log.warn("Failed to serialize audit details to JSON", e);
            return null;
        }
    }

    // ========================================================================
    // Convenience methods for common audit scenarios
    // ========================================================================

    /**
     * Log job execution started.
     */
    public void logJobStarted(UUID billingRunId, String runMode, String asOfDate, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("runMode", runMode);
        details.put("asOfDate", asOfDate);
        logJobExecution(billingRunId, "STARTED", userId, details);
    }

    /**
     * Log job execution completed.
     */
    public void logJobCompleted(UUID billingRunId, Map<String, Object> summary, String userId) {
        logJobExecution(billingRunId, "COMPLETED", userId, summary);
    }

    /**
     * Log job execution failed.
     */
    public void logJobFailed(UUID billingRunId, String error, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("error", error);
        logJobExecution(billingRunId, "FAILED", userId, details);
    }

    /**
     * Log invoice processing started.
     */
    public void logInvoiceProcessingStarted(UUID invoiceId, UUID billingRunId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("billingRunId", billingRunId != null ? billingRunId.toString() : null);
        logInvoiceProcessing(invoiceId, "PROCESSING_STARTED", userId, details);
    }

    /**
     * Log invoice processing success.
     */
    public void logInvoiceProcessingSuccess(UUID invoiceId, String status, UUID billingRunId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("status", status);
        details.put("billingRunId", billingRunId != null ? billingRunId.toString() : null);
        logInvoiceProcessing(invoiceId, "PROCESSED_SUCCESS", userId, details);
    }

    /**
     * Log invoice processing failure.
     */
    public void logInvoiceProcessingFailure(UUID invoiceId, String status, String reason, UUID billingRunId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("status", status);
        details.put("failureReason", reason);
        details.put("billingRunId", billingRunId != null ? billingRunId.toString() : null);
        logInvoiceProcessing(invoiceId, "PROCESSED_FAILURE", userId, details);
    }

    /**
     * Log invoice skipped (not eligible).
     */
    public void logInvoiceSkipped(UUID invoiceId, String reason, UUID billingRunId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("skipReason", reason);
        details.put("billingRunId", billingRunId != null ? billingRunId.toString() : null);
        logInvoiceProcessing(invoiceId, "SKIPPED", userId, details);
    }

    /**
     * Log payment initiated.
     */
    public void logPaymentInitiated(UUID invoiceId, UUID paymentIntentId, long amountMinor, String currency, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("amountMinor", amountMinor);
        details.put("currency", currency);
        logPayment(invoiceId, paymentIntentId, "PAYMENT_INITIATED", userId, details);
    }

    /**
     * Log payment success.
     */
    public void logPaymentSuccess(UUID invoiceId, UUID paymentIntentId, UUID transactionId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("transactionId", transactionId != null ? transactionId.toString() : null);
        logPayment(invoiceId, paymentIntentId, "PAYMENT_SUCCESS", userId, details);
    }

    /**
     * Log payment failure.
     */
    public void logPaymentFailure(UUID invoiceId, UUID paymentIntentId, String reason, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("failureReason", reason);
        logPayment(invoiceId, paymentIntentId, "PAYMENT_FAILURE", userId, details);
    }

    /**
     * Log payment pending (awaiting capture).
     */
    public void logPaymentPending(UUID invoiceId, UUID paymentIntentId, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("status", "PENDING_CAPTURE");
        logPayment(invoiceId, paymentIntentId, "PAYMENT_PENDING", userId, details);
    }

    /**
     * Log DLQ entry created.
     */
    public void logDLQCreated(UUID dlqId, UUID invoiceId, String errorType, String errorMessage, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("errorType", errorType);
        details.put("errorMessage", errorMessage);
        logDLQ(dlqId, invoiceId, "DLQ_CREATED", userId, details);
    }

    /**
     * Log DLQ entry resolved.
     */
    public void logDLQResolved(UUID dlqId, UUID invoiceId, String resolvedBy, String resolutionNotes, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("invoiceId", invoiceId != null ? invoiceId.toString() : null);
        details.put("resolvedBy", resolvedBy);
        details.put("resolutionNotes", resolutionNotes);
        logDLQ(dlqId, invoiceId, "DLQ_RESOLVED", userId, details);
    }

    /**
     * Log reconciliation started.
     */
    public void logReconciliationStarted(UUID reconciliationId, String period, String userId) {
        Map<String, Object> details = new HashMap<>();
        details.put("period", period);
        logReconciliation(reconciliationId, "RECONCILIATION_STARTED", userId, details);
    }

    /**
     * Log reconciliation completed.
     */
    public void logReconciliationCompleted(UUID reconciliationId, Map<String, Object> results, String userId) {
        logReconciliation(reconciliationId, "RECONCILIATION_COMPLETED", userId, results);
    }
}
