package io.clubone.billing.batch.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.batch.audit.AuditService;
import io.clubone.billing.batch.model.BillingWorkItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for managing Dead Letter Queue (DLQ) entries.
 * Stores failed billing items for manual review and recovery.
 */
@Service
public class DeadLetterQueueService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final AuditService auditService;

    public DeadLetterQueueService(JdbcTemplate jdbc, ObjectMapper objectMapper, AuditService auditService) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.auditService = auditService;
    }

    /**
     * Add a failed billing item to the Dead Letter Queue.
     */
    public void addToDLQ(BillingWorkItem item, Throwable error, String errorType) {
        UUID dlqId = null;
        try {
            String workItemJson = objectMapper.writeValueAsString(item);
            String stackTrace = getStackTrace(error);
            String errorMessage = error != null ? error.getMessage() : "Unknown error";

            // Generate DLQ ID before insert for audit logging
            dlqId = UUID.randomUUID();

            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_dead_letter_queue
                (dlq_id, billing_run_id, invoice_id, subscription_instance_id, error_type, 
                 error_message, error_stack_trace, work_item_json, created_on)
                VALUES (?::uuid, ?::uuid, ?::uuid, ?::uuid, ?, ?, ?, ?::jsonb, now())
                """,
                dlqId.toString(),
                item.getBillingRunId() != null ? item.getBillingRunId().toString() : null,
                item.getInvoiceId() != null ? item.getInvoiceId().toString() : null,
                item.getSubscriptionInstanceId() != null ? item.getSubscriptionInstanceId().toString() : null,
                errorType != null ? errorType : "UNKNOWN",
                errorMessage,
                stackTrace,
                workItemJson
            );

            log.warn("Added to DLQ: invoiceId={} billingRunId={} errorType={} error={}",
                item.getInvoiceId(), item.getBillingRunId(), errorType, errorMessage);
            
            // Audit: DLQ entry created
            auditService.logDLQCreated(dlqId, item.getInvoiceId(), errorType, errorMessage, "system");
        } catch (Exception e) {
            log.error("Failed to add to DLQ: invoiceId={} billingRunId={}",
                item.getInvoiceId(), item.getBillingRunId(), e);
        }
    }

    /**
     * Get unresolved DLQ entries for a billing run.
     */
    public List<Map<String, Object>> getUnresolvedEntries(UUID billingRunId) {
        return jdbc.queryForList("""
            SELECT dlq_id, billing_run_id, invoice_id, subscription_instance_id,
                   error_type, error_message, created_on, retry_count, last_retry_on
            FROM client_subscription_billing.billing_dead_letter_queue
            WHERE billing_run_id = ?::uuid AND resolved = false
            ORDER BY created_on DESC
            """,
            billingRunId.toString()
        );
    }

    /**
     * Get count of unresolved DLQ entries.
     */
    public int getUnresolvedCount() {
        try {
            Integer count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM client_subscription_billing.billing_dead_letter_queue WHERE resolved = false",
                Integer.class
            );
            return count != null ? count : 0;
        } catch (DataAccessException e) {
            log.error("Failed to get DLQ count", e);
            return 0;
        }
    }

    /**
     * Mark a DLQ entry as resolved.
     */
    public void markResolved(UUID dlqId, String resolvedBy, String resolutionNotes) {
        // Get invoice_id before update for audit logging
        UUID invoiceId = null;
        try {
            List<Map<String, Object>> results = jdbc.queryForList("""
                SELECT invoice_id FROM client_subscription_billing.billing_dead_letter_queue
                WHERE dlq_id = ?::uuid
                """,
                dlqId.toString()
            );
            if (!results.isEmpty() && results.get(0).get("invoice_id") != null) {
                invoiceId = UUID.fromString(results.get(0).get("invoice_id").toString());
            }
        } catch (Exception e) {
            log.debug("Could not get invoice_id for DLQ audit: dlqId={}", dlqId, e);
        }
        
        jdbc.update("""
            UPDATE client_subscription_billing.billing_dead_letter_queue
            SET resolved = true, resolved_on = now(), resolved_by = ?, resolution_notes = ?
            WHERE dlq_id = ?::uuid
            """,
            resolvedBy,
            resolutionNotes,
            dlqId.toString()
        );
        log.info("Marked DLQ entry as resolved: dlqId={} resolvedBy={}", dlqId, resolvedBy);
        
        // Audit: DLQ entry resolved
        auditService.logDLQResolved(dlqId, invoiceId, resolvedBy, resolutionNotes, resolvedBy);
    }

    /**
     * Retry a DLQ entry (increment retry count).
     */
    public void incrementRetryCount(UUID dlqId) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_dead_letter_queue
            SET retry_count = retry_count + 1, last_retry_on = now()
            WHERE dlq_id = ?::uuid
            """,
            dlqId.toString()
        );
    }

    private String getStackTrace(Throwable error) {
        if (error == null) {
            return null;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        error.printStackTrace(pw);
        return sw.toString();
    }
}
