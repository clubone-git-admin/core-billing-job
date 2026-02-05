package io.clubone.billing.batch.listener;

import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.DueInvoiceRow;
import io.clubone.billing.batch.util.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Skip listener to add failed items to Dead Letter Queue (DLQ).
 * Ensures all skipped items (from processor or writer) are tracked in DLQ for audit and recovery.
 * 
 * This is critical for production-grade systems to maintain complete audit trail.
 */
@Component
public class BillingSkipListener implements SkipListener<DueInvoiceRow, BillingWorkItem> {

    private static final Logger log = LoggerFactory.getLogger(BillingSkipListener.class);

    private final DeadLetterQueueService dlqService;

    public BillingSkipListener(DeadLetterQueueService dlqService) {
        this.dlqService = dlqService;
    }

    /**
     * Gets billingRunId from step execution context.
     * Called during skip operations to get context.
     */
    private UUID getBillingRunId() {
        try {
            StepContext stepContext = StepSynchronizationManager.getContext();
            if (stepContext != null) {
                StepExecution stepExecution = stepContext.getStepExecution();
                if (stepExecution != null) {
                    String runIdStr = stepExecution.getJobExecution().getExecutionContext().getString("billingRunId");
                    if (runIdStr != null) {
                        return UUID.fromString(runIdStr);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not get billingRunId from execution context: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Called when an item is skipped during reading.
     * Note: Reader failures are rare, but we log them for completeness.
     */
    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Item skipped during read: error={}", t.getMessage(), t);
        // Reader failures don't have invoice context, so we can't add to DLQ
        // This is acceptable as reader failures are typically infrastructure issues
    }

    /**
     * Called when an item is skipped during processing.
     * This is where processor failures are captured and added to DLQ.
     * 
     * @param item The DueInvoiceRow that failed processing
     * @param t The exception that caused the skip
     */
    @Override
    public void onSkipInProcess(DueInvoiceRow item, Throwable t) {
        LoggingUtils.setInvoiceId(item.getInvoiceId());
        
        log.warn("Item skipped during processing: invoiceId={} error={}", 
            item.getInvoiceId(), t.getMessage(), t);

        try {
            // Create a BillingWorkItem from the DueInvoiceRow for DLQ
            // This ensures we have all context needed for recovery
            BillingWorkItem workItem = createWorkItemFromRow(item, t);
            
            // Determine error type
            String errorType = determineErrorType(t);
            
            // Add to DLQ
            dlqService.addToDLQ(workItem, t, errorType);
            
            log.info("Added processor failure to DLQ: invoiceId={} errorType={}", 
                item.getInvoiceId(), errorType);
        } catch (Exception e) {
            // If DLQ insertion fails, log but don't fail the job
            log.error("Failed to add processor failure to DLQ: invoiceId={}", 
                item.getInvoiceId(), e);
        }
    }

    /**
     * Called when an item is skipped during writing.
     * Note: Writer failures are already handled in BillingItemWriter.addToDLQ(),
     * but this provides a safety net in case writer doesn't catch all cases.
     */
    @Override
    public void onSkipInWrite(BillingWorkItem item, Throwable t) {
        LoggingUtils.setInvoiceId(item.getInvoiceId());
        LoggingUtils.setBillingRunId(item.getBillingRunId());
        
        log.warn("Item skipped during writing: invoiceId={} billingRunId={} error={}", 
            item.getInvoiceId(), item.getBillingRunId(), t.getMessage(), t);

        try {
            // Writer should have already added to DLQ, but add here as safety net
            // Check if already in DLQ to avoid duplicates (optional optimization)
            String errorType = determineErrorType(t);
            dlqService.addToDLQ(item, t, errorType);
            
            log.info("Added writer failure to DLQ (safety net): invoiceId={} errorType={}", 
                item.getInvoiceId(), errorType);
        } catch (Exception e) {
            log.error("Failed to add writer failure to DLQ: invoiceId={}", 
                item.getInvoiceId(), e);
        }
    }

    /**
     * Creates a BillingWorkItem from DueInvoiceRow for DLQ entry.
     * This ensures we have all necessary context for recovery.
     */
    private BillingWorkItem createWorkItemFromRow(DueInvoiceRow row, Throwable error) {
        BillingWorkItem workItem = new BillingWorkItem();
        
        // Get billingRunId from execution context
        workItem.setBillingRunId(getBillingRunId());
        workItem.setInvoiceId(row.getInvoiceId());
        workItem.setSubscriptionInstanceId(row.getSubscriptionInstanceId());
        workItem.setCycleNumber(row.getCycleNumber());
        workItem.setPaymentDueDate(row.getPaymentDueDate());
        workItem.setInvoiceSubTotal(row.getSubTotal());
        workItem.setInvoiceTaxAmount(row.getTaxAmount());
        workItem.setInvoiceDiscountAmount(row.getDiscountAmount());
        workItem.setInvoiceTotalAmount(row.getTotalAmount());
        workItem.setClientRoleId(row.getClientRoleId());
        workItem.setClientPaymentMethodId(row.getClientPaymentMethodId());
        
        // Set error status - use LIVE_ERROR as default since we don't have runMode context here
        // This workItem is only for DLQ, not written to billing_history, but use valid status for consistency
        workItem.setHistoryStatusCode(io.clubone.billing.batch.model.BillingStatus.LIVE_ERROR.getCode());
        workItem.setFailureReason(error != null ? error.getMessage() : "Unknown error");
        workItem.setMock(false);
        
        return workItem;
    }

    /**
     * Determines error type from exception for categorization in DLQ.
     */
    private String determineErrorType(Throwable t) {
        if (t == null) {
            return "UNKNOWN";
        }
        
        String className = t.getClass().getSimpleName();
        
        // Map common exceptions to error types
        if (className.contains("BillingDataException")) {
            return "BillingDataException";
        } else if (className.contains("BillingProcessingException")) {
            return "BillingProcessingException";
        } else if (className.contains("DataAccessException") || className.contains("SQLException")) {
            return "DatabaseError";
        } else if (className.contains("Timeout") || className.contains("SocketTimeout")) {
            return "TimeoutError";
        } else if (className.contains("CircuitBreaker") || className.contains("Circuit")) {
            return "CircuitBreakerOpen";
        } else if (className.contains("Payment") || className.contains("PaymentService")) {
            return "PaymentServiceError";
        } else {
            return className;
        }
    }
}
