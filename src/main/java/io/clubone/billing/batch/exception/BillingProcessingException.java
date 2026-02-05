package io.clubone.billing.batch.exception;

import java.util.UUID;

/**
 * Base exception for billing processing errors.
 * Includes context information for better error tracking.
 */
public class BillingProcessingException extends RuntimeException {
    
    private final UUID invoiceId;
    private final UUID billingRunId;
    private final String operation;

    public BillingProcessingException(String message, UUID invoiceId, UUID billingRunId, String operation) {
        super(message);
        this.invoiceId = invoiceId;
        this.billingRunId = billingRunId;
        this.operation = operation;
    }

    public BillingProcessingException(String message, UUID invoiceId, UUID billingRunId, String operation, Throwable cause) {
        super(message, cause);
        this.invoiceId = invoiceId;
        this.billingRunId = billingRunId;
        this.operation = operation;
    }

    public UUID getInvoiceId() {
        return invoiceId;
    }

    public UUID getBillingRunId() {
        return billingRunId;
    }

    public String getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return String.format("BillingProcessingException{invoiceId=%s, billingRunId=%s, operation='%s', message='%s'}",
                invoiceId, billingRunId, operation, getMessage());
    }
}
