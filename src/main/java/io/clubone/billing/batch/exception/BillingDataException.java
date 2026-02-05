package io.clubone.billing.batch.exception;

import java.util.UUID;

/**
 * Exception for data validation and integrity errors.
 * These are typically not retryable.
 */
public class BillingDataException extends RuntimeException {
    
    private final UUID invoiceId;
    private final UUID billingRunId;
    private final String fieldName;
    private final Object fieldValue;

    public BillingDataException(String message, UUID invoiceId, UUID billingRunId, String fieldName, Object fieldValue) {
        super(message);
        this.invoiceId = invoiceId;
        this.billingRunId = billingRunId;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public BillingDataException(String message, UUID invoiceId, UUID billingRunId, String fieldName, Object fieldValue, Throwable cause) {
        super(message, cause);
        this.invoiceId = invoiceId;
        this.billingRunId = billingRunId;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public UUID getInvoiceId() {
        return invoiceId;
    }

    public UUID getBillingRunId() {
        return billingRunId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getFieldValue() {
        return fieldValue;
    }
}
