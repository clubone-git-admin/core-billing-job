package io.clubone.billing.batch.model;

/**
 * Enumeration of billing_run.status values.
 * Centralizing these avoids hard-coded strings like 'RUNNING','COMPLETED'.
 */
public enum BillingRunStatus {
    RUNNING("RUNNING"),
    COMPLETED("COMPLETED"),
    FAILED("FAILED");

    private final String code;

    BillingRunStatus(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code;
    }
}
