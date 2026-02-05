package io.clubone.billing.batch.model;

/**
 * Enumeration of invoice status values used in transactions.lu_invoice_status.
 * Centralizing these avoids hard-coded strings like 'PENDING','DUE'.
 */
public enum InvoiceStatus {
    PENDING("PENDING"),
    DUE("DUE");

    private final String code;

    InvoiceStatus(String code) {
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
