package io.clubone.billing.batch.model;

/**
 * Enumeration of schedule_status values used in subscription_invoice_schedule.
 * Centralizing these avoids hard-coded strings like 'PENDING','DUE','BILLED','FAILED'.
 */
public enum ScheduleStatus {
    PENDING("PENDING"),
    DUE("DUE"),
    BILLED("BILLED"),
    FAILED("FAILED");

    private final String code;

    ScheduleStatus(String code) {
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

