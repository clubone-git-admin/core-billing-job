package io.clubone.billing.batch.model;

/**
 * Enumeration of subscription instance status values used in lu_subscription_instance_status.
 * Centralizing these avoids hard-coded strings like 'ACTIVE'.
 */
public enum SubscriptionInstanceStatus {
    ACTIVE("ACTIVE");

    private final String code;

    SubscriptionInstanceStatus(String code) {
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
