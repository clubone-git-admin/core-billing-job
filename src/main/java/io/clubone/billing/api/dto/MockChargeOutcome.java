package io.clubone.billing.api.dto;

/**
 * Classification for mock charge validation (persisted in history / stage summary breakdown).
 */
public enum MockChargeOutcome {
    ELIGIBLE,
    READY_FOR_CHARGE,
    BLOCKED,
    MANDATE_NOT_FOUND,
    MANDATE_EXPIRED,
    MANDATE_REVOKED,
    LIMIT_EXCEEDED,
    PAYMENT_METHOD_INVALID,
    SUBSCRIPTION_INACTIVE,
    INVOICE_INVALID,
    /** Invoice billing currency does not match mandate currency. */
    CURRENCY_MISMATCH,
    PROVIDER_ERROR
}
