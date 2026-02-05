package io.clubone.billing.batch.model;

/**
 * Enumeration of payment gateway statuses returned by the external payment service.
 * Keeps values like PENDING_CAPTURE / FAILED / AUTHORIZED centralized.
 */
public enum GatewayStatus {
    PENDING_CAPTURE("PENDING_CAPTURE"),
    FAILED("FAILED"),
    AUTHORIZED("AUTHORIZED"),
    CREATED("CREATED"),
    CAPTURED("CAPTURED");

    private final String code;

    GatewayStatus(String code) {
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

