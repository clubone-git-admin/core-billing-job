package io.clubone.billing.service.mockcharge;

import io.clubone.billing.api.dto.MockChargeOutcome;

/**
 * Maps validation outcomes to UI-facing {@code mock_charge_status} values (see mock charge API spec).
 */
public final class MockChargeUiMapper {

    private MockChargeUiMapper() {}

    /**
     * UI label stored in {@code subscription_billing_history.mock_charge_status}.
     */
    public static String toUiStatus(MockChargeOutcome o) {
        return switch (o) {
            case ELIGIBLE, READY_FOR_CHARGE -> "READY_FOR_CHARGE";
            case MANDATE_NOT_FOUND -> "MANDATE_INVALID";
            case BLOCKED -> "BLOCKED";
            case MANDATE_EXPIRED -> "MANDATE_EXPIRED";
            case MANDATE_REVOKED -> "MANDATE_REVOKED";
            case LIMIT_EXCEEDED -> "LIMIT_EXCEEDED";
            case PAYMENT_METHOD_INVALID -> "PAYMENT_METHOD_INVALID";
            case SUBSCRIPTION_INACTIVE -> "SUBSCRIPTION_INACTIVE";
            case INVOICE_INVALID -> "INVOICE_INVALID";
            case CURRENCY_MISMATCH -> "CURRENCY_MISMATCH";
            case PROVIDER_ERROR -> "PROVIDER_ERROR";
        };
    }
}
