package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Row from {@code subscription_billing_history} for billing run invoice lists.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubscriptionBillingHistoryItemDto(
        UUID subscriptionBillingHistoryId,
        UUID invoiceId,
        String invoiceNumber,
        UUID subscriptionInstanceId,
        OffsetDateTime billingAttemptOn,
        String billingStatusCode,
        String failureReason,
        Boolean isMock,
        UUID billingRunId,
        UUID stageRunId,
        BigDecimal invoiceSubTotal,
        BigDecimal invoiceTaxAmount,
        BigDecimal invoiceDiscountAmount,
        BigDecimal invoiceTotalAmount
) {
}
