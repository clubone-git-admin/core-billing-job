package io.clubone.billing.api.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record InvoiceGenerationInvoiceActionError(
        UUID subscriptionBillingHistoryId,
        UUID invoiceId,
        String code,
        String message
) {
}
