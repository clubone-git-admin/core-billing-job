package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Per-row outcome for void / revert-pending. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationInvoiceActionRow(
        UUID subscriptionBillingHistoryId,
        UUID invoiceId,
        String invoiceStatus,
        OffsetDateTime voidedAt,
        OffsetDateTime revertedAt,
        String note
) {
}
