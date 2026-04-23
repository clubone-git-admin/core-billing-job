package io.clubone.billing.api.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;
import java.util.UUID;

/**
 * Body for reverting voided rows back to {@code PENDING} invoice status.
 * Same id resolution rules as {@link InvoiceGenerationVoidInvoicesRequest}.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record InvoiceGenerationRevertInvoicesRequest(
        List<UUID> subscriptionBillingHistoryIds,
        List<UUID> invoiceIds,
        String requestedBy
) {
}
