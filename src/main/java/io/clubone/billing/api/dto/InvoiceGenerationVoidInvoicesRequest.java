package io.clubone.billing.api.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;
import java.util.UUID;

/**
 * Body for voiding one or more invoice rows tied to an invoice generation run.
 * Use {@code subscription_billing_history_ids} when the grid row has a history id; when draft rows have no
 * history record yet, pass {@code invoice_ids} (at least one of the two lists must be non-empty).
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record InvoiceGenerationVoidInvoicesRequest(
        List<UUID> subscriptionBillingHistoryIds,
        List<UUID> invoiceIds,
        String reason,
        String requestedBy
) {
}
