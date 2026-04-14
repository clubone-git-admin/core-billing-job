package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.UUID;

/**
 * Options for an invoice generation execution (stored in stage {@code summary_json}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationOptionsDto(
        Boolean regenerateAll,
        List<UUID> onlyFailedSubscriptionInstanceIds,
        Boolean dryRun,
        Integer maxInvoices,
        String correlationId
) {
}
