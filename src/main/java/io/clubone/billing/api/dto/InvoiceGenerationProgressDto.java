package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationProgressDto(
        Integer totalWorkItems,
        Integer processedWorkItems,
        Integer invoicesCreated,
        Integer invoicesFailed,
        Double percentComplete
) {
}
