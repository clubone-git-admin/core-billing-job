package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;

/** Batch void / revert result (spec §2 / §3). */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationInvoiceBatchResponse(
        int voided, // for void endpoint
        int reverted, // for revert endpoint
        int failed,
        List<InvoiceGenerationInvoiceActionRow> results,
        List<InvoiceGenerationInvoiceActionError> errors
) {
    public static InvoiceGenerationInvoiceBatchResponse voidResult(
            int voided, int failed, List<InvoiceGenerationInvoiceActionRow> results, List<InvoiceGenerationInvoiceActionError> errors) {
        return new InvoiceGenerationInvoiceBatchResponse(voided, 0, failed, results, errors);
    }

    public static InvoiceGenerationInvoiceBatchResponse revertResult(
            int reverted, int failed, List<InvoiceGenerationInvoiceActionRow> results, List<InvoiceGenerationInvoiceActionError> errors) {
        return new InvoiceGenerationInvoiceBatchResponse(0, reverted, failed, results, errors);
    }
}
