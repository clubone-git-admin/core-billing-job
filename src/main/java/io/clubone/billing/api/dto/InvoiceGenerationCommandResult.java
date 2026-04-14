package io.clubone.billing.api.dto;

/**
 * Outcome of POST start: {@link InvoiceGenerationStartHttpStatus#ACCEPTED_202} for async queue, {@code OK_200} otherwise.
 */
public record InvoiceGenerationCommandResult(
        InvoiceGenerationRunResponse response,
        InvoiceGenerationStartHttpStatus httpStatus
) {
}
