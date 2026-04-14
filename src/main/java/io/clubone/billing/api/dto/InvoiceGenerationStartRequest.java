package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * POST /api/billing/invoice-generation/runs
 */
public record InvoiceGenerationStartRequest(
        @NotNull UUID billingRunId,
        UUID stageRunId,
        UUID triggeredBy,
        String mode,
        InvoiceGenerationOptionsDto options
) {
}
