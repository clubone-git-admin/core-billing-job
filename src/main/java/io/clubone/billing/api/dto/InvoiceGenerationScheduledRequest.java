package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * POST /api/billing/invoice-generation/scheduled-runs
 * {@code scheduledFor} must be an absolute instant (prefer {@code Z} / UTC).
 */
public record InvoiceGenerationScheduledRequest(
        @NotNull UUID billingRunId,
        UUID stageRunId,
        @NotNull OffsetDateTime scheduledFor,
        String timezone,
        UUID triggeredBy,
        InvoiceGenerationOptionsDto options
) {}
