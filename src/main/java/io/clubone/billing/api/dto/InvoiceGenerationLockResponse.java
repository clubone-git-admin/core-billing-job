package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationLockResponse(
        UUID billingRunId,
        UUID invoiceGenerationStageRunId,
        int invoicesTransitionedPendingToDue,
        List<UUID> invoiceIds,
        OffsetDateTime lockedAt,
        UUID lockedBy,
        String notes,
        String message
) {}
