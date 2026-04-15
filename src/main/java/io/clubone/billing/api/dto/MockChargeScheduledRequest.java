package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.time.OffsetDateTime;
import java.util.UUID;

public record MockChargeScheduledRequest(
        @NotNull UUID billingRunId,
        UUID stageRunId,
        @NotNull OffsetDateTime scheduledFor,
        String timezone,
        UUID triggeredBy,
        MockChargeOptionsDto options
) {}
