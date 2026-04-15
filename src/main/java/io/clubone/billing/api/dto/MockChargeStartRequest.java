package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record MockChargeStartRequest(
        @NotNull UUID billingRunId,
        UUID stageRunId,
        UUID triggeredBy,
        String mode,
        MockChargeOptionsDto options
) {}
