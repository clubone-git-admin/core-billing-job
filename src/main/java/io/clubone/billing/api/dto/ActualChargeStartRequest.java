package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record ActualChargeStartRequest(
        @NotNull UUID billingRunId,
        UUID stageRunId,
        UUID triggeredBy,
        String mode,
        ActualChargeOptionsDto options) {}
