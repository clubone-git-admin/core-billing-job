package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record MockChargeSkipRequest(
        @NotNull UUID skippedBy,
        String reason
) {}
