package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record MockChargeProceedRequest(
        @NotNull UUID requestedBy,
        String notes,
        @NotNull Integer confirmEligibleCount,
        @NotNull BigDecimal confirmEligibleAmount,
        @NotNull Integer confirmBlockedCount
) {}
