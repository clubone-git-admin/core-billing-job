package io.clubone.billing.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record ActualChargePaymentStatusUpdateRequest(
        @NotNull UUID clientPaymentTransactionId,
        @NotBlank String gatewayStatus,
        UUID clientPaymentIntentId,
        String reason,
        UUID updatedBy) {}
