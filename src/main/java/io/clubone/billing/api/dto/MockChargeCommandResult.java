package io.clubone.billing.api.dto;

public record MockChargeCommandResult(
        MockChargeRunResponse response,
        MockChargeStartHttpStatus httpStatus
) {}
