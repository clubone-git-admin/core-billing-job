package io.clubone.billing.api.dto;

public record ActualChargeCommandResult(
        ActualChargeRunResponse response, ActualChargeStartHttpStatus httpStatus) {}
