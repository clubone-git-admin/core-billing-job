package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

/**
 * 409 response body for idempotent re-POST of the same logical run (idempotency key).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingRunConflictResponse(
    String message,
    UUID billingRunId
) {
}
