package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * Request DTO for cancelling a billing run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CancelBillingRunRequest(
    @NotNull String cancellationReason,
    @NotNull UUID cancelledBy
) {
}
