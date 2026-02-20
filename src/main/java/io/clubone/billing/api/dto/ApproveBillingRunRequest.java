package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * Request DTO for approving a billing run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApproveBillingRunRequest(
    @NotNull Integer approvalLevel,
    @NotNull String approverRole,
    @NotNull UUID approverId,
    String notes
) {
}
