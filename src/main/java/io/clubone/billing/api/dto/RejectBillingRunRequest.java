package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * Request DTO for rejecting a billing run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RejectBillingRunRequest(
    @NotNull Integer approvalLevel,
    @NotNull String approverRole,
    @NotNull UUID approverId,
    @NotNull String notes,
    String rejectionReason
) {
}
