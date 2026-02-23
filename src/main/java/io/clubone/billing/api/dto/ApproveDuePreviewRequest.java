package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * Request DTO for approving/denying a due preview stage run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApproveDuePreviewRequest(
    @NotNull(message = "approverId is required")
    UUID approverId,
    
    String approverRole,
    
    Integer approvalLevel,
    
    String notes,
    
    String rejectionReason
) {
}
