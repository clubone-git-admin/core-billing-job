package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * DTO for approval information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApprovalDto(
    UUID approvalId,
    UUID billingRunId,
    Integer approvalLevel,
    String approverRole,
    StatusDto approvalStatus,
    UUID approverId,
    OffsetDateTime approvedOn,
    String notes,
    OffsetDateTime createdOn
) {
}
