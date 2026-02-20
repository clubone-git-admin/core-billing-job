package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for billing run information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingRunDto(
    UUID billingRunId,
    String billingRunCode,
    LocalDate dueDate,
    UUID locationId,
    String locationName,
    StatusDto billingRunStatus,
    StageDto currentStage,
    StatusDto approvalStatus,
    OffsetDateTime startedOn,
    OffsetDateTime endedOn,
    Map<String, Object> summaryJson,
    UUID createdBy,
    OffsetDateTime createdOn,
    OffsetDateTime modifiedOn,
    UUID sourceRunId,
    String sourceRunCode,
    UUID approvedBy,
    OffsetDateTime approvedOn,
    String approvalNotes,
    List<StageRunDto> stages,
    List<ApprovalDto> approvals
) {
}
