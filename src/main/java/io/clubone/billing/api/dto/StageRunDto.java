package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for stage run information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StageRunDto(
    UUID stageRunId,
    String stageRunCode,
    UUID billingRunId,
    String stageCode,
    String stageDisplayName,
    Integer stageSequence,
    String statusCode,
    String statusDisplayName,
    OffsetDateTime scheduledFor,
    OffsetDateTime startedOn,
    OffsetDateTime endedOn,
    Map<String, Object> summaryJson,
    String errorMessage,
    Map<String, Object> errorDetails,
    Integer attemptNumber,
    Integer maxAttempts
) {
}
