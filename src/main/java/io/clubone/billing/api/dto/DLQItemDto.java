package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for Dead Letter Queue item.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DLQItemDto(
    UUID dlqId,
    UUID billingRunId,
    String billingRunCode,
    UUID stageRunId,
    String stageRunCode,
    UUID invoiceId,
    String invoiceNumber,
    UUID subscriptionInstanceId,
    String errorType,
    String errorMessage,
    String errorStackTrace,
    FailureTypeDto failureType,
    Map<String, Object> workItemJson,
    OffsetDateTime createdOn,
    Integer retryCount,
    OffsetDateTime lastRetryOn,
    Boolean resolved,
    OffsetDateTime resolvedOn,
    String resolvedBy,
    String resolutionNotes,
    Map<String, Object> retryStrategy
) {
}
