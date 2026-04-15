package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ActualChargeRunResponse(
        UUID actualChargeRunId,
        UUID billingRunId,
        String billingRunCode,
        UUID stageRunId,
        String stageRunCode,
        String statusCode,
        String statusDisplayName,
        String mode,
        OffsetDateTime queuedAt,
        OffsetDateTime startedAt,
        OffsetDateTime endedAt,
        Map<String, Object> summaryJson,
        String errorMessage,
        Integer attemptNumber,
        Integer maxAttempts,
        String pollUrl) {}
