package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingJobMonitorItemDto(
        UUID stageRunId,
        String stageRunCode,
        UUID billingRunId,
        String billingRunCode,
        LocalDate dueDate,
        String stageCode,
        String stageDisplayName,
        String statusCode,
        String statusDisplayName,
        OffsetDateTime scheduledFor,
        OffsetDateTime startedOn,
        OffsetDateTime endedOn,
        OffsetDateTime modifiedOn,
        Integer attemptNumber,
        Integer staleReclaimCount,
        Boolean canCancel,
        Boolean canRedispatch,
        String billingRunDetailPath
) {}
