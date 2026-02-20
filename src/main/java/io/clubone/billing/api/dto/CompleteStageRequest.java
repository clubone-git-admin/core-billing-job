package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.UUID;

/**
 * Request DTO for completing a stage.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CompleteStageRequest(
    Map<String, Object> summaryJson,
    UUID completedBy
) {
}
