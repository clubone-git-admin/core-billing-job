package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.UUID;

/**
 * Request DTO for failing a stage.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FailStageRequest(
    String errorMessage,
    Map<String, Object> errorDetails,
    UUID failedBy
) {
}
