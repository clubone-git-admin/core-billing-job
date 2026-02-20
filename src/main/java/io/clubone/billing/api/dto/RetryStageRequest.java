package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * Request DTO for retrying a stage.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RetryStageRequest(
    Map<String, Object> retryConfig
) {
}
