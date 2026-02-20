package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * DTO for failure type information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FailureTypeDto(
    String failureTypeCode,
    String displayName,
    Boolean isRetryable
) {
}
