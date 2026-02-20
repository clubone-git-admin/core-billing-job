package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * Standard error response DTO.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
    ErrorDetail error
) {
    public record ErrorDetail(
        String code,
        String message,
        Map<String, Object> details,
        String requestId
    ) {
    }
}
