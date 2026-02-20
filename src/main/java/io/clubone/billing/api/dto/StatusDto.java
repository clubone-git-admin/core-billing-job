package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * DTO for status information with code and display name.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StatusDto(
    String statusCode,
    String displayName,
    String description
) {
}
