package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * DTO for billing stage information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StageDto(
    String stageCode,
    String displayName,
    Integer stageSequence,
    String description,
    Boolean isOptional
) {
}
