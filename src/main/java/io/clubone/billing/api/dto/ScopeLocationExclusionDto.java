package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScopeLocationExclusionDto(
    UUID locationId,
    String locationName,
    String reason,
    UUID existingBillingRunId
) {
}
