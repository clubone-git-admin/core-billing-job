package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * Request DTO for updating a billing run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UpdateBillingRunRequest(
    String approvalNotes,
    Map<String, Object> summaryJson
) {
}
