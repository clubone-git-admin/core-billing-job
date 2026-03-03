package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for POST /api/crm/leads/bulk/activities.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmBulkActivitiesResponse(
        @JsonProperty("created_count") int createdCount,
        @JsonProperty("failed_count") int failedCount,
        @JsonProperty("activity_ids") List<String> activityIds,
        @JsonProperty("errors") List<CrmBulkActivityErrorDto> errors
) {
    public record CrmBulkActivityErrorDto(
            @JsonProperty("lead_id") String leadId,
            @JsonProperty("error") String error
    ) {}
}
