package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for GET /api/crm/leads/{lead_id}/activities.
 */
public record CrmLeadActivitiesResponse(
        @JsonProperty("activities") List<CrmLeadActivityDto> activities,
        @JsonProperty("total") long total
) {
}
