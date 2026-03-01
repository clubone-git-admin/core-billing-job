package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Summary DTO for lead list / Kanban views.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadSummaryDto(
        @JsonProperty("lead_id") String leadId,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("company") String company,
        @JsonProperty("status_code") String statusCode,
        @JsonProperty("status_display_name") String statusDisplayName,
        @JsonProperty("owner_name") String ownerName,
        @JsonProperty("source_display_name") String sourceDisplayName,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("last_contacted_on") String lastContactedOn,
        @JsonProperty("warmth_score") Integer warmthScore,
        @JsonProperty("converted_contact_id") String convertedContactId,
        @JsonProperty("converted_opportunity_id") String convertedOpportunityId
) {
}

