package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmOpportunitySummaryDto(
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("opportunity_code") String opportunityCode,
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("contact_code") String contactCode,
        @JsonProperty("client_id") String clientId,
        @JsonProperty("name") String name,
        @JsonProperty("client_status") String clientStatus,
        @JsonProperty("club") String club,
        @JsonProperty("opportunity_stage_id") String opportunityStageId,
        @JsonProperty("stage_code") String stageCode,
        @JsonProperty("stage_display_name") String stageDisplayName,
        @JsonProperty("lead_type_id") String leadTypeId,
        @JsonProperty("type_display_name") String typeDisplayName,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("owner_user_id") String ownerUserId,
        @JsonProperty("owner_display_name") String ownerDisplayName,
        @JsonProperty("amount") Double amount,
        @JsonProperty("expected_close_date") String expectedCloseDate,
        @JsonProperty("contact_name") String contactName
) {}
