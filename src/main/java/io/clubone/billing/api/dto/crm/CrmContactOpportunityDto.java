package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactOpportunityDto(
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("opportunity_name") String opportunityName,
        @JsonProperty("stage_code") String stageCode,
        @JsonProperty("stage_display_name") String stageDisplayName,
        @JsonProperty("amount") Double amount,
        @JsonProperty("expected_close_date") String expectedCloseDate,
        @JsonProperty("owner_name") String ownerName
) {}
