package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactOpportunityDto(
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("opportunity_name") String opportunityName,
        @JsonProperty("stage_code") String stageCode,
        @JsonProperty("stage_display_name") String stageDisplayName,
        @JsonProperty("amount") @JsonInclude(JsonInclude.Include.ALWAYS) Double amount,
        @JsonProperty("expected_close_date") String expectedCloseDate,
        @JsonProperty("owner_name") String ownerName,
        @JsonProperty("has_recurring") @JsonInclude(JsonInclude.Include.ALWAYS) Boolean hasRecurring,
        @JsonProperty("recurring_amount") @JsonInclude(JsonInclude.Include.ALWAYS) Double recurringAmount,
        @JsonProperty("recurring_total_amount") @JsonInclude(JsonInclude.Include.ALWAYS) Double recurringTotalAmount
) {}
