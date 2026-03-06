package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateCampaignRequest(
        @JsonProperty("campaign_name") String campaignName,
        @JsonProperty("campaign_type_id") String campaignTypeId,
        @JsonProperty("campaign_status_id") String campaignStatusId,
        @JsonProperty("description") String description,
        @JsonProperty("start_date") String startDate,
        @JsonProperty("end_date") String endDate,
        @JsonProperty("budget") Double budget,
        @JsonProperty("expected_leads") Integer expectedLeads,
        @JsonProperty("expected_revenue") Double expectedRevenue,
        @JsonProperty("marketing_code") String marketingCode
) {}
