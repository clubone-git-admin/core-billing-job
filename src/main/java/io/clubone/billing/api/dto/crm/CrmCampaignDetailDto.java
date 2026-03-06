package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCampaignDetailDto(
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("campaign_name") String campaignName,
        @JsonProperty("campaign_type_id") String campaignTypeId,
        @JsonProperty("campaign_type_display_name") String campaignTypeDisplayName,
        @JsonProperty("campaign_status_id") String campaignStatusId,
        @JsonProperty("campaign_status_display_name") String campaignStatusDisplayName,
        @JsonProperty("description") String description,
        @JsonProperty("start_date") String startDate,
        @JsonProperty("end_date") String endDate,
        @JsonProperty("budget") Double budget,
        @JsonProperty("expected_leads") Integer expectedLeads,
        @JsonProperty("actual_leads") Integer actualLeads,
        @JsonProperty("expected_revenue") Double expectedRevenue,
        @JsonProperty("actual_revenue") Double actualRevenue,
        @JsonProperty("conversion_rate") Double conversionRate,
        @JsonProperty("marketing_code") String marketingCode,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("modified_on") String modifiedOn
) {}
