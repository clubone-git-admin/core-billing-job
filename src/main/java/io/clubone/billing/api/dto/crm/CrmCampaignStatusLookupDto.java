package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmCampaignStatusLookupDto(
        @JsonProperty("campaign_status_id") String campaignStatusId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
