package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmCampaignTypeLookupDto(
        @JsonProperty("campaign_type_id") String campaignTypeId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
