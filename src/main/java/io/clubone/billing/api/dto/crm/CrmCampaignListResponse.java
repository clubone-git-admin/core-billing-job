package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmCampaignListResponse(
        @JsonProperty("items") List<CrmCampaignSummaryDto> items,
        @JsonProperty("total") long total
) {}
