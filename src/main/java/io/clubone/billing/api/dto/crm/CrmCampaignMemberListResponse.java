package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmCampaignMemberListResponse(
        @JsonProperty("items") List<CrmCampaignMemberDto> items,
        @JsonProperty("total") long total
) {}
