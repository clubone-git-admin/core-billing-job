package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmOpportunityListResponse(
        @JsonProperty("items") List<CrmOpportunitySummaryDto> items,
        @JsonProperty("total") long total
) {}
