package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScopePreviewResponse(
    int selectedBranchTotal,
    List<ScopeLocationItemDto> includedLocations,
    List<ScopeLocationExclusionDto> excludedLocations,
    @JsonProperty("inclusionScopeSummaries")
    @JsonAlias("inclusion_scope_summaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<InclusionScopeSummaryDto> inclusionScopeSummaries
) {
}
