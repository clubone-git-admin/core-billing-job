package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
/**
 * Location scope resolution for a billing run (create response + persisted summary).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingScopeSummaryDto(
    int selectedBranchTotal,
    int includedCount,
    int excludedCount,
    List<ScopeLocationExclusionDto> excludedLocations,
    @com.fasterxml.jackson.annotation.JsonProperty("inclusionScopeSummaries")
    @com.fasterxml.jackson.annotation.JsonAlias("inclusion_scope_summaries")
    List<InclusionScopeSummaryDto> inclusionScopeSummaries
) {
    public static BillingScopeSummaryDto ofBasics(
            int selectedBranchTotal, int includedCount, int excludedCount, List<ScopeLocationExclusionDto> excluded) {
        return new BillingScopeSummaryDto(selectedBranchTotal, includedCount, excludedCount, excluded, null);
    }
}
