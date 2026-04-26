package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * One inclusion scope after normalization, for API responses and (optionally) persisted run summary.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InclusionScopeSummaryDto(
        @JsonProperty("locationLevelId")
        @JsonAlias("location_level_id")
        UUID locationLevelId,
        @JsonProperty("includeChildLocations")
        @JsonAlias("include_child_locations")
        boolean includeChildLocations,
        @JsonProperty("locationLevelName")
        @JsonAlias("location_level_name")
        String locationLevelName,
        // leafCount: distinct physical sites from this scope only (pre-merge)
        @JsonProperty("leafCount")
        @JsonAlias({"leaf_count", "resolved_location_count"})
        int leafCount
) {
    public static InclusionScopeSummaryDto fromInclusion(
            InclusionScopeDto scope, String levelName, int resolvedLocationCount) {
        return new InclusionScopeSummaryDto(
                scope.locationLevelId(), scope.includeChildLevels(), levelName, resolvedLocationCount);
    }
}
