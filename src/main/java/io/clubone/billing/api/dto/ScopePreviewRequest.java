package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Pre-create scope: included / excluded for a given due date.
 * Use {@code inclusion_scopes} for multiple roots, or the legacy single {@code locationLevelId} pair.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScopePreviewRequest(
    @NotNull LocalDate dueDate,
    @Valid
    @JsonProperty("inclusionScopes")
    @JsonAlias("inclusion_scopes")
    List<@Valid InclusionScopeDto> inclusionScopes,
    UUID locationLevelId,
    @JsonAlias("include_child_locations")
    Boolean includeChildLocations,
    /** When {@code inclusionScopes} is not used, required if {@code locationLevelId} is null. */
    UUID applicationId
) {
    public boolean isUseInclusion() {
        return inclusionScopes != null && !inclusionScopes.isEmpty();
    }

    public boolean includeChildLevels() {
        return includeChildLocations == null || includeChildLocations;
    }
}
