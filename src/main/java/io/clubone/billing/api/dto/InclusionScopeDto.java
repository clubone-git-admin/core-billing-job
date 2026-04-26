package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * One root in a multi-inclusion {@code location.levels} scope (see billing run scope-preview / create).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InclusionScopeDto(
        @NotNull
        @JsonProperty("locationLevelId")
        @JsonAlias("location_level_id")
        UUID locationLevelId,
        /**
         * {@code true} = this level node + all descendants. {@code null} = treat as {@code false}
         * (per contract default).
         */
        @JsonAlias("include_child_locations")
        Boolean includeChildLocations
) {
    /** Whether child levels are rolled up. Default is {@code false} when the field is omitted. */
    public boolean includeChildLevels() {
        return Boolean.TRUE.equals(includeChildLocations);
    }
}
