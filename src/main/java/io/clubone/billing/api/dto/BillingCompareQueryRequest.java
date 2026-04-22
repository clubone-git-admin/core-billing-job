package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public record BillingCompareQueryRequest(
        @NotBlank String scenarioCode,
        @NotNull SnapshotRef left,
        @NotNull SnapshotRef right,
        CompareFilters filters,
        CompareSort sort,
        String format,
        @Min(1) Integer page,
        @Min(1) @Max(500) Integer pageSize
) {
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnapshotRef(
            @NotBlank String stageCode,
            @NotNull UUID runId
    ) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CompareFilters(
            String search,
            Boolean diffOnly,
            String severity
    ) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CompareSort(
            String by,
            String dir
    ) {}
}
