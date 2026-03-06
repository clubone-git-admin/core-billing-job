package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmPipelineSummaryDto(
        @JsonProperty("stages") List<CrmPipelineStageSummaryDto> stages,
        @JsonProperty("total_value") double totalValue
) {}
