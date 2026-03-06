package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmPipelineStageSummaryDto(
        @JsonProperty("stage_code") String stageCode,
        @JsonProperty("stage_name") String stageName,
        @JsonProperty("count") int count,
        @JsonProperty("total_amount") double totalAmount
) {}
