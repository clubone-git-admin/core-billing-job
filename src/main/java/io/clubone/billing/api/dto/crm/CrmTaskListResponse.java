package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmTaskListResponse(
        @JsonProperty("tasks") List<CrmTaskSummaryDto> tasks,
        @JsonProperty("total") long total
) {}
