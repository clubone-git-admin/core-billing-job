package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmTaskPriorityLookupDto(
        @JsonProperty("task_priority_id") String taskPriorityId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
