package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmTaskStatusLookupDto(
        @JsonProperty("task_status_id") String taskStatusId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
