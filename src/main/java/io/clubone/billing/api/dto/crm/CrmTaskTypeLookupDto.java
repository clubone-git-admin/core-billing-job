package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmTaskTypeLookupDto(
        @JsonProperty("task_type_id") String taskTypeId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
