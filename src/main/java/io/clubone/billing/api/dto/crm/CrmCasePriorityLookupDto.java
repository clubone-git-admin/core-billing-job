package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmCasePriorityLookupDto(
        @JsonProperty("case_priority_id") String casePriorityId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
