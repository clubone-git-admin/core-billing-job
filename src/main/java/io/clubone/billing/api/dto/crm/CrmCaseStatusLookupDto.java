package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmCaseStatusLookupDto(
        @JsonProperty("case_status_id") String caseStatusId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
