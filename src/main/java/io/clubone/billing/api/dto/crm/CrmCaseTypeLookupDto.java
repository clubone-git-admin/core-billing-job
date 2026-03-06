package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmCaseTypeLookupDto(
        @JsonProperty("case_type_id") String caseTypeId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
