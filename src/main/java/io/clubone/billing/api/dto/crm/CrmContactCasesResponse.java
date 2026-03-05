package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmContactCasesResponse(
        @JsonProperty("cases") List<CrmContactCaseDto> cases,
        @JsonProperty("total") long total
) {}
