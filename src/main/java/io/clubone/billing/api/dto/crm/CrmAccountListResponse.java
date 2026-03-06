package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmAccountListResponse(
        @JsonProperty("accounts") List<CrmAccountSummaryDto> accounts,
        @JsonProperty("total") long total
) {}
