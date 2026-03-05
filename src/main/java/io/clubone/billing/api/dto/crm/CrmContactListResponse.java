package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmContactListResponse(
        @JsonProperty("contacts") List<CrmContactSummaryDto> contacts,
        @JsonProperty("total") long total
) {}
