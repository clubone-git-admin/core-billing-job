package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmEntitySearchResponse(
        @JsonProperty("items") List<CrmEntitySearchItemDto> items,
        @JsonProperty("total") long total
) {}
