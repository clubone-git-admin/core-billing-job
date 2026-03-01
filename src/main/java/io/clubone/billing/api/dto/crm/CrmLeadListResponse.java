package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Wrapper for lead list responses with total count.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadListResponse(
        @JsonProperty("leads") List<CrmLeadSummaryDto> leads,
        @JsonProperty("total") long total
) {
}

