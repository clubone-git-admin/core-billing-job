package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Linked tab: account, opportunities, related_cases (no converted_contact/converted_opportunity). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactLinkedDto(
        @JsonProperty("account") CrmLeadRelatedDto.RelatedAccount account,
        @JsonProperty("opportunities") List<CrmContactOpportunityDto> opportunities,
        @JsonProperty("related_cases") List<CrmLeadRelatedDto.RelatedCase> relatedCases
) {}
