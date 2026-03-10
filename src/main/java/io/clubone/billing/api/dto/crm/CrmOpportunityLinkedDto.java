package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for GET /api/crm/opportunities/{opportunityId}/linked.
 * Linked tab: linked contact, related cases, related account.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmOpportunityLinkedDto(
        @JsonProperty("linked_contact") LinkedContact linkedContact,
        @JsonProperty("related_cases") List<CrmLeadRelatedDto.RelatedCase> relatedCases,
        @JsonProperty("related_account") CrmLeadRelatedDto.RelatedAccount relatedAccount
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record LinkedContact(
            @JsonProperty("contact_id") String contactId,
            @JsonProperty("full_name") String fullName,
            @JsonProperty("email") String email,
            @JsonProperty("phone") String phone,
            @JsonProperty("account_name") String accountName
    ) {}
}
