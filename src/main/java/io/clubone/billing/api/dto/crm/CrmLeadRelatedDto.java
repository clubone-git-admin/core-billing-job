package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * DTO for lead related records.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadRelatedDto(
        @JsonProperty("converted_contact") ConvertedContact convertedContact,
        @JsonProperty("converted_opportunity") ConvertedOpportunity convertedOpportunity,
        @JsonProperty("related_cases") List<RelatedCase> relatedCases,
        @JsonProperty("related_account") RelatedAccount relatedAccount
) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ConvertedContact(
            @JsonProperty("contact_id") String contactId,
            @JsonProperty("full_name") String fullName,
            @JsonProperty("email") String email,
            @JsonProperty("phone") String phone,
            @JsonProperty("account_name") String accountName
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ConvertedOpportunity(
            @JsonProperty("opportunity_id") String opportunityId,
            @JsonProperty("opportunity_name") String opportunityName,
            @JsonProperty("stage_code") String stageCode,
            @JsonProperty("stage_display_name") String stageDisplayName,
            @JsonProperty("amount") Double amount,
            @JsonProperty("expected_close_date") String expectedCloseDate,
            @JsonProperty("contact_name") String contactName,
            @JsonProperty("owner_name") String ownerName
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RelatedCase(
            @JsonProperty("case_id") String caseId,
            @JsonProperty("case_number") String caseNumber,
            @JsonProperty("subject") String subject,
            @JsonProperty("status_display_name") String statusDisplayName,
            @JsonProperty("priority_display_name") String priorityDisplayName,
            @JsonProperty("created_date") String createdDate
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RelatedAccount(
            @JsonProperty("account_id") String accountId,
            @JsonProperty("account_name") String accountName,
            @JsonProperty("type_display_name") String typeDisplayName,
            @JsonProperty("phone") String phone,
            @JsonProperty("address") String address
    ) {
    }
}

