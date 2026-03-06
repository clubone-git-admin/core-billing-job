package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request for creating a case.
 * - POST /api/crm/contacts/{contactId}/cases: contact_id from path; body contact_id ignored.
 * - POST /api/crm/cases: contact_id optional in body; no path contact.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateCaseRequest(
        @JsonProperty("subject") String subject,
        @JsonProperty("case_type_id") String caseTypeId,
        @JsonProperty("case_status_id") String caseStatusId,
        @JsonProperty("case_priority_id") String casePriorityId,
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("account_id") String accountId,
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("owner_user_id") String ownerUserId,
        @JsonProperty("channel_code") String channelCode,
        @JsonProperty("description") String description,
        @JsonProperty("internal_notes") String internalNotes
) {}
