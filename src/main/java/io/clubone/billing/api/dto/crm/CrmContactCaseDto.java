package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactCaseDto(
        @JsonProperty("case_id") String caseId,
        @JsonProperty("case_number") String caseNumber,
        @JsonProperty("subject") String subject,
        @JsonProperty("case_type_display_name") String caseTypeDisplayName,
        @JsonProperty("case_status_display_name") String caseStatusDisplayName,
        @JsonProperty("case_priority_display_name") String casePriorityDisplayName,
        @JsonProperty("owner_name") String ownerName,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("sla_resolve_due_at") String slaResolveDueAt
) {}
