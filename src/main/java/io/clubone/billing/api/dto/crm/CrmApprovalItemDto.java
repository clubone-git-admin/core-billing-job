package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmApprovalItemDto(
        @JsonProperty("approval_id") String approvalId,
        @JsonProperty("request_type") String requestType,
        @JsonProperty("entity_ref") String entityRef,
        @JsonProperty("amount") Double amount,
        @JsonProperty("requester_name") String requesterName,
        @JsonProperty("requested_on") String requestedOn
) {}
