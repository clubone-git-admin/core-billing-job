package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmAccountSummaryDto(
        @JsonProperty("account_id") String accountId,
        @JsonProperty("account_code") String accountCode,
        @JsonProperty("account_name") String accountName,
        @JsonProperty("account_type_display_name") String accountTypeDisplayName,
        @JsonProperty("industry") String industry,
        @JsonProperty("website") String website,
        @JsonProperty("phone") String phone,
        @JsonProperty("email") String email,
        @JsonProperty("relationship_manager_name") String relationshipManagerName,
        @JsonProperty("created_on") String createdOn
) {}
