package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateAccountRequest(
        @JsonProperty("account_name") String accountName,
        @JsonProperty("account_type_id") String accountTypeId,
        @JsonProperty("industry") String industry,
        @JsonProperty("website") String website,
        @JsonProperty("phone") String phone,
        @JsonProperty("email") String email,
        @JsonProperty("parent_account_id") String parentAccountId,
        @JsonProperty("relationship_manager_id") String relationshipManagerId,
        @JsonProperty("description") String description
) {}
