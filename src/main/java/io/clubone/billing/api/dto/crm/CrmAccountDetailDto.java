package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmAccountDetailDto(
        @JsonProperty("account_id") String accountId,
        @JsonProperty("account_code") String accountCode,
        @JsonProperty("account_name") String accountName,
        @JsonProperty("account_type_id") String accountTypeId,
        @JsonProperty("account_type_display_name") String accountTypeDisplayName,
        @JsonProperty("industry") String industry,
        @JsonProperty("website") String website,
        @JsonProperty("phone") String phone,
        @JsonProperty("email") String email,
        @JsonProperty("parent_account_id") String parentAccountId,
        @JsonProperty("parent_account_name") String parentAccountName,
        @JsonProperty("relationship_manager_id") String relationshipManagerId,
        @JsonProperty("relationship_manager_name") String relationshipManagerName,
        @JsonProperty("external_ref") String externalRef,
        @JsonProperty("description") String description,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("created_by") String createdBy,
        @JsonProperty("modified_on") String modifiedOn,
        @JsonProperty("modified_by") String modifiedBy
) {}
