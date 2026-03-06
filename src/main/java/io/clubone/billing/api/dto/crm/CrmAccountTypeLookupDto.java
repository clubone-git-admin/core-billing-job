package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmAccountTypeLookupDto(
        @JsonProperty("account_type_id") String accountTypeId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
