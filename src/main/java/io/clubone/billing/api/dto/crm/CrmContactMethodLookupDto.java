package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmContactMethodLookupDto(
        @JsonProperty("contact_method_id") String contactMethodId,
        @JsonProperty("contact_method_name") String contactMethodName
) {}
