package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmAddContactMethodRequest(
        @JsonProperty("contact_method_id") String contactMethodId,
        @JsonProperty("is_primary") Boolean isPrimary
) {}
