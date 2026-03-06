package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmEntityTypeLookupDto(
        @JsonProperty("entity_type_id") String entityTypeId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {}
