package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmEntitySearchItemDto(
        @JsonProperty("entity_id") String entityId,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("secondary_text") String secondaryText
) {}
