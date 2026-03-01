package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simple code/display_name lookup item.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLookupItemDto(
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName
) {
}

