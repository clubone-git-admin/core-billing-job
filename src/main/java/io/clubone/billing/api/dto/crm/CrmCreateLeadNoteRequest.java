package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for creating a lead note.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateLeadNoteRequest(
        @JsonProperty("body") String body,
        @JsonProperty("author") String author
) {
}

