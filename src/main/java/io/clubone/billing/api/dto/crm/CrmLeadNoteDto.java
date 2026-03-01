package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for lead notes.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadNoteDto(
        @JsonProperty("id") String id,
        @JsonProperty("lead_id") String leadId,
        @JsonProperty("author") String author,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("body") String body
) {
}

