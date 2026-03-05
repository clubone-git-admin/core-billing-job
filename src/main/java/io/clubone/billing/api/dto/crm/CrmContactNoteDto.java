package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactNoteDto(
        @JsonProperty("id") String id,
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("author") String author,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("body") String body
) {}
