package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for lead field history entries.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadHistoryItemDto(
        @JsonProperty("id") String id,
        @JsonProperty("lead_id") String leadId,
        @JsonProperty("changed_at") String changedAt,
        @JsonProperty("changed_by") String changedBy,
        @JsonProperty("field_display_name") String fieldDisplayName,
        @JsonProperty("old_value") String oldValue,
        @JsonProperty("new_value") String newValue
) {
}

