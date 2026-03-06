package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCaseHistoryItemDto(
        @JsonProperty("history_id") String historyId,
        @JsonProperty("case_status_id") String caseStatusId,
        @JsonProperty("previous_case_status_id") String previousCaseStatusId,
        @JsonProperty("status_display_name") String statusDisplayName,
        @JsonProperty("previous_status_display_name") String previousStatusDisplayName,
        @JsonProperty("entered_on") String enteredOn,
        @JsonProperty("exited_on") String exitedOn,
        @JsonProperty("duration_seconds") Integer durationSeconds,
        @JsonProperty("changed_by_display_name") String changedByDisplayName,
        @JsonProperty("change_reason") String changeReason,
        @JsonProperty("change_source") String changeSource,
        @JsonProperty("notes") String notes
) {}
