package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Request for POST /api/crm/leads/bulk/activities.
 */
public record CrmBulkActivitiesRequest(
        @JsonProperty("lead_ids") List<String> leadIds,
        @JsonProperty("activity_type_code") String activityTypeCode,
        @JsonProperty("activity_status_code") String activityStatusCode,
        @JsonProperty("activity_visibility_code") String activityVisibilityCode,
        @JsonProperty("activity_outcome_code") String activityOutcomeCode,
        @JsonProperty("subject") String subject,
        @JsonProperty("description") String description,
        @JsonProperty("notes") String notes,
        @JsonProperty("start_date_time") String startDateTime,
        @JsonProperty("end_date_time") String endDateTime,
        @JsonProperty("assigned_to_user_id") String assignedToUserId,
        @JsonProperty("call_direction_code") String callDirectionCode,
        @JsonProperty("call_result_code") String callResultCode,
        @JsonProperty("call_duration_seconds") Integer callDurationSeconds
) {
}
