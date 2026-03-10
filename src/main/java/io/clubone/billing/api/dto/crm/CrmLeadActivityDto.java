package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Single activity item for lead timeline and log-activity response.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadActivityDto(
        @JsonProperty("activity_id") String activityId,
        @JsonProperty("activity_type_code") String activityTypeCode,
        @JsonProperty("activity_type_display_name") String activityTypeDisplayName,
        @JsonProperty("activity_status_code") String activityStatusCode,
        @JsonProperty("activity_status_display_name") String activityStatusDisplayName,
        @JsonProperty("activity_outcome_code") String activityOutcomeCode,
        @JsonProperty("activity_outcome_display_name") String activityOutcomeDisplayName,
        @JsonProperty("activity_visibility_code") String activityVisibilityCode,
        @JsonProperty("activity_visibility_display_name") String activityVisibilityDisplayName,
        @JsonProperty("subject") String subject,
        @JsonProperty("description") String description,
        @JsonProperty("notes") String notes,
        @JsonProperty("start_date_time") String startDateTime,
        @JsonProperty("end_date_time") String endDateTime,
        @JsonProperty("owner_user_id") String ownerUserId,
        @JsonProperty("owner_user_display_name") String ownerUserDisplayName,
        @JsonProperty("assigned_to_user_id") String assignedToUserId,
        @JsonProperty("assigned_to_user_display_name") String assignedToUserDisplayName,
        @JsonProperty("channel_summary") String channelSummary,
        @JsonProperty("call_direction_code") String callDirectionCode,
        @JsonProperty("call_direction_display_name") String callDirectionDisplayName,
        @JsonProperty("call_result_code") String callResultCode,
        @JsonProperty("call_result_display_name") String callResultDisplayName,
        @JsonProperty("call_duration_seconds") Integer callDurationSeconds,
        @JsonProperty("email_delivery_status_code") String emailDeliveryStatusCode,
        @JsonProperty("email_delivery_status_display_name") String emailDeliveryStatusDisplayName,
        @JsonProperty("sms_delivery_status_code") String smsDeliveryStatusCode,
        @JsonProperty("sms_delivery_status_display_name") String smsDeliveryStatusDisplayName,
        @JsonProperty("whatsapp_delivery_status_code") String whatsappDeliveryStatusCode,
        @JsonProperty("whatsapp_delivery_status_display_name") String whatsappDeliveryStatusDisplayName,
        @JsonProperty("event_status_code") String eventStatusCode,
        @JsonProperty("event_status_display_name") String eventStatusDisplayName,
        @JsonProperty("event_purpose_display_name") String eventPurposeDisplayName,
        @JsonProperty("location") String location,
        @JsonProperty("task_type_code") String taskTypeCode,
        @JsonProperty("task_type_display_name") String taskTypeDisplayName,
        @JsonProperty("task_status_code") String taskStatusCode,
        @JsonProperty("task_status_display_name") String taskStatusDisplayName,
        @JsonProperty("task_priority_code") String taskPriorityCode,
        @JsonProperty("task_priority_display_name") String taskPriorityDisplayName
) {
}
