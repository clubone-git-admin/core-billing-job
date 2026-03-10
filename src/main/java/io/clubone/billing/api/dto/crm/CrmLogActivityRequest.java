package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Request for POST /api/crm/leads/{lead_id}/activities.
 * Base fields plus optional channel-specific (call, email, event, task, sms, whatsapp).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLogActivityRequest(
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
        // Call
        @JsonProperty("call_direction_code") String callDirectionCode,
        @JsonProperty("call_result_code") String callResultCode,
        @JsonProperty("call_duration_seconds") Integer callDurationSeconds,
        // Email
        @JsonProperty("email_to") List<String> emailTo,
        @JsonProperty("email_cc") List<String> emailCc,
        @JsonProperty("email_bcc") List<String> emailBcc,
        @JsonProperty("email_subject") String emailSubject,
        @JsonProperty("email_body") String emailBody,
        @JsonProperty("email_identity_id") String emailIdentityId,
        @JsonProperty("email_template_id") String emailTemplateId,
        // Event (event_purpose_id, event_status_id, location are mandatory for EVENT type)
        @JsonProperty("event_purpose_id") String eventPurposeId,
        @JsonProperty("event_status_id") String eventStatusId,
        @JsonProperty("location") String location,
        @JsonProperty("event_status_code") String eventStatusCode,
        @JsonProperty("location_id") String locationId,
        @JsonProperty("meeting_purpose") String meetingPurpose,
        @JsonProperty("attendees") List<CrmEventAttendeeDto> attendees,
        // Task
        @JsonProperty("task_type_code") String taskTypeCode,
        @JsonProperty("task_status_code") String taskStatusCode,
        @JsonProperty("task_priority_code") String taskPriorityCode,
        @JsonProperty("due_date") String dueDate,
        @JsonProperty("due_time") String dueTime,
        @JsonProperty("reminder_set") Boolean reminderSet,
        @JsonProperty("reminder_start_date_time") String reminderStartDateTime,
        // SMS / WhatsApp
        @JsonProperty("to_phone") String toPhone,
        @JsonProperty("body") String body,
        @JsonProperty("sms_template_id") String smsTemplateId,
        @JsonProperty("sms_identity_id") String smsIdentityId,
        @JsonProperty("whatsapp_template_id") String whatsappTemplateId,
        @JsonProperty("whatsapp_identity_id") String whatsappIdentityId,
        // Notification (for EMAIL activity → notification job/send)
        @JsonProperty("notification_template_code") String notificationTemplateCode,
        /** Dynamic key-value map for notification common params (e.g. brandName, clubName, fromEmail, etc.). */
        @JsonProperty("common_params") Map<String, Object> commonParams,
        @JsonProperty("recipients") List<NotificationRecipientDto> recipients
) {
    public record CrmEventAttendeeDto(
            @JsonProperty("attendee_name") String attendeeName,
            @JsonProperty("attendee_email") String attendeeEmail,
            @JsonProperty("rsvp_status_code") String rsvpStatusCode
    ) {}

    /** Recipient for notification job (from request body). params is dynamic by template placeholders. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record NotificationRecipientDto(
            @JsonProperty("client_id") String clientId,
            @JsonProperty("to") NotificationRecipientToDto to,
            /** Dynamic key-value map for template placeholders (e.g. firstName, lastName, displayName, expiryDate, etc.). */
            @JsonProperty("params") Map<String, Object> params
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record NotificationRecipientToDto(
            @JsonProperty("email") String email,
            @JsonProperty("phone") String phone
    ) {}
}
