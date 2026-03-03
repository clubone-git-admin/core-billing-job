package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmActivityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for lead activities: timeline, log single, bulk log.
 */
@Service
public class CrmActivityService {

    private static final Logger log = LoggerFactory.getLogger(CrmActivityService.class);
    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final CrmActivityRepository activityRepository;

    public CrmActivityService(CrmActivityRepository activityRepository) {
        this.activityRepository = activityRepository;
    }

    public CrmLeadActivitiesResponse getLeadActivities(UUID leadId, String typeCode, String statusCode, String outcomeCode,
                                                       String from, String to, String search, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        if (!activityRepository.leadExists(orgId, leadId)) {
            return new CrmLeadActivitiesResponse(List.of(), 0L);
        }
        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "LEAD");
        if (entityTypeId == null) {
            return new CrmLeadActivitiesResponse(List.of(), 0L);
        }
        Timestamp fromTs = parseIsoTimestamp(from);
        Timestamp toTs = parseIsoTimestamp(to);
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;

        List<Map<String, Object>> rows = activityRepository.findActivitiesByLead(
                orgId, leadId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search, limitVal, offsetVal);
        long total = activityRepository.countActivitiesByLead(
                orgId, leadId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search);

        List<CrmLeadActivityDto> activities = rows.stream().map(this::mapToActivityDto).toList();
        return new CrmLeadActivitiesResponse(activities, total);
    }

    @Transactional
    public CrmLeadActivityDto logActivity(UUID leadId, CrmLogActivityRequest request) {
        if (request == null) throw new IllegalArgumentException("Request body is required");
        if (request.activityTypeCode() == null || request.activityTypeCode().isBlank())
            throw new IllegalArgumentException("activity_type_code is required");
        if (request.activityStatusCode() == null || request.activityStatusCode().isBlank())
            throw new IllegalArgumentException("activity_status_code is required");
        if (request.activityVisibilityCode() == null || request.activityVisibilityCode().isBlank())
            throw new IllegalArgumentException("activity_visibility_code is required");

        UUID orgId = getOrgClientId();
        if (!activityRepository.leadExists(orgId, leadId))
            throw new IllegalArgumentException("Lead not found: " + leadId);

        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "LEAD");
        if (entityTypeId == null) throw new IllegalStateException("Entity type LEAD not found");

        UUID activityTypeId = activityRepository.resolveActivityTypeIdByCode(orgId, request.activityTypeCode());
        if (activityTypeId == null) throw new IllegalArgumentException("Unknown activity_type_code: " + request.activityTypeCode());
        UUID activityStatusId = activityRepository.resolveActivityStatusIdByCode(orgId, request.activityStatusCode());
        if (activityStatusId == null) throw new IllegalArgumentException("Unknown activity_status_code: " + request.activityStatusCode());
        UUID activityVisibilityId = activityRepository.resolveActivityVisibilityIdByCode(orgId, request.activityVisibilityCode());
        if (activityVisibilityId == null) throw new IllegalArgumentException("Unknown activity_visibility_code: " + request.activityVisibilityCode());
        UUID activityOutcomeId = activityRepository.resolveActivityOutcomeIdByCode(orgId, request.activityOutcomeCode());

        Timestamp startDt = parseIsoTimestamp(request.startDateTime());
        Timestamp endDt = parseIsoTimestamp(request.endDateTime());
        if (startDt != null && endDt != null && endDt.before(startDt))
            throw new IllegalArgumentException("end_date_time must be >= start_date_time");

        UUID assignedTo = request.assignedToUserId() != null ? UUID.fromString(request.assignedToUserId()) : SYSTEM_USER_ID;

        UUID activityId = activityRepository.insertActivity(orgId, entityTypeId, leadId,
                activityTypeId, activityStatusId, activityVisibilityId, activityOutcomeId,
                request.subject(), request.description(), request.notes(),
                startDt, endDt, assignedTo, assignedTo, SYSTEM_USER_ID);

        String typeCode = request.activityTypeCode().toUpperCase();
        if ("CALL".equals(typeCode)) {
            UUID callDirId = request.callDirectionCode() != null ? activityRepository.resolveCallDirectionIdByCode(orgId, request.callDirectionCode()) : null;
            UUID callResultId = request.callResultCode() != null ? activityRepository.resolveCallResultIdByCode(orgId, request.callResultCode()) : null;
            activityRepository.insertActivityCall(activityId, orgId, callDirId, callResultId, request.callDurationSeconds(), SYSTEM_USER_ID);
        } else if ("EMAIL".equals(typeCode)) {
            if (request.emailIdentityId() == null || request.emailIdentityId().isBlank()) {
                throw new IllegalArgumentException("email_identity_id is required for EMAIL activity");
            }
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for email");
            String[] toArr = request.emailTo() != null ? request.emailTo().toArray(new String[0]) : null;
            String[] ccArr = request.emailCc() != null ? request.emailCc().toArray(new String[0]) : null;
            String[] bccArr = request.emailBcc() != null ? request.emailBcc().toArray(new String[0]) : null;
            UUID emailIdentityId = UUID.fromString(request.emailIdentityId());
            UUID emailTemplateId = request.emailTemplateId() != null && !request.emailTemplateId().isBlank()
                    ? UUID.fromString(request.emailTemplateId())
                    : null;
            activityRepository.insertActivityEmail(
                    activityId, orgId, msgProviderId,
                    toArr, ccArr, bccArr,
                    request.emailSubject(), request.emailBody(),
                    emailTemplateId, emailIdentityId,
                    SYSTEM_USER_ID);
        } else if ("EVENT".equals(typeCode)) {
            if (request.eventPurposeId() == null || request.eventPurposeId().isBlank())
                throw new IllegalArgumentException("event_purpose_id is required for EVENT activity");
            if (request.eventStatusId() == null || request.eventStatusId().isBlank())
                throw new IllegalArgumentException("event_status_id is required for EVENT activity");
            if (request.location() == null || request.location().isBlank())
                throw new IllegalArgumentException("location is required for EVENT activity (location/place UUID)");
            UUID eventPurposeId = UUID.fromString(request.eventPurposeId());
            UUID eventStatusId = UUID.fromString(request.eventStatusId());
            String eventPlaceId = request.location();
            activityRepository.insertActivityEvent(activityId, orgId, eventStatusId, eventPlaceId, eventPurposeId, SYSTEM_USER_ID);
            if (request.attendees() != null) {
                for (CrmLogActivityRequest.CrmEventAttendeeDto att : request.attendees()) {
                    UUID rsvpId = att.rsvpStatusCode() != null ? activityRepository.resolveRsvpStatusIdByCode(orgId, att.rsvpStatusCode()) : null;
                    activityRepository.insertActivityEventAttendee(activityId, orgId, att.attendeeName(), att.attendeeEmail(), rsvpId, SYSTEM_USER_ID);
                }
            }
        } else if ("TASK".equals(typeCode)) {
            UUID taskTypeId = activityRepository.resolveTaskTypeIdByCode(orgId, request.taskTypeCode() != null && !request.taskTypeCode().isBlank() ? request.taskTypeCode() : "CALL");
            if (taskTypeId == null) taskTypeId = activityRepository.resolveTaskTypeIdByCode(orgId, "OTHER");
            UUID taskStatusId = activityRepository.resolveTaskStatusIdByCode(orgId, request.taskStatusCode() != null && !request.taskStatusCode().isBlank() ? request.taskStatusCode() : "OPEN");
            UUID taskPriorityId = activityRepository.resolveTaskPriorityIdByCode(orgId, request.taskPriorityCode() != null && !request.taskPriorityCode().isBlank() ? request.taskPriorityCode() : "NORMAL");
            boolean reminderSet = request.reminderSet() != null && request.reminderSet();
            Timestamp reminderTime = parseIsoTimestamp(request.reminderStartDateTime());
            activityRepository.insertActivityTask(
                    activityId, orgId, taskTypeId, taskStatusId, taskPriorityId,
                    reminderSet, reminderTime, null, request.notes(), SYSTEM_USER_ID);
        } else if ("SMS".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for SMS");
            UUID smsTemplateId = request.smsTemplateId() != null && !request.smsTemplateId().isBlank()
                    ? UUID.fromString(request.smsTemplateId())
                    : null;
            activityRepository.insertActivitySms(activityId, orgId, msgProviderId, request.toPhone(), request.body(), smsTemplateId, SYSTEM_USER_ID);
        } else if ("WHATSAPP".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for WhatsApp");
            UUID whatsappTemplateId = request.whatsappTemplateId() != null && !request.whatsappTemplateId().isBlank()
                    ? UUID.fromString(request.whatsappTemplateId())
                    : null;
            activityRepository.insertActivityWhatsapp(activityId, orgId, msgProviderId, request.toPhone(), request.body(), whatsappTemplateId, SYSTEM_USER_ID);
        }

        Map<String, Object> row = activityRepository.findActivityById(orgId, activityId);
        return row != null ? mapToActivityDto(row) : null;
    }

    public CrmBulkActivitiesResponse bulkLogActivities(CrmBulkActivitiesRequest request) {
        if (request == null || request.leadIds() == null || request.leadIds().isEmpty())
            throw new IllegalArgumentException("lead_ids is required and must not be empty");
        if (request.activityTypeCode() == null || request.activityTypeCode().isBlank())
            throw new IllegalArgumentException("activity_type_code is required");
        if (request.activityStatusCode() == null || request.activityStatusCode().isBlank())
            throw new IllegalArgumentException("activity_status_code is required");
        if (request.activityVisibilityCode() == null || request.activityVisibilityCode().isBlank())
            throw new IllegalArgumentException("activity_visibility_code is required");

        List<String> activityIds = new ArrayList<>();
        List<CrmBulkActivitiesResponse.CrmBulkActivityErrorDto> errors = new ArrayList<>();
        UUID orgId = getOrgClientId();

        CrmLogActivityRequest single = new CrmLogActivityRequest(
                request.activityTypeCode(),
                request.activityStatusCode(),
                request.activityVisibilityCode(),
                request.activityOutcomeCode(),
                request.subject(),
                request.description(),
                request.notes(),
                request.startDateTime(),
                request.endDateTime(),
                request.assignedToUserId(),
                request.callDirectionCode(),
                request.callResultCode(),
                request.callDurationSeconds(),
                null, // email_to
                null, // email_cc
                null, // email_bcc
                null, // email_subject
                null, // email_body
                null, // email_identity_id
                null, // email_template_id
                null, // event_purpose_id
                null, // event_status_id
                null, // location
                null, // event_status_code
                null, // location_id
                null, // meeting_purpose
                null, // attendees
                null, // task_type_code
                null, // task_status_code
                null, // task_priority_code
                null, // due_date
                null, // due_time
                null, // reminder_set
                null, // reminder_start_date_time
                null, // to_phone
                null, // body
                null, // sms_template_id
                null  // whatsapp_template_id
        );

        for (String leadIdStr : request.leadIds()) {
            try {
                UUID leadId = UUID.fromString(leadIdStr);
                if (!activityRepository.leadExists(orgId, leadId)) {
                    errors.add(new CrmBulkActivitiesResponse.CrmBulkActivityErrorDto(leadIdStr, "Lead not found"));
                    continue;
                }
                CrmLeadActivityDto created = logActivity(leadId, single);
                if (created != null) activityIds.add(created.activityId());
            } catch (Exception e) {
                log.warn("Bulk activity failed for lead {}: {}", leadIdStr, e.getMessage());
                errors.add(new CrmBulkActivitiesResponse.CrmBulkActivityErrorDto(leadIdStr, e.getMessage()));
            }
        }

        return new CrmBulkActivitiesResponse(activityIds.size(), errors.size(), activityIds, errors.isEmpty() ? null : errors);
    }

    private CrmLeadActivityDto mapToActivityDto(Map<String, Object> row) {
        String callDir = asString(row.get("call_direction_display_name"));
        String callRes = asString(row.get("call_result_display_name"));
        String channelSummary = null;
        if (callDir != null || callRes != null)
            channelSummary = (callDir != null ? callDir : "") + (callRes != null ? (callDir != null ? ", Result: " : "") + callRes : "");

        return new CrmLeadActivityDto(
                asString(row.get("activity_id")),
                asString(row.get("activity_type_code")),
                asString(row.get("activity_type_display_name")),
                asString(row.get("activity_status_code")),
                asString(row.get("activity_status_display_name")),
                asString(row.get("activity_outcome_code")),
                asString(row.get("activity_outcome_display_name")),
                asString(row.get("activity_visibility_code")),
                asString(row.get("activity_visibility_display_name")),
                asString(row.get("subject")),
                asString(row.get("description")),
                asString(row.get("notes")),
                toIsoString(row.get("start_date_time")),
                toIsoString(row.get("end_date_time")),
                asString(row.get("owner_user_id")),
                asString(row.get("owner_user_display_name")),
                asString(row.get("assigned_to_user_id")),
                asString(row.get("assigned_to_user_display_name")),
                channelSummary,
                asString(row.get("call_direction_code")),
                asString(row.get("call_direction_display_name")),
                asString(row.get("call_result_code")),
                asString(row.get("call_result_display_name")),
                asInteger(row.get("call_duration_seconds")),
                asString(row.get("email_delivery_status_code")),
                asString(row.get("email_delivery_status_display_name")),
                asString(row.get("sms_delivery_status_code")),
                asString(row.get("sms_delivery_status_display_name")),
                asString(row.get("whatsapp_delivery_status_code")),
                asString(row.get("whatsapp_delivery_status_display_name")),
                asString(row.get("event_status_code")),
                asString(row.get("event_status_display_name")),
                asString(row.get("task_type_code")),
                asString(row.get("task_type_display_name")),
                asString(row.get("task_status_code")),
                asString(row.get("task_status_display_name")),
                asString(row.get("task_priority_code")),
                asString(row.get("task_priority_display_name"))
        );
    }

    private static Timestamp parseIsoTimestamp(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Timestamp.from(Instant.parse(value.replace("Z", "+00:00")));
        } catch (Exception e) {
            return null;
        }
    }

    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp ts) return ts.toInstant().toString();
        return value.toString();
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Integer asInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.intValue();
        try { return Integer.parseInt(value.toString()); } catch (Exception e) { return null; }
    }

    private UUID getOrgClientId() {
        return DEFAULT_ORG_CLIENT_ID;
    }
}
