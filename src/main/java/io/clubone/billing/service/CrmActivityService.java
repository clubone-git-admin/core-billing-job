package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.api.dto.notification.NotificationJobRequest;
import io.clubone.billing.repo.CrmActivityRepository;
import io.clubone.billing.repo.CrmOpportunityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for lead activities: timeline, log single, bulk log.
 */
@Service
public class CrmActivityService {

    private static final Logger log = LoggerFactory.getLogger(CrmActivityService.class);
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 500;

    private final CrmActivityRepository activityRepository;
    private final CrmLeadService leadService;
    private final CrmContactService contactService;
    private final CrmOpportunityRepository opportunityRepository;
    private final NotificationClient notificationClient;
    private final LeadWarmthService leadWarmthService;
    private final CrmRequestContext context;

    public CrmActivityService(CrmActivityRepository activityRepository, CrmLeadService leadService, CrmContactService contactService, CrmOpportunityRepository opportunityRepository, NotificationClient notificationClient, LeadWarmthService leadWarmthService, CrmRequestContext context) {
        this.activityRepository = activityRepository;
        this.leadService = leadService;
        this.contactService = contactService;
        this.opportunityRepository = opportunityRepository;
        this.notificationClient = notificationClient;
        this.leadWarmthService = leadWarmthService;
        this.context = context;
    }

    public CrmLeadActivitiesResponse getLeadActivities(UUID leadId, String typeCode, String statusCode, String outcomeCode,
                                                       String from, String to, String search, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
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

    public CrmLeadActivitiesResponse getContactActivities(UUID contactId, String typeCode, String statusCode, String outcomeCode,
                                                          String from, String to, String search, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        if (!activityRepository.contactExists(orgId, contactId)) {
            return new CrmLeadActivitiesResponse(List.of(), 0L);
        }
        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "CONTACT");
        if (entityTypeId == null) {
            return new CrmLeadActivitiesResponse(List.of(), 0L);
        }
        Timestamp fromTs = parseIsoTimestamp(from);
        Timestamp toTs = parseIsoTimestamp(to);
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = activityRepository.findActivitiesByLead(
                orgId, contactId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search, limitVal, offsetVal);
        long total = activityRepository.countActivitiesByLead(
                orgId, contactId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search);
        List<CrmLeadActivityDto> activities = rows.stream().map(this::mapToActivityDto).toList();
        return new CrmLeadActivitiesResponse(activities, total);
    }

    public CrmLeadActivitiesResponse getOpportunityActivities(UUID opportunityId, String typeCode, String statusCode, String outcomeCode,
                                                              String from, String to, String search, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        if (!activityRepository.opportunityExists(orgId, opportunityId)) {
            return null;
        }
        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "OPPORTUNITY");
        if (entityTypeId == null) {
            return null;
        }
        Timestamp fromTs = parseIsoTimestamp(from);
        Timestamp toTs = parseIsoTimestamp(to);
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = activityRepository.findActivitiesByLead(
                orgId, opportunityId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search, limitVal, offsetVal);
        long total = activityRepository.countActivitiesByLead(
                orgId, opportunityId, entityTypeId, typeCode, statusCode, outcomeCode, fromTs, toTs, search);
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

        UUID orgId = context.getOrgClientId();
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

        UUID assignedTo = request.assignedToUserId() != null ? UUID.fromString(request.assignedToUserId()) : context.getActorId();

        UUID activityId = activityRepository.insertActivity(orgId, entityTypeId, leadId,
                activityTypeId, activityStatusId, activityVisibilityId, activityOutcomeId,
                request.subject(), request.description(), request.notes(),
                startDt, endDt, assignedTo, assignedTo, context.getActorId());

        String typeCode = request.activityTypeCode().toUpperCase();
        if ("CALL".equals(typeCode)) {
            UUID callDirId = request.callDirectionCode() != null ? activityRepository.resolveCallDirectionIdByCode(orgId, request.callDirectionCode()) : null;
            UUID callResultId = request.callResultCode() != null ? activityRepository.resolveCallResultIdByCode(orgId, request.callResultCode()) : null;
            activityRepository.insertActivityCall(activityId, orgId, callDirId, callResultId, request.callDurationSeconds(), context.getActorId());
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
                    context.getActorId());
            sendNotificationJobForLeadEmail(leadId, request);
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
            activityRepository.insertActivityEvent(activityId, orgId, eventStatusId, eventPlaceId, eventPurposeId, context.getActorId());
            if (request.attendees() != null) {
                for (CrmLogActivityRequest.CrmEventAttendeeDto att : request.attendees()) {
                    UUID rsvpId = att.rsvpStatusCode() != null ? activityRepository.resolveRsvpStatusIdByCode(orgId, att.rsvpStatusCode()) : null;
                    activityRepository.insertActivityEventAttendee(activityId, orgId, att.attendeeName(), att.attendeeEmail(), rsvpId, context.getActorId());
                }
            }
        } else if ("TASK".equals(typeCode)) {
            UUID taskTypeId = activityRepository.resolveTaskTypeId(orgId, request.taskTypeCode() != null && !request.taskTypeCode().isBlank() ? request.taskTypeCode() : "CALL");
            if (taskTypeId == null) taskTypeId = activityRepository.resolveTaskTypeIdByCode(orgId, "OTHER");
            UUID taskStatusId = activityRepository.resolveTaskStatusId(orgId, request.taskStatusCode() != null && !request.taskStatusCode().isBlank() ? request.taskStatusCode() : "OPEN");
            UUID taskPriorityId = activityRepository.resolveTaskPriorityId(orgId, request.taskPriorityCode() != null && !request.taskPriorityCode().isBlank() ? request.taskPriorityCode() : "NORMAL");
            if (taskTypeId == null) throw new IllegalArgumentException("Invalid or inactive task_type_code / task_type_id");
            if (taskStatusId == null) throw new IllegalArgumentException("Invalid or inactive task_status_code / task_status_id");
            if (taskPriorityId == null) throw new IllegalArgumentException("Invalid or inactive task_priority_code / task_priority_id");
            boolean reminderSet = request.reminderSet() != null && request.reminderSet();
            Timestamp reminderTime = parseIsoTimestamp(request.reminderStartDateTime());
            activityRepository.insertActivityTask(
                    activityId, orgId, taskTypeId, taskStatusId, taskPriorityId,
                    reminderSet, reminderTime, null, request.notes(), context.getActorId());
        } else if ("SMS".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for SMS");
            UUID smsTemplateId = request.smsTemplateId() != null && !request.smsTemplateId().isBlank()
                    ? UUID.fromString(request.smsTemplateId())
                    : null;
            UUID smsIdentityId = request.smsIdentityId() != null && !request.smsIdentityId().isBlank()
                    ? UUID.fromString(request.smsIdentityId())
                    : null;
            activityRepository.insertActivitySms(activityId, orgId, msgProviderId, request.toPhone(), request.body(), smsTemplateId, smsIdentityId, context.getActorId());
            sendNotificationJobForLeadSms(leadId, request);
        } else if ("WHATSAPP".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for WhatsApp");
            UUID whatsappTemplateId = request.whatsappTemplateId() != null && !request.whatsappTemplateId().isBlank()
                    ? UUID.fromString(request.whatsappTemplateId())
                    : null;
            UUID whatsappIdentityId = request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank()
                    ? UUID.fromString(request.whatsappIdentityId())
                    : null;
            activityRepository.insertActivityWhatsapp(activityId, orgId, msgProviderId, request.toPhone(), request.body(), whatsappTemplateId, whatsappIdentityId, context.getActorId());
            sendNotificationJobForLeadWhatsapp(leadId, request);
        }

        leadWarmthService.recalculateWarmthForLead(leadId);

        Map<String, Object> row = activityRepository.findActivityById(orgId, activityId);
        return row != null ? mapToActivityDto(row) : null;
    }

    @Transactional
    public CrmLeadActivityDto logActivityForContact(UUID contactId, CrmLogActivityRequest request) {
        if (request == null) throw new IllegalArgumentException("Request body is required");
        if (request.activityTypeCode() == null || request.activityTypeCode().isBlank())
            throw new IllegalArgumentException("activity_type_code is required");
        if (request.activityStatusCode() == null || request.activityStatusCode().isBlank())
            throw new IllegalArgumentException("activity_status_code is required");
        if (request.activityVisibilityCode() == null || request.activityVisibilityCode().isBlank())
            throw new IllegalArgumentException("activity_visibility_code is required");
        UUID orgId = context.getOrgClientId();
        if (!activityRepository.contactExists(orgId, contactId))
            throw new IllegalArgumentException("Contact not found: " + contactId);
        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "CONTACT");
        if (entityTypeId == null) throw new IllegalStateException("Entity type CONTACT not found");
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
        UUID assignedTo = request.assignedToUserId() != null ? UUID.fromString(request.assignedToUserId()) : context.getActorId();
        UUID activityId = activityRepository.insertActivity(orgId, entityTypeId, contactId,
                activityTypeId, activityStatusId, activityVisibilityId, activityOutcomeId,
                request.subject(), request.description(), request.notes(),
                startDt, endDt, assignedTo, assignedTo, context.getActorId());
        String typeCode = request.activityTypeCode().toUpperCase();
        if ("CALL".equals(typeCode)) {
            UUID callDirId = request.callDirectionCode() != null ? activityRepository.resolveCallDirectionIdByCode(orgId, request.callDirectionCode()) : null;
            UUID callResultId = request.callResultCode() != null ? activityRepository.resolveCallResultIdByCode(orgId, request.callResultCode()) : null;
            activityRepository.insertActivityCall(activityId, orgId, callDirId, callResultId, request.callDurationSeconds(), context.getActorId());
        } else if ("EMAIL".equals(typeCode)) {
            if (request.emailIdentityId() == null || request.emailIdentityId().isBlank())
                throw new IllegalArgumentException("email_identity_id is required for EMAIL activity");
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for email");
            String[] toArr = request.emailTo() != null ? request.emailTo().toArray(new String[0]) : null;
            String[] ccArr = request.emailCc() != null ? request.emailCc().toArray(new String[0]) : null;
            String[] bccArr = request.emailBcc() != null ? request.emailBcc().toArray(new String[0]) : null;
            UUID emailIdentityId = UUID.fromString(request.emailIdentityId());
            UUID emailTemplateId = request.emailTemplateId() != null && !request.emailTemplateId().isBlank() ? UUID.fromString(request.emailTemplateId()) : null;
            activityRepository.insertActivityEmail(activityId, orgId, msgProviderId, toArr, ccArr, bccArr, request.emailSubject(), request.emailBody(), emailTemplateId, emailIdentityId, context.getActorId());
            sendNotificationJobForContactEmail(contactId, request);
        } else if ("EVENT".equals(typeCode)) {
            if (request.eventPurposeId() == null || request.eventPurposeId().isBlank()) throw new IllegalArgumentException("event_purpose_id is required for EVENT activity");
            if (request.eventStatusId() == null || request.eventStatusId().isBlank()) throw new IllegalArgumentException("event_status_id is required for EVENT activity");
            if (request.location() == null || request.location().isBlank()) throw new IllegalArgumentException("location is required for EVENT activity");
            UUID eventPurposeId = UUID.fromString(request.eventPurposeId());
            UUID eventStatusId = UUID.fromString(request.eventStatusId());
            String eventPlaceId = request.location();
            activityRepository.insertActivityEvent(activityId, orgId, eventStatusId, eventPlaceId, eventPurposeId, context.getActorId());
            if (request.attendees() != null) {
                for (CrmLogActivityRequest.CrmEventAttendeeDto att : request.attendees()) {
                    UUID rsvpId = att.rsvpStatusCode() != null ? activityRepository.resolveRsvpStatusIdByCode(orgId, att.rsvpStatusCode()) : null;
                    activityRepository.insertActivityEventAttendee(activityId, orgId, att.attendeeName(), att.attendeeEmail(), rsvpId, context.getActorId());
                }
            }
        } else if ("TASK".equals(typeCode)) {
            UUID taskTypeId = activityRepository.resolveTaskTypeId(orgId, request.taskTypeCode() != null && !request.taskTypeCode().isBlank() ? request.taskTypeCode() : "CALL");
            if (taskTypeId == null) taskTypeId = activityRepository.resolveTaskTypeIdByCode(orgId, "OTHER");
            UUID taskStatusId = activityRepository.resolveTaskStatusId(orgId, request.taskStatusCode() != null && !request.taskStatusCode().isBlank() ? request.taskStatusCode() : "OPEN");
            UUID taskPriorityId = activityRepository.resolveTaskPriorityId(orgId, request.taskPriorityCode() != null && !request.taskPriorityCode().isBlank() ? request.taskPriorityCode() : "NORMAL");
            if (taskTypeId == null) throw new IllegalArgumentException("Invalid or inactive task_type_code / task_type_id");
            if (taskStatusId == null) throw new IllegalArgumentException("Invalid or inactive task_status_code / task_status_id");
            if (taskPriorityId == null) throw new IllegalArgumentException("Invalid or inactive task_priority_code / task_priority_id");
            boolean reminderSet = request.reminderSet() != null && request.reminderSet();
            Timestamp reminderTime = parseIsoTimestamp(request.reminderStartDateTime());
            activityRepository.insertActivityTask(activityId, orgId, taskTypeId, taskStatusId, taskPriorityId, reminderSet, reminderTime, null, request.notes(), context.getActorId());
        } else if ("SMS".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for SMS");
            UUID smsTemplateId = request.smsTemplateId() != null && !request.smsTemplateId().isBlank() ? UUID.fromString(request.smsTemplateId()) : null;
            UUID smsIdentityId = request.smsIdentityId() != null && !request.smsIdentityId().isBlank() ? UUID.fromString(request.smsIdentityId()) : null;
            activityRepository.insertActivitySms(activityId, orgId, msgProviderId, request.toPhone(), request.body(), smsTemplateId, smsIdentityId, context.getActorId());
            sendNotificationJobForContactSms(contactId, request);
        } else if ("WHATSAPP".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for WhatsApp");
            UUID whatsappTemplateId = request.whatsappTemplateId() != null && !request.whatsappTemplateId().isBlank() ? UUID.fromString(request.whatsappTemplateId()) : null;
            UUID whatsappIdentityId = request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank() ? UUID.fromString(request.whatsappIdentityId()) : null;
            activityRepository.insertActivityWhatsapp(activityId, orgId, msgProviderId, request.toPhone(), request.body(), whatsappTemplateId, whatsappIdentityId, context.getActorId());
            sendNotificationJobForContactWhatsapp(contactId, request);
        }
        Map<String, Object> row = activityRepository.findActivityById(orgId, activityId);
        return row != null ? mapToActivityDto(row) : null;
    }

    @Transactional
    public CrmLeadActivityDto logActivityForOpportunity(UUID opportunityId, CrmLogActivityRequest request) {
        if (request == null) throw new IllegalArgumentException("Request body is required");
        if (request.activityTypeCode() == null || request.activityTypeCode().isBlank())
            throw new IllegalArgumentException("activity_type_code is required");
        if (request.activityStatusCode() == null || request.activityStatusCode().isBlank())
            throw new IllegalArgumentException("activity_status_code is required");
        if (request.activityVisibilityCode() == null || request.activityVisibilityCode().isBlank())
            throw new IllegalArgumentException("activity_visibility_code is required");
        UUID orgId = context.getOrgClientId();
        if (!activityRepository.opportunityExists(orgId, opportunityId))
            throw new IllegalArgumentException("Opportunity not found: " + opportunityId);
        UUID entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "OPPORTUNITY");
        if (entityTypeId == null) throw new IllegalStateException("Entity type OPPORTUNITY not found");
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
        UUID assignedTo = request.assignedToUserId() != null ? UUID.fromString(request.assignedToUserId()) : context.getActorId();
        UUID activityId = activityRepository.insertActivity(orgId, entityTypeId, opportunityId,
                activityTypeId, activityStatusId, activityVisibilityId, activityOutcomeId,
                request.subject(), request.description(), request.notes(),
                startDt, endDt, assignedTo, assignedTo, context.getActorId());
        String typeCode = request.activityTypeCode().toUpperCase();
        if ("CALL".equals(typeCode)) {
            UUID callDirId = request.callDirectionCode() != null ? activityRepository.resolveCallDirectionIdByCode(orgId, request.callDirectionCode()) : null;
            UUID callResultId = request.callResultCode() != null ? activityRepository.resolveCallResultIdByCode(orgId, request.callResultCode()) : null;
            activityRepository.insertActivityCall(activityId, orgId, callDirId, callResultId, request.callDurationSeconds(), context.getActorId());
        } else if ("EMAIL".equals(typeCode)) {
            if (request.emailIdentityId() == null || request.emailIdentityId().isBlank())
                throw new IllegalArgumentException("email_identity_id is required for EMAIL activity");
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for email");
            String[] toArr = request.emailTo() != null ? request.emailTo().toArray(new String[0]) : null;
            String[] ccArr = request.emailCc() != null ? request.emailCc().toArray(new String[0]) : null;
            String[] bccArr = request.emailBcc() != null ? request.emailBcc().toArray(new String[0]) : null;
            UUID emailIdentityId = UUID.fromString(request.emailIdentityId());
            UUID emailTemplateId = request.emailTemplateId() != null && !request.emailTemplateId().isBlank() ? UUID.fromString(request.emailTemplateId()) : null;
            activityRepository.insertActivityEmail(activityId, orgId, msgProviderId, toArr, ccArr, bccArr, request.emailSubject(), request.emailBody(), emailTemplateId, emailIdentityId, context.getActorId());
            sendNotificationJobForOpportunityEmail(opportunityId, request);
        } else if ("EVENT".equals(typeCode)) {
            if (request.eventPurposeId() == null || request.eventPurposeId().isBlank()) throw new IllegalArgumentException("event_purpose_id is required for EVENT activity");
            if (request.eventStatusId() == null || request.eventStatusId().isBlank()) throw new IllegalArgumentException("event_status_id is required for EVENT activity");
            if (request.location() == null || request.location().isBlank()) throw new IllegalArgumentException("location is required for EVENT activity");
            UUID eventPurposeId = UUID.fromString(request.eventPurposeId());
            UUID eventStatusId = UUID.fromString(request.eventStatusId());
            String eventPlaceId = request.location();
            activityRepository.insertActivityEvent(activityId, orgId, eventStatusId, eventPlaceId, eventPurposeId, context.getActorId());
            if (request.attendees() != null) {
                for (CrmLogActivityRequest.CrmEventAttendeeDto att : request.attendees()) {
                    UUID rsvpId = att.rsvpStatusCode() != null ? activityRepository.resolveRsvpStatusIdByCode(orgId, att.rsvpStatusCode()) : null;
                    activityRepository.insertActivityEventAttendee(activityId, orgId, att.attendeeName(), att.attendeeEmail(), rsvpId, context.getActorId());
                }
            }
        } else if ("TASK".equals(typeCode)) {
            UUID taskTypeId = activityRepository.resolveTaskTypeId(orgId, request.taskTypeCode() != null && !request.taskTypeCode().isBlank() ? request.taskTypeCode() : "CALL");
            if (taskTypeId == null) taskTypeId = activityRepository.resolveTaskTypeIdByCode(orgId, "OTHER");
            UUID taskStatusId = activityRepository.resolveTaskStatusId(orgId, request.taskStatusCode() != null && !request.taskStatusCode().isBlank() ? request.taskStatusCode() : "OPEN");
            UUID taskPriorityId = activityRepository.resolveTaskPriorityId(orgId, request.taskPriorityCode() != null && !request.taskPriorityCode().isBlank() ? request.taskPriorityCode() : "NORMAL");
            if (taskTypeId == null) throw new IllegalArgumentException("Invalid or inactive task_type_code / task_type_id");
            if (taskStatusId == null) throw new IllegalArgumentException("Invalid or inactive task_status_code / task_status_id");
            if (taskPriorityId == null) throw new IllegalArgumentException("Invalid or inactive task_priority_code / task_priority_id");
            boolean reminderSet = request.reminderSet() != null && request.reminderSet();
            Timestamp reminderTime = parseIsoTimestamp(request.reminderStartDateTime());
            activityRepository.insertActivityTask(activityId, orgId, taskTypeId, taskStatusId, taskPriorityId, reminderSet, reminderTime, null, request.notes(), context.getActorId());
        } else if ("SMS".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for SMS");
            String body = request.body() != null ? request.body() : request.description();
            if (body == null || body.isBlank()) throw new IllegalArgumentException("description or body is required for SMS activity");
            UUID smsTemplateId = request.smsTemplateId() != null && !request.smsTemplateId().isBlank() ? UUID.fromString(request.smsTemplateId()) : null;
            UUID smsIdentityId = request.smsIdentityId() != null && !request.smsIdentityId().isBlank() ? UUID.fromString(request.smsIdentityId()) : null;
            activityRepository.insertActivitySms(activityId, orgId, msgProviderId, request.toPhone(), body, smsTemplateId, smsIdentityId, context.getActorId());
            sendNotificationJobForOpportunitySms(opportunityId, request);
        } else if ("WHATSAPP".equals(typeCode)) {
            UUID msgProviderId = activityRepository.resolveMessageProviderId(orgId);
            if (msgProviderId == null) throw new IllegalStateException("No message provider configured for WhatsApp");
            String body = request.body() != null ? request.body() : request.description();
            if (body == null || body.isBlank()) throw new IllegalArgumentException("description or body is required for WhatsApp activity");
            UUID whatsappTemplateId = request.whatsappTemplateId() != null && !request.whatsappTemplateId().isBlank() ? UUID.fromString(request.whatsappTemplateId()) : null;
            UUID whatsappIdentityId = request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank() ? UUID.fromString(request.whatsappIdentityId()) : null;
            activityRepository.insertActivityWhatsapp(activityId, orgId, msgProviderId, request.toPhone(), body, whatsappTemplateId, whatsappIdentityId, context.getActorId());
            sendNotificationJobForOpportunityWhatsapp(opportunityId, request);
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
        UUID orgId = context.getOrgClientId();

        CrmLogActivityRequest single = toLogActivityRequest(request);

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
                toTimestampStringAsStored(row.get("start_date_time")),
                toTimestampStringAsStored(row.get("end_date_time")),
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
                asString(row.get("event_purpose_display_name")),
                asString(row.get("event_purpose_display_name")),
                asString(row.get("location")),
                asString(row.get("task_type_code")),
                asString(row.get("task_type_display_name")),
                asString(row.get("task_status_code")),
                asString(row.get("task_status_display_name")),
                asString(row.get("task_priority_code")),
                asString(row.get("task_priority_display_name")),
                asString(row.get("channel_from")),
                asString(row.get("channel_template"))
        );
    }

    /** Accepts ISO 8601 date-time with any offset (e.g. IST +05:30); stores the instant. Used for start_date_time, end_date_time, reminder_start_date_time. */
    private static Timestamp parseIsoTimestamp(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Timestamp.from(Instant.parse(value.replace("Z", "+00:00")));
        } catch (Exception e) {
            return null;
        }
    }

    /** Formats timestamp as in DB / server local time (no UTC conversion). Used for activity start_date_time, end_date_time. */
    private static String toTimestampStringAsStored(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp ts) return ts.toLocalDateTime().toString();
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

    /**
     * For EMAIL activity: call notification service job/send.
     * When template is selected: pass template only, do not pass channelContent (null).
     * When template is null: pass channelContent.EMAIL (subject + body); do not pass template.
     */
    private void sendNotificationJobForLeadEmail(UUID leadId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;

        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequest(request, leadId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for lead {}: no recipients (provide recipients in request or email_to/lead email)", leadId);
            return;
        }

        Map<String, Object> commonParams = request.commonParams() != null && !request.commonParams().isEmpty()
                ? request.commonParams()
                : null;

        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;

        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            /* Only set channelContent when no template; when template is selected, channelContent stays null */
            Map<String, Object> emailBody = new LinkedHashMap<>();
            emailBody.put("format", "HTML");
            emailBody.put("value", request.emailBody() != null ? request.emailBody() : "");
            Map<String, Object> emailContent = new LinkedHashMap<>();
            emailContent.put("subject", request.emailSubject() != null ? request.emailSubject() : "");
            emailContent.put("body", emailBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("EMAIL", emailContent);
        }

        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("EMAIL"),
                templateSpec,
                commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /**
     * For SMS activity: call notification service job/send.
     * When template is selected: pass template only, do not pass channelContent (null).
     * When template is null: pass channelContent.SMS (body format "TEXT"); do not pass template.
     */
    private void sendNotificationJobForLeadSms(UUID leadId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;

        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForPhone(request, leadId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for lead {} SMS: no recipients (provide recipients with phone or to_phone/lead phone)", leadId);
            return;
        }

        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.smsIdentityId() != null && !request.smsIdentityId().isBlank()) {
            try {
                UUID smsIdentityId = UUID.fromString(request.smsIdentityId().trim());
                String fromPhoneNo = activityRepository.getSmsIdentitySenderId(context.getOrgClientId(), smsIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve sender_id for sms_identity_id {}: {}", request.smsIdentityId(), e.getMessage());
            }
        }

        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;

        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            /* Only set channelContent when no template; when template is selected, channelContent stays null */
            String smsText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> smsBody = new LinkedHashMap<>();
            smsBody.put("format", "TEXT");
            smsBody.put("value", smsText);
            Map<String, Object> smsContent = new LinkedHashMap<>();
            smsContent.put("body", smsBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("SMS", smsContent);
        }

        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("SMS"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /**
     * For WhatsApp activity: call notification service job/send.
     * When template is selected: pass template only, do not pass channelContent (null).
     * When template is null: pass channelContent.WHATSAPP (body format "TEXT"); do not pass template.
     */
    private void sendNotificationJobForLeadWhatsapp(UUID leadId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;

        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForPhone(request, leadId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for lead {} WhatsApp: no recipients (provide recipients with phone or to_phone/lead phone)", leadId);
            return;
        }

        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank()) {
            try {
                UUID whatsappIdentityId = UUID.fromString(request.whatsappIdentityId().trim());
                String fromPhoneNo = activityRepository.getWhatsappIdentityPhoneNumber(context.getOrgClientId(), whatsappIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve phone_number for whatsapp_identity_id {}: {}", request.whatsappIdentityId(), e.getMessage());
            }
        }

        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;

        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            /* Only set channelContent when no template; when template is selected, channelContent stays null */
            String whatsappText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> whatsappBody = new LinkedHashMap<>();
            whatsappBody.put("format", "TEXT");
            whatsappBody.put("value", whatsappText);
            Map<String, Object> whatsappContent = new LinkedHashMap<>();
            whatsappContent.put("body", whatsappBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("WHATSAPP", whatsappContent);
        }

        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("WHATSAPP"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** For EMAIL activity on contact: same as lead (template vs channelContent, commonParams, recipients from contact). */
    private void sendNotificationJobForContactEmail(UUID contactId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;
        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForContact(request, contactId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for contact {}: no recipients (provide recipients in request or email_to/contact email)", contactId);
            return;
        }
        Map<String, Object> commonParams = request.commonParams() != null && !request.commonParams().isEmpty()
                ? request.commonParams()
                : null;
        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;
        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            Map<String, Object> emailBody = new LinkedHashMap<>();
            emailBody.put("format", "HTML");
            emailBody.put("value", request.emailBody() != null ? request.emailBody() : "");
            Map<String, Object> emailContent = new LinkedHashMap<>();
            emailContent.put("subject", request.emailSubject() != null ? request.emailSubject() : "");
            emailContent.put("body", emailBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("EMAIL", emailContent);
        }
        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("EMAIL"),
                templateSpec,
                commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** For SMS activity on contact: same as lead (template vs channelContent, commonParams + FromPhoneNo, recipients from contact). */
    private void sendNotificationJobForContactSms(UUID contactId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;
        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForPhoneContact(request, contactId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for contact {} SMS: no recipients (provide recipients with phone or to_phone/contact phone)", contactId);
            return;
        }
        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.smsIdentityId() != null && !request.smsIdentityId().isBlank()) {
            try {
                UUID smsIdentityId = UUID.fromString(request.smsIdentityId().trim());
                String fromPhoneNo = activityRepository.getSmsIdentitySenderId(context.getOrgClientId(), smsIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve sender_id for sms_identity_id {}: {}", request.smsIdentityId(), e.getMessage());
            }
        }
        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;
        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            String smsText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> smsBody = new LinkedHashMap<>();
            smsBody.put("format", "TEXT");
            smsBody.put("value", smsText);
            Map<String, Object> smsContent = new LinkedHashMap<>();
            smsContent.put("body", smsBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("SMS", smsContent);
        }
        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("SMS"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** For WhatsApp activity on contact: same as lead (template vs channelContent, commonParams + FromPhoneNo, recipients from contact). */
    private void sendNotificationJobForContactWhatsapp(UUID contactId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;
        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForPhoneContact(request, contactId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for contact {} WhatsApp: no recipients (provide recipients with phone or to_phone/contact phone)", contactId);
            return;
        }
        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank()) {
            try {
                UUID whatsappIdentityId = UUID.fromString(request.whatsappIdentityId().trim());
                String fromPhoneNo = activityRepository.getWhatsappIdentityPhoneNumber(context.getOrgClientId(), whatsappIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve phone_number for whatsapp_identity_id {}: {}", request.whatsappIdentityId(), e.getMessage());
            }
        }
        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;
        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            String whatsappText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> whatsappBody = new LinkedHashMap<>();
            whatsappBody.put("format", "TEXT");
            whatsappBody.put("value", whatsappText);
            Map<String, Object> whatsappContent = new LinkedHashMap<>();
            whatsappContent.put("body", whatsappBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("WHATSAPP", whatsappContent);
        }
        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("WHATSAPP"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /**
     * For EMAIL activity: call notification service job/send.
     * When template is selected: pass template only, do not pass channelContent (null).
     * When template is null: pass channelContent.EMAIL (subject + body); do not pass template.
     */
    private void sendNotificationJobForOpportunityEmail(UUID opportunityId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;

        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForOpportunityEmail(request, opportunityId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for opportunity {}: no recipients (provide recipients in request or email_to/linked contact email)", opportunityId);
            return;
        }

        Map<String, Object> commonParams = request.commonParams() != null && !request.commonParams().isEmpty()
                ? request.commonParams()
                : null;

        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;

        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            /* Only set channelContent when no template; when template is selected, channelContent stays null */
            Map<String, Object> emailBody = new LinkedHashMap<>();
            emailBody.put("format", "HTML");
            emailBody.put("value", request.emailBody() != null ? request.emailBody() : "");
            Map<String, Object> emailContent = new LinkedHashMap<>();
            emailContent.put("subject", request.emailSubject() != null ? request.emailSubject() : "");
            emailContent.put("body", emailBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("EMAIL", emailContent);
        }

        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("EMAIL"),
                templateSpec,
                commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** For SMS activity on opportunity: recipients from linked contact or request only. */
    private void sendNotificationJobForOpportunitySms(UUID opportunityId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;
        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForOpportunityPhone(request, opportunityId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for opportunity {} SMS: no recipients (provide recipients with phone or to_phone/linked contact phone)", opportunityId);
            return;
        }
        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.smsIdentityId() != null && !request.smsIdentityId().isBlank()) {
            try {
                UUID smsIdentityId = UUID.fromString(request.smsIdentityId().trim());
                String fromPhoneNo = activityRepository.getSmsIdentitySenderId(context.getOrgClientId(), smsIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve sender_id for sms_identity_id {}: {}", request.smsIdentityId(), e.getMessage());
            }
        }
        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;
        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            String smsText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> smsBody = new LinkedHashMap<>();
            smsBody.put("format", "TEXT");
            smsBody.put("value", smsText);
            Map<String, Object> smsContent = new LinkedHashMap<>();
            smsContent.put("body", smsBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("SMS", smsContent);
        }
        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("SMS"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** For WhatsApp activity on opportunity: recipients from linked contact or request only. */
    private void sendNotificationJobForOpportunityWhatsapp(UUID opportunityId, CrmLogActivityRequest request) {
        String templateCode = request.notificationTemplateCode() != null && !request.notificationTemplateCode().isBlank()
                ? request.notificationTemplateCode().trim()
                : null;
        List<NotificationJobRequest.Recipient> recipients = mapRecipientsFromRequestForOpportunityPhone(request, opportunityId);
        if (recipients == null || recipients.isEmpty()) {
            log.warn("Notification job skipped for opportunity {} WhatsApp: no recipients (provide recipients with phone or to_phone/linked contact phone)", opportunityId);
            return;
        }
        Map<String, Object> commonParams = new LinkedHashMap<>();
        if (request.commonParams() != null && !request.commonParams().isEmpty()) {
            commonParams.putAll(request.commonParams());
        }
        if (request.whatsappIdentityId() != null && !request.whatsappIdentityId().isBlank()) {
            try {
                UUID whatsappIdentityId = UUID.fromString(request.whatsappIdentityId().trim());
                String fromPhoneNo = activityRepository.getWhatsappIdentityPhoneNumber(context.getOrgClientId(), whatsappIdentityId);
                if (fromPhoneNo != null && !fromPhoneNo.isBlank()) {
                    commonParams.put("FromPhoneNo", fromPhoneNo);
                }
            } catch (Exception e) {
                log.warn("Could not resolve phone_number for whatsapp_identity_id {}: {}", request.whatsappIdentityId(), e.getMessage());
            }
        }
        NotificationJobRequest.TemplateSpec templateSpec = templateCode != null
                ? new NotificationJobRequest.TemplateSpec(templateCode)
                : null;
        Map<String, Object> channelContent = null;
        if (templateCode == null) {
            String whatsappText = (request.body() != null && !request.body().isBlank())
                    ? request.body()
                    : (request.description() != null ? request.description() : "");
            Map<String, Object> whatsappBody = new LinkedHashMap<>();
            whatsappBody.put("format", "TEXT");
            whatsappBody.put("value", whatsappText);
            Map<String, Object> whatsappContent = new LinkedHashMap<>();
            whatsappContent.put("body", whatsappBody);
            channelContent = new LinkedHashMap<>();
            channelContent.put("WHATSAPP", whatsappContent);
        }
        NotificationJobRequest jobRequest = new NotificationJobRequest(
                context.getOrgClientId().toString(),
                context.getApplicationId().toString(),
                "CRM",
                "NORMAL",
                List.of("WHATSAPP"),
                templateSpec,
                commonParams.isEmpty() ? null : commonParams,
                recipients,
                channelContent
        );
        notificationClient.sendJob(jobRequest);
    }

    /** Phone recipients for SMS/WhatsApp: from request.recipients (to.phone) or request.to_phone/lead.phone(). */
    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequestForPhone(CrmLogActivityRequest request, UUID leadId) {
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            List<NotificationJobRequest.Recipient> withPhone = request.recipients().stream()
                    .filter(r -> r != null && r.to() != null && r.to().phone() != null && !r.to().phone().isBlank())
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
            if (!withPhone.isEmpty()) return withPhone;
        }
        String phone = request.toPhone() != null && !request.toPhone().isBlank()
                ? request.toPhone()
                : null;
        if (phone == null && leadId != null) {
            CrmLeadDetailDto lead = leadService.getLeadById(leadId);
            if (lead != null && lead.phone() != null && !lead.phone().isBlank()) phone = lead.phone();
        }
        if (phone == null || phone.isBlank()) return List.of();
        return List.of(new NotificationJobRequest.Recipient(
                null,
                new NotificationJobRequest.RecipientTo(null, phone),
                null
        ));
    }

    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequest(CrmLogActivityRequest request, UUID leadId) {
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            return request.recipients().stream()
                    .filter(r -> r != null && r.to() != null)
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
        }
        CrmLeadDetailDto lead = leadService.getLeadById(leadId);
        if (lead == null) return List.of();
        List<String> toEmails = request.emailTo() != null && !request.emailTo().isEmpty()
                ? request.emailTo()
                : (lead.email() != null && !lead.email().isBlank() ? List.of(lead.email()) : List.of());
        if (toEmails.isEmpty()) return List.of();
        String firstName = lead.firstName() != null ? lead.firstName() : "";
        String lastName = lead.lastName() != null ? lead.lastName() : "";
        String rawDisplay = lead.fullName() != null && !lead.fullName().isBlank()
                ? lead.fullName()
                : (firstName + " " + lastName).trim();
        final String displayName = (rawDisplay != null && !rawDisplay.isBlank())
                ? rawDisplay
                : (lead.email() != null ? lead.email() : "");
        Map<String, Object> defaultParams = new LinkedHashMap<>();
        defaultParams.put("firstName", firstName);
        defaultParams.put("lastName", lastName);
        defaultParams.put("displayName", displayName);
        final Map<String, Object> params = defaultParams;
        return toEmails.stream()
                .filter(e -> e != null && !e.isBlank())
                .map(email -> new NotificationJobRequest.Recipient(
                        null,
                        new NotificationJobRequest.RecipientTo(email, null),
                        params
                ))
                .toList();
    }

    /** Email recipients for contact: from request.recipients or request.emailTo/contact email and name params. */
    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequestForContact(CrmLogActivityRequest request, UUID contactId) {
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            return request.recipients().stream()
                    .filter(r -> r != null && r.to() != null)
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
        }
        CrmContactDetailDto contact = contactService.getContactById(contactId);
        if (contact == null) return List.of();
        List<String> toEmails = request.emailTo() != null && !request.emailTo().isEmpty()
                ? request.emailTo()
                : (contact.email() != null && !contact.email().isBlank() ? List.of(contact.email()) : List.of());
        if (toEmails.isEmpty()) return List.of();
        String firstName = contact.firstName() != null ? contact.firstName() : "";
        String lastName = contact.lastName() != null ? contact.lastName() : "";
        String rawDisplay = contact.fullName() != null && !contact.fullName().isBlank()
                ? contact.fullName()
                : (firstName + " " + lastName).trim();
        final String displayName = (rawDisplay != null && !rawDisplay.isBlank())
                ? rawDisplay
                : (contact.email() != null ? contact.email() : "");
        Map<String, Object> defaultParams = new LinkedHashMap<>();
        defaultParams.put("firstName", firstName);
        defaultParams.put("lastName", lastName);
        defaultParams.put("displayName", displayName);
        final Map<String, Object> params = defaultParams;
        return toEmails.stream()
                .filter(e -> e != null && !e.isBlank())
                .map(email -> new NotificationJobRequest.Recipient(
                        null,
                        new NotificationJobRequest.RecipientTo(email, null),
                        params
                ))
                .toList();
    }

    /** Phone recipients for contact: from request.recipients (to.phone) or request.to_phone/contact.phone(). */
    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequestForPhoneContact(CrmLogActivityRequest request, UUID contactId) {
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            List<NotificationJobRequest.Recipient> withPhone = request.recipients().stream()
                    .filter(r -> r != null && r.to() != null && r.to().phone() != null && !r.to().phone().isBlank())
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
            if (!withPhone.isEmpty()) return withPhone;
        }
        String phone = request.toPhone() != null && !request.toPhone().isBlank()
                ? request.toPhone()
                : null;
        if (phone == null && contactId != null) {
            CrmContactDetailDto contact = contactService.getContactById(contactId);
            if (contact != null && contact.phone() != null && !contact.phone().isBlank()) phone = contact.phone();
        }
        if (phone == null || phone.isBlank()) return List.of();
        return List.of(new NotificationJobRequest.Recipient(
                null,
                new NotificationJobRequest.RecipientTo(null, phone),
                null
        ));
    }

    /** Email recipients for opportunity: from linked contact (when present) or request.recipients/request.emailTo only. */
    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequestForOpportunityEmail(CrmLogActivityRequest request, UUID opportunityId) {
        UUID orgId = context.getOrgClientId();
        Map<String, Object> opp = opportunityRepository.findById(orgId, opportunityId);
        UUID contactId = null;
        if (opp != null && opp.get("contact_id") != null) {
            Object cid = opp.get("contact_id");
            contactId = cid instanceof UUID ? (UUID) cid : UUID.fromString(cid.toString());
        }
        if (contactId != null) {
            return mapRecipientsFromRequestForContact(request, contactId);
        }
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            return request.recipients().stream()
                    .filter(r -> r != null && r.to() != null && r.to().email() != null && !r.to().email().isBlank())
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
        }
        if (request.emailTo() != null && !request.emailTo().isEmpty()) {
            return request.emailTo().stream()
                    .filter(e -> e != null && !e.isBlank())
                    .map(email -> new NotificationJobRequest.Recipient(
                            null,
                            new NotificationJobRequest.RecipientTo(email, null),
                            null
                    ))
                    .toList();
        }
        return List.of();
    }

    /** Phone recipients for opportunity: from linked contact (when present) or request.recipients/request.to_phone only. */
    private List<NotificationJobRequest.Recipient> mapRecipientsFromRequestForOpportunityPhone(CrmLogActivityRequest request, UUID opportunityId) {
        UUID orgId = context.getOrgClientId();
        Map<String, Object> opp = opportunityRepository.findById(orgId, opportunityId);
        UUID contactId = null;
        if (opp != null && opp.get("contact_id") != null) {
            Object cid = opp.get("contact_id");
            contactId = cid instanceof UUID ? (UUID) cid : UUID.fromString(cid.toString());
        }
        if (contactId != null) {
            return mapRecipientsFromRequestForPhoneContact(request, contactId);
        }
        if (request.recipients() != null && !request.recipients().isEmpty()) {
            List<NotificationJobRequest.Recipient> withPhone = request.recipients().stream()
                    .filter(r -> r != null && r.to() != null && r.to().phone() != null && !r.to().phone().isBlank())
                    .map(r -> new NotificationJobRequest.Recipient(
                            r.clientId(),
                            new NotificationJobRequest.RecipientTo(r.to().email(), r.to().phone()),
                            r.params() != null && !r.params().isEmpty() ? r.params() : null
                    ))
                    .toList();
            if (!withPhone.isEmpty()) return withPhone;
        }
        String phone = request.toPhone() != null && !request.toPhone().isBlank() ? request.toPhone() : null;
        if (phone == null || phone.isBlank()) return List.of();
        return List.of(new NotificationJobRequest.Recipient(
                null,
                new NotificationJobRequest.RecipientTo(null, phone),
                null
        ));
    }

    /** Build a single CrmLogActivityRequest from bulk request (bulk has no email/notification fields). */
    private static CrmLogActivityRequest toLogActivityRequest(CrmBulkActivitiesRequest request) {
        return new CrmLogActivityRequest(
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
                // email (7): emailTo, emailCc, emailBcc, emailSubject, emailBody, emailIdentityId, emailTemplateId
                null, null, null, null, null, null, null,
                // event (7)
                null, null, null, null, null, null, null,
                // task (7)
                null, null, null, null, null, null, null,
                // sms/whatsapp (6): to_phone, body, sms_template_id, sms_identity_id, whatsapp_template_id, whatsapp_identity_id
                null, null, null, null, null, null,
                // notification (3)
                null, null, null
        );
    }

}
