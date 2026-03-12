package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for crm.activity and channel tables (call, email, event, task, sms, whatsapp).
 */
@Repository
public class CrmActivityRepository {

    private final JdbcTemplate jdbc;

    public CrmActivityRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public UUID resolveEntityTypeIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_entity_type", "entity_type_id", orgClientId, code);
    }

    public UUID resolveActivityTypeIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_activity_type", "activity_type_id", orgClientId, code);
    }

    public UUID resolveActivityStatusIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_activity_status", "activity_status_id", orgClientId, code);
    }

    public UUID resolveActivityVisibilityIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_activity_visibility", "activity_visibility_id", orgClientId, code);
    }

    public UUID resolveActivityOutcomeIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) return null;
        return resolveIdByCode("crm.lu_activity_outcome", "activity_outcome_id", orgClientId, code);
    }

    public UUID resolveCallDirectionIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_call_direction", "call_direction_id", orgClientId, code);
    }

    public UUID resolveCallResultIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_call_result", "call_result_id", orgClientId, code);
    }

    public UUID resolveEventStatusIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_event_status", "event_status_id", orgClientId, code);
    }

    public UUID resolveRsvpStatusIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_rsvp_status", "rsvp_status_id", orgClientId, code);
    }

    public UUID resolveTaskTypeIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_task_type", "task_type_id", orgClientId, code);
    }

    public UUID resolveTaskStatusIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_task_status", "task_status_id", orgClientId, code);
    }

    public UUID resolveTaskPriorityIdByCode(UUID orgClientId, String code) {
        return resolveIdByCode("crm.lu_task_priority", "task_priority_id", orgClientId, code);
    }

    /** Resolve task type by UUID (if value is UUID and exists for org) or by code. */
    public UUID resolveTaskTypeId(UUID orgClientId, String value) {
        if (value == null || value.isBlank()) return null;
        try {
            UUID id = UUID.fromString(value.trim());
            List<UUID> list = jdbc.query(
                    "SELECT task_type_id FROM crm.lu_task_type WHERE org_client_id = ? AND task_type_id = ? AND is_active = true LIMIT 1",
                    (rs, i) -> (UUID) rs.getObject("task_type_id"), orgClientId, id);
            if (!list.isEmpty()) return list.get(0);
        } catch (IllegalArgumentException ignored) {}
        return resolveTaskTypeIdByCode(orgClientId, value);
    }

    /** Resolve task status by UUID (if value is UUID and exists for org) or by code. */
    public UUID resolveTaskStatusId(UUID orgClientId, String value) {
        if (value == null || value.isBlank()) return null;
        try {
            UUID id = UUID.fromString(value.trim());
            List<UUID> list = jdbc.query(
                    "SELECT task_status_id FROM crm.lu_task_status WHERE org_client_id = ? AND task_status_id = ? AND is_active = true LIMIT 1",
                    (rs, i) -> (UUID) rs.getObject("task_status_id"), orgClientId, id);
            if (!list.isEmpty()) return list.get(0);
        } catch (IllegalArgumentException ignored) {}
        return resolveTaskStatusIdByCode(orgClientId, value);
    }

    /** Resolve task priority by UUID (if value is UUID and exists for org) or by code. */
    public UUID resolveTaskPriorityId(UUID orgClientId, String value) {
        if (value == null || value.isBlank()) return null;
        try {
            UUID id = UUID.fromString(value.trim());
            List<UUID> list = jdbc.query(
                    "SELECT task_priority_id FROM crm.lu_task_priority WHERE org_client_id = ? AND task_priority_id = ? AND is_active = true LIMIT 1",
                    (rs, i) -> (UUID) rs.getObject("task_priority_id"), orgClientId, id);
            if (!list.isEmpty()) return list.get(0);
        } catch (IllegalArgumentException ignored) {}
        return resolveTaskPriorityIdByCode(orgClientId, value);
    }

    public UUID resolveMessageProviderId(UUID orgClientId) {
        List<UUID> ids = jdbc.query(
                "SELECT message_provider_id FROM crm.lu_message_provider WHERE org_client_id = ? AND is_active = true LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("message_provider_id"),
                orgClientId);
        return ids.isEmpty() ? null : ids.get(0);
    }

    private UUID resolveIdByCode(String table, String idColumn, UUID orgId, String code) {
        if (code == null || code.isBlank()) return null;
        List<UUID> ids = jdbc.query(
                "SELECT " + idColumn + " FROM " + table + " WHERE org_client_id = ? AND UPPER(TRIM(code)) = UPPER(TRIM(?)) AND is_active = true LIMIT 1",
                (rs, i) -> (UUID) rs.getObject(idColumn),
                orgId, code);
        return ids.isEmpty() ? null : ids.get(0);
    }

    public boolean leadExists(UUID orgClientId, UUID leadId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.leads WHERE org_client_id = ? AND lead_id = ?)",
                Boolean.class, orgClientId, leadId);
        return Boolean.TRUE.equals(exists);
    }

    public boolean contactExists(UUID orgClientId, UUID contactId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.contact WHERE org_client_id = ? AND contact_id = ?)",
                Boolean.class, orgClientId, contactId);
        return Boolean.TRUE.equals(exists);
    }

    public boolean opportunityExists(UUID orgClientId, UUID opportunityId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.opportunity WHERE org_client_id = ? AND opportunity_id = ?)",
                Boolean.class, orgClientId, opportunityId);
        return Boolean.TRUE.equals(exists);
    }

    public List<Map<String, Object>> findActivitiesByLead(UUID orgClientId, UUID leadId, UUID entityTypeId,
                                                          String typeCode, String statusCode, String outcomeCode,
                                                          Timestamp from, Timestamp to, String search,
                                                          int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                a.activity_id,
                at.code AS activity_type_code,
                at.display_name AS activity_type_display_name,
                ast.code AS activity_status_code,
                ast.display_name AS activity_status_display_name,
                ao.code AS activity_outcome_code,
                ao.display_name AS activity_outcome_display_name,
                av.code AS activity_visibility_code,
                av.display_name AS activity_visibility_display_name,
                a.subject, a.description, a.notes,
                a.start_date_time, a.end_date_time,
                a.owner_user_id,
                TRIM(COALESCE(own.first_name,'') || ' ' || COALESCE(own.last_name,'')) AS owner_user_display_name,
                a.assigned_to_user_id,
                TRIM(COALESCE(ass.first_name,'') || ' ' || COALESCE(ass.last_name,'')) AS assigned_to_user_display_name,
                cd.code AS call_direction_code,
                cd.display_name AS call_direction_display_name,
                cr.code AS call_result_code,
                cr.display_name AS call_result_display_name,
                ac.call_duration_seconds,
                eds.code AS email_delivery_status_code,
                eds.display_name AS email_delivery_status_display_name,
                sds.code AS sms_delivery_status_code,
                sds.display_name AS sms_delivery_status_display_name,
                wds.code AS whatsapp_delivery_status_code,
                wds.display_name AS whatsapp_delivery_status_display_name,
                es.code AS event_status_code,
                es.display_name AS event_status_display_name,
                ep.display_name AS event_purpose_display_name,
                aev.event_place AS location,
                tt.code AS task_type_code,
                tt.display_name AS task_type_display_name,
                ts.code AS task_status_code,
                ts.display_name AS task_status_display_name,
                tp.code AS task_priority_code,
                tp.display_name AS task_priority_display_name,
                COALESCE(COALESCE(ei.from_email, ei.code), COALESCE(si.sender_name, si.code), COALESCE(wi.phone_number, wi.code)) AS channel_from,
                COALESCE(et.template_name, et.template_code, st.template_name, st.template_code, wt.template_name, wt.template_code) AS channel_template
            FROM crm.activity a
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id
            INNER JOIN crm.lu_activity_status ast ON ast.activity_status_id = a.activity_status_id
            INNER JOIN crm.lu_activity_visibility av ON av.activity_visibility_id = a.activity_visibility_id
            LEFT JOIN crm.lu_activity_outcome ao ON ao.activity_outcome_id = a.activity_outcome_id
            LEFT JOIN "access".access_user own ON own.user_id = a.owner_user_id
            LEFT JOIN "access".access_user ass ON ass.user_id = a.assigned_to_user_id
            LEFT JOIN crm.activity_call ac ON ac.activity_id = a.activity_id
            LEFT JOIN crm.lu_call_direction cd ON cd.call_direction_id = ac.call_direction_id
            LEFT JOIN crm.lu_call_result cr ON cr.call_result_id = ac.call_result_id
            LEFT JOIN crm.activity_email ae ON ae.activity_id = a.activity_id
            LEFT JOIN crm.lu_email_delivery_status eds ON eds.email_delivery_status_id = ae.email_delivery_status_id
            LEFT JOIN crm.lu_email_identity ei ON ei.email_identity_id = ae.email_identity_id AND ei.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template et ON et.template_id = ae.email_template_id AND et.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_sms asms ON asms.activity_id = a.activity_id
            LEFT JOIN crm.lu_sms_delivery_status sds ON sds.sms_delivery_status_id = asms.sms_delivery_status_id
            LEFT JOIN crm.lu_sms_identity si ON si.sms_identity_id = asms.sms_identity_id AND si.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template st ON st.template_id = asms.sms_template_id AND st.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_whatsapp aw ON aw.activity_id = a.activity_id
            LEFT JOIN crm.lu_whatsapp_delivery_status wds ON wds.whatsapp_delivery_status_id = aw.whatsapp_delivery_status_id
            LEFT JOIN crm.lu_whatsapp_identity wi ON wi.whatsapp_identity_id = aw.whatsapp_identity_id AND wi.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template wt ON wt.template_id = aw.whatsapp_template_id AND wt.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_event aev ON aev.activity_id = a.activity_id
            LEFT JOIN crm.lu_event_status es ON es.event_status_id = aev.event_status_id
            LEFT JOIN crm.lu_event_purpose ep ON ep.event_purpose_id = aev.event_purpose_id AND ep.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_task atask ON atask.activity_id = a.activity_id
            LEFT JOIN crm.lu_task_type tt ON tt.task_type_id = atask.task_type_id
            LEFT JOIN crm.lu_task_status ts ON ts.task_status_id = atask.task_status_id
            LEFT JOIN crm.lu_task_priority tp ON tp.task_priority_id = atask.task_priority_id
            WHERE a.org_client_id = ? AND a.entity_type_id = ? AND a.entity_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(entityTypeId);
        params.add(leadId);

        if (typeCode != null && !typeCode.isBlank()) {
            sql.append(" AND at.code = ? ");
            params.add(typeCode);
        }
        if (statusCode != null && !statusCode.isBlank()) {
            sql.append(" AND ast.code = ? ");
            params.add(statusCode);
        }
        if (outcomeCode != null && !outcomeCode.isBlank()) {
            sql.append(" AND ao.code = ? ");
            params.add(outcomeCode);
        }
        if (from != null) {
            sql.append(" AND a.start_date_time >= ? ");
            params.add(from);
        }
        if (to != null) {
            sql.append(" AND a.start_date_time <= ? ");
            params.add(to);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.subject ILIKE ? OR a.description ILIKE ? OR a.notes ILIKE ?) ");
            String pattern = "%" + search + "%";
            params.add(pattern);
            params.add(pattern);
            params.add(pattern);
        }

        sql.append(" ORDER BY a.start_date_time DESC NULLS LAST, a.created_on DESC LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countActivitiesByLead(UUID orgClientId, UUID leadId, UUID entityTypeId,
                                      String typeCode, String statusCode, String outcomeCode,
                                      Timestamp from, Timestamp to, String search) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*)
            FROM crm.activity a
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id
            INNER JOIN crm.lu_activity_status ast ON ast.activity_status_id = a.activity_status_id
            LEFT JOIN crm.lu_activity_outcome ao ON ao.activity_outcome_id = a.activity_outcome_id
            WHERE a.org_client_id = ? AND a.entity_type_id = ? AND a.entity_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(entityTypeId);
        params.add(leadId);
        if (typeCode != null && !typeCode.isBlank()) { sql.append(" AND at.code = ? "); params.add(typeCode); }
        if (statusCode != null && !statusCode.isBlank()) { sql.append(" AND ast.code = ? "); params.add(statusCode); }
        if (outcomeCode != null && !outcomeCode.isBlank()) { sql.append(" AND ao.code = ? "); params.add(outcomeCode); }
        if (from != null) { sql.append(" AND a.start_date_time >= ? "); params.add(from); }
        if (to != null) { sql.append(" AND a.start_date_time <= ? "); params.add(to); }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.subject ILIKE ? OR a.description ILIKE ? OR a.notes ILIKE ?) ");
            String pattern = "%" + search + "%";
            params.add(pattern);
            params.add(pattern);
            params.add(pattern);
        }
        Long count = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }

    /**
     * Returns activity stats for a lead used to compute warmth score.
     * Uses COALESCE(start_date_time, created_on) as activity time.
     *
     * @return map with "last_activity_at" (Timestamp, may be null) and "activity_count_90d" (Long, >= 0)
     */
    public Map<String, Object> getLeadActivityStatsForWarmth(UUID orgClientId, UUID leadId, UUID entityTypeId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT
                MAX(COALESCE(a.start_date_time, a.created_on)) AS last_activity_at,
                COUNT(*) FILTER (WHERE (COALESCE(a.start_date_time, a.created_on)) >= (CURRENT_TIMESTAMP - INTERVAL '90 days')) AS activity_count_90d
            FROM crm.activity a
            WHERE a.org_client_id = ? AND a.entity_type_id = ? AND a.entity_id = ?
              AND COALESCE(a.is_deleted, false) = false
            """, orgClientId, entityTypeId, leadId);
        if (rows.isEmpty()) {
            return Map.of("last_activity_at", (Object) null, "activity_count_90d", 0L);
        }
        Map<String, Object> row = rows.get(0);
        Object countObj = row.get("activity_count_90d");
        long count90d = countObj instanceof Number n ? n.longValue() : 0L;
        return Map.of(
            "last_activity_at", row.get("last_activity_at"),
            "activity_count_90d", count90d
        );
    }

    public UUID insertActivity(UUID orgClientId, UUID entityTypeId, UUID entityId,
                               UUID activityTypeId, UUID activityStatusId, UUID activityVisibilityId, UUID activityOutcomeId,
                               String subject, String description, String notes,
                               Timestamp startDateTime, Timestamp endDateTime,
                               UUID ownerUserId, UUID assignedToUserId, UUID createdBy) {
        UUID activityId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.activity (
                activity_id, org_client_id, entity_type_id, entity_id,
                activity_type_id, activity_status_id, activity_visibility_id, activity_outcome_id,
                subject, description, notes, start_date_time, end_date_time,
                owner_user_id, assigned_to_user_id, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, activityId, orgClientId, entityTypeId, entityId,
                activityTypeId, activityStatusId, activityVisibilityId, activityOutcomeId,
                subject, description, notes, startDateTime, endDateTime,
                ownerUserId, assignedToUserId != null ? assignedToUserId : ownerUserId, createdBy);
        return activityId;
    }

    public void insertActivityCall(UUID activityId, UUID orgClientId, UUID callDirectionId, UUID callResultId,
                                   Integer callDurationSeconds, UUID createdBy) {
        jdbc.update("""
            INSERT INTO crm.activity_call (activity_id, org_client_id, call_direction_id, call_result_id, call_duration_seconds, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """, activityId, orgClientId, callDirectionId, callResultId, callDurationSeconds, createdBy);
    }

    public void insertActivityEmail(UUID activityId, UUID orgClientId, UUID messageProviderId,
                                    String[] emailTo, String[] emailCc, String[] emailBcc,
                                    String emailSubject, String emailBody,
                                    UUID emailTemplateId, UUID emailIdentityId, UUID createdBy) {
        String toArr = toTextArrayLiteral(emailTo);
        String ccArr = toTextArrayLiteral(emailCc);
        String bccArr = toTextArrayLiteral(emailBcc);
        jdbc.update("""
            INSERT INTO crm.activity_email (
                activity_id, org_client_id, message_provider_id,
                email_to, email_cc, email_bcc,
                email_subject, email_body,
                email_template_id, email_identity_id,
                created_by
            ) VALUES (
                ?, ?, ?,
                ?::text[], ?::text[], ?::text[],
                ?, ?,
                ?, ?,
                ?
            )
            """, activityId, orgClientId, messageProviderId,
                toArr, ccArr, bccArr,
                emailSubject, emailBody,
                emailTemplateId, emailIdentityId,
                createdBy);
    }

    private static String toTextArrayLiteral(String[] arr) {
        if (arr == null || arr.length == 0) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(arr[i].replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    public void insertActivityEvent(UUID activityId, UUID orgClientId, UUID eventStatusId, String eventPlaceId,
                                    UUID eventPurposeId, UUID createdBy) {
        jdbc.update("""
            INSERT INTO crm.activity_event (activity_id, org_client_id, event_status_id, event_place, event_purpose_id, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """, activityId, orgClientId, eventStatusId, eventPlaceId, eventPurposeId, createdBy);
    }

    public void insertActivityEventAttendee(UUID activityId, UUID orgClientId, String attendeeName, String attendeeEmail,
                                            UUID rsvpStatusId, UUID createdBy) {
        UUID attendeeId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.activity_event_attendee (attendee_id, org_client_id, activity_id, attendee_name, attendee_email, rsvp_status_id, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, attendeeId, orgClientId, activityId, attendeeName, attendeeEmail, rsvpStatusId, createdBy);
    }

    /**
     * Inserts into crm.activity_task (new schema: task data lives here, activity_id links to crm.activity).
     */
    public void insertActivityTask(UUID activityId, UUID orgClientId, UUID taskTypeId, UUID taskStatusId, UUID taskPriorityId,
                                   boolean reminderSet, Timestamp reminderTime, UUID eventPlaceId, String comments, UUID createdBy) {
        UUID taskId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.activity_task (
                task_id, org_client_id, task_type_id, task_status_id, task_priority_id,
                reminder_set, reminder_time, comments, tags, attributes,
                activity_id, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]'::jsonb, '{}'::jsonb, ?, ?)
            """, taskId, orgClientId, taskTypeId, taskStatusId, taskPriorityId,
                reminderSet, reminderTime, comments, activityId, createdBy);
    }

    public void insertActivitySms(UUID activityId, UUID orgClientId, UUID messageProviderId,
                                  String toPhone, String body, UUID smsTemplateId, UUID smsIdentityId, UUID createdBy) {
        jdbc.update("""
            INSERT INTO crm.activity_sms (
                activity_id, org_client_id, message_provider_id,
                to_phone, sms_body, sms_template_id, sms_identity_id,
                created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, activityId, orgClientId, messageProviderId,
                toPhone, body, smsTemplateId, smsIdentityId, createdBy);
    }

    public void insertActivityWhatsapp(UUID activityId, UUID orgClientId, UUID messageProviderId,
                                       String toPhone, String body, UUID whatsappTemplateId, UUID whatsappIdentityId, UUID createdBy) {
        jdbc.update("""
            INSERT INTO crm.activity_whatsapp (
                activity_id, org_client_id, message_provider_id,
                to_phone, whatsapp_body, whatsapp_template_id, whatsapp_identity_id,
                created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, activityId, orgClientId, messageProviderId,
                toPhone, body, whatsappTemplateId, whatsappIdentityId, createdBy);
    }

    /** Fetch one activity by id (same row shape as timeline for mapping). */
    public Map<String, Object> findActivityById(UUID orgClientId, UUID activityId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT
                a.activity_id,
                at.code AS activity_type_code,
                at.display_name AS activity_type_display_name,
                ast.code AS activity_status_code,
                ast.display_name AS activity_status_display_name,
                ao.code AS activity_outcome_code,
                ao.display_name AS activity_outcome_display_name,
                av.code AS activity_visibility_code,
                av.display_name AS activity_visibility_display_name,
                a.subject, a.description, a.notes,
                a.start_date_time, a.end_date_time,
                a.owner_user_id,
                TRIM(COALESCE(own.first_name,'') || ' ' || COALESCE(own.last_name,'')) AS owner_user_display_name,
                a.assigned_to_user_id,
                TRIM(COALESCE(ass.first_name,'') || ' ' || COALESCE(ass.last_name,'')) AS assigned_to_user_display_name,
                cd.code AS call_direction_code,
                cd.display_name AS call_direction_display_name,
                cr.code AS call_result_code,
                cr.display_name AS call_result_display_name,
                ac.call_duration_seconds,
                eds.code AS email_delivery_status_code,
                eds.display_name AS email_delivery_status_display_name,
                sds.code AS sms_delivery_status_code,
                sds.display_name AS sms_delivery_status_display_name,
                wds.code AS whatsapp_delivery_status_code,
                wds.display_name AS whatsapp_delivery_status_display_name,
                es.code AS event_status_code,
                es.display_name AS event_status_display_name,
                ep.display_name AS event_purpose_display_name,
                aev.event_place AS location,
                tt.code AS task_type_code,
                tt.display_name AS task_type_display_name,
                ts.code AS task_status_code,
                ts.display_name AS task_status_display_name,
                tp.code AS task_priority_code,
                tp.display_name AS task_priority_display_name,
                COALESCE(COALESCE(ei.from_email, ei.code), COALESCE(si.sender_name, si.code), COALESCE(wi.phone_number, wi.code)) AS channel_from,
                COALESCE(et.template_name, et.template_code, st.template_name, st.template_code, wt.template_name, wt.template_code) AS channel_template
            FROM crm.activity a
            INNER JOIN crm.lu_activity_type at ON at.activity_type_id = a.activity_type_id
            INNER JOIN crm.lu_activity_status ast ON ast.activity_status_id = a.activity_status_id
            INNER JOIN crm.lu_activity_visibility av ON av.activity_visibility_id = a.activity_visibility_id
            LEFT JOIN crm.lu_activity_outcome ao ON ao.activity_outcome_id = a.activity_outcome_id
            LEFT JOIN "access".access_user own ON own.user_id = a.owner_user_id
            LEFT JOIN "access".access_user ass ON ass.user_id = a.assigned_to_user_id
            LEFT JOIN crm.activity_call ac ON ac.activity_id = a.activity_id
            LEFT JOIN crm.lu_call_direction cd ON cd.call_direction_id = ac.call_direction_id
            LEFT JOIN crm.lu_call_result cr ON cr.call_result_id = ac.call_result_id
            LEFT JOIN crm.activity_email ae ON ae.activity_id = a.activity_id
            LEFT JOIN crm.lu_email_delivery_status eds ON eds.email_delivery_status_id = ae.email_delivery_status_id
            LEFT JOIN crm.lu_email_identity ei ON ei.email_identity_id = ae.email_identity_id AND ei.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template et ON et.template_id = ae.email_template_id AND et.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_sms asms ON asms.activity_id = a.activity_id
            LEFT JOIN crm.lu_sms_delivery_status sds ON sds.sms_delivery_status_id = asms.sms_delivery_status_id
            LEFT JOIN crm.lu_sms_identity si ON si.sms_identity_id = asms.sms_identity_id AND si.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template st ON st.template_id = asms.sms_template_id AND st.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_whatsapp aw ON aw.activity_id = a.activity_id
            LEFT JOIN crm.lu_whatsapp_delivery_status wds ON wds.whatsapp_delivery_status_id = aw.whatsapp_delivery_status_id
            LEFT JOIN crm.lu_whatsapp_identity wi ON wi.whatsapp_identity_id = aw.whatsapp_identity_id AND wi.org_client_id = a.org_client_id
            LEFT JOIN notification.notification_template wt ON wt.template_id = aw.whatsapp_template_id AND wt.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_event aev ON aev.activity_id = a.activity_id
            LEFT JOIN crm.lu_event_status es ON es.event_status_id = aev.event_status_id
            LEFT JOIN crm.lu_event_purpose ep ON ep.event_purpose_id = aev.event_purpose_id AND ep.org_client_id = a.org_client_id
            LEFT JOIN crm.activity_task atask ON atask.activity_id = a.activity_id
            LEFT JOIN crm.lu_task_type tt ON tt.task_type_id = atask.task_type_id
            LEFT JOIN crm.lu_task_status ts ON ts.task_status_id = atask.task_status_id
            LEFT JOIN crm.lu_task_priority tp ON tp.task_priority_id = atask.task_priority_id
            WHERE a.org_client_id = ? AND a.activity_id = ? AND COALESCE(a.is_deleted, false) = false
            """, orgClientId, activityId);
        return rows.isEmpty() ? null : rows.get(0);
    }
}
