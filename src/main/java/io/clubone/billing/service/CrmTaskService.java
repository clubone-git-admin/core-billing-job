package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmActivityRepository;
import io.clubone.billing.repo.CrmEntitySearchRepository;
import io.clubone.billing.repo.CrmTaskRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmTaskService {

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID CURRENT_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final CrmTaskRepository taskRepository;
    private final CrmActivityRepository activityRepository;
    private final CrmEntitySearchRepository entitySearchRepository;

    public CrmTaskService(CrmTaskRepository taskRepository, CrmActivityRepository activityRepository,
                          CrmEntitySearchRepository entitySearchRepository) {
        this.taskRepository = taskRepository;
        this.activityRepository = activityRepository;
        this.entitySearchRepository = entitySearchRepository;
    }

    public CrmTaskListResponse listTasks(String scope, String view, String search, UUID taskTypeId, UUID taskStatusId,
                                         UUID taskPriorityId, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        UUID assignedFilter = "my".equalsIgnoreCase(scope != null ? scope.trim() : "") ? CURRENT_USER_ID : null;
        String viewVal = (view != null && !view.isBlank()) ? view.trim() : "today";
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = taskRepository.listTasks(orgId, assignedFilter, viewVal, search,
                taskTypeId, taskStatusId, taskPriorityId, limitVal, offsetVal);
        long total = taskRepository.countTasks(orgId, assignedFilter, viewVal, search, taskTypeId, taskStatusId, taskPriorityId);
        List<CrmTaskSummaryDto> items = rows.stream().map(this::mapToSummary).toList();
        return new CrmTaskListResponse(items, total);
    }

    public CrmTaskDetailDto getTaskById(UUID taskId) {
        if (taskId == null) return null;
        Map<String, Object> row = taskRepository.findTaskById(getOrgClientId(), taskId);
        return row != null ? mapToDetail(row) : null;
    }

    @Transactional
    public CrmTaskDetailDto createTask(CrmCreateTaskRequest request) {
        if (request == null) return null;
        String subject = request.subject();
        if (subject == null || subject.isBlank()) return null;
        if (subject.length() > 255) return null;
        if (request.taskTypeId() == null || request.taskTypeId().isBlank()) return null;
        UUID orgId = getOrgClientId();

        UUID existingActivityId = parseUuid(request.activityId());
        UUID entityTypeId = parseUuid(request.entityTypeId());
        UUID entityId = parseUuid(request.entityId());
        UUID relatedEntityTypeId = parseUuid(request.relatedEntityTypeId());
        UUID relatedEntityId = parseUuid(request.relatedEntityId());

        if (existingActivityId == null) {
            if (entityTypeId == null || entityId == null)
                throw new IllegalArgumentException("entity_type_id and entity_id are required when not providing activity_id");
            if (entitySearchRepository.getEntityTypeCode(orgId, entityTypeId) == null)
                throw new IllegalArgumentException("entity_type_id must exist in crm.lu_entity_type");
            if (!entitySearchRepository.entityExists(orgId, entityTypeId, entityId))
                throw new IllegalArgumentException("entity_id must exist for the given entity_type in this org");
        }
        if ((relatedEntityTypeId != null) != (relatedEntityId != null))
            throw new IllegalArgumentException("related_entity_type_id and related_entity_id must be provided together or both omitted");

        UUID taskTypeId = UUID.fromString(request.taskTypeId());
        if (!taskRepository.taskTypeExists(orgId, taskTypeId)) return null;
        UUID taskStatusId = null;
        if (request.taskStatusId() != null && !request.taskStatusId().isBlank()) {
            taskStatusId = UUID.fromString(request.taskStatusId());
            if (!taskRepository.taskStatusExists(orgId, taskStatusId)) return null;
        } else {
            taskStatusId = taskRepository.resolveTaskStatusIdByCode(orgId, "NOT_STARTED");
            if (taskStatusId == null) taskStatusId = taskRepository.resolveTaskStatusIdByCode(orgId, "PENDING");
        }
        UUID taskPriorityId = parseUuid(request.taskPriorityId());
        if (taskPriorityId != null && !taskRepository.taskPriorityExists(orgId, taskPriorityId)) return null;
        Timestamp dueDateTime = parseIsoToTimestamp(request.dueDate());
        UUID assignedToUserId = parseUuid(request.assignedToUserId());
        boolean reminderSet = Boolean.TRUE.equals(request.reminderSet());
        Timestamp reminderTime = parseIsoToTimestamp(request.reminderTime());
        UUID activityTypeId = activityRepository.resolveActivityTypeIdByCode(orgId, "TASK");
        if (activityTypeId == null) return null;
        UUID activityStatusId = activityRepository.resolveActivityStatusIdByCode(orgId, "NOT_STARTED");
        if (activityStatusId == null) activityStatusId = activityRepository.resolveActivityStatusIdByCode(orgId, "PENDING");
        UUID visibilityId = activityRepository.resolveActivityVisibilityIdByCode(orgId, "INTERNAL");
        if (visibilityId == null) visibilityId = activityRepository.resolveActivityVisibilityIdByCode(orgId, "PRIVATE");
        UUID taskId = taskRepository.insertTask(orgId, activityTypeId, activityStatusId, visibilityId,
                existingActivityId != null ? null : entityTypeId, existingActivityId != null ? null : entityId,
                relatedEntityTypeId, relatedEntityId, existingActivityId,
                subject, dueDateTime, assignedToUserId, CURRENT_USER_ID,
                taskTypeId, taskStatusId, taskPriorityId,
                reminderSet, reminderTime, request.comments(), CURRENT_USER_ID);
        return getTaskById(taskId);
    }

    @Transactional
    public CrmTaskDetailDto updateTask(UUID taskId, Map<String, Object> body) {
        if (taskId == null || body == null) return null;
        UUID orgId = getOrgClientId();
        if (!taskRepository.taskExists(orgId, taskId)) return null;
        Map<String, Object> activityUpdates = new HashMap<>();
        Map<String, Object> taskUpdates = new HashMap<>();
        if (body.containsKey("subject")) {
            String v = body.get("subject") != null ? body.get("subject").toString().trim() : null;
            if (v != null && v.length() > 255) throw new IllegalArgumentException("subject must be at most 255 characters");
            activityUpdates.put("subject", v);
        }
        if (body.containsKey("due_date")) {
            Timestamp due = parseIsoToTimestamp(body.get("due_date") != null ? body.get("due_date").toString() : null);
            activityUpdates.put("start_date_time", due);
            activityUpdates.put("end_date_time", due);
        }
        if (body.containsKey("assigned_to_user_id")) {
            Object v = body.get("assigned_to_user_id");
            activityUpdates.put("assigned_to_user_id", v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString()));
        }
        if (body.containsKey("task_type_id")) {
            Object v = body.get("task_type_id");
            UUID id = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            if (id != null && !taskRepository.taskTypeExists(orgId, id)) throw new IllegalArgumentException("task_type_id must exist");
            taskUpdates.put("task_type_id", id);
        }
        if (body.containsKey("task_status_id")) {
            Object v = body.get("task_status_id");
            UUID id = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            if (id != null && !taskRepository.taskStatusExists(orgId, id)) throw new IllegalArgumentException("task_status_id must exist");
            taskUpdates.put("task_status_id", id);
        }
        if (body.containsKey("task_priority_id")) {
            Object v = body.get("task_priority_id");
            UUID id = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            if (id != null && !taskRepository.taskPriorityExists(orgId, id)) throw new IllegalArgumentException("task_priority_id must exist");
            taskUpdates.put("task_priority_id", id);
        }
        if (body.containsKey("comments")) taskUpdates.put("comments", body.get("comments") != null ? body.get("comments").toString() : null);
        if (body.containsKey("reminder_set")) taskUpdates.put("reminder_set", body.get("reminder_set"));
        if (body.containsKey("reminder_time")) taskUpdates.put("reminder_time", parseIsoToTimestamp(body.get("reminder_time") != null ? body.get("reminder_time").toString() : null));
        if (body.containsKey("entity_type_id") || body.containsKey("entity_id")) {
            UUID newEntityTypeId = body.containsKey("entity_type_id") ? parseUuid(body.get("entity_type_id") != null ? body.get("entity_type_id").toString() : null) : null;
            UUID newEntityId = body.containsKey("entity_id") ? parseUuid(body.get("entity_id") != null ? body.get("entity_id").toString() : null) : null;
            if (newEntityTypeId != null && newEntityId != null) {
                if (entitySearchRepository.getEntityTypeCode(orgId, newEntityTypeId) == null)
                    throw new IllegalArgumentException("entity_type_id must exist in crm.lu_entity_type");
                if (!entitySearchRepository.entityExists(orgId, newEntityTypeId, newEntityId))
                    throw new IllegalArgumentException("entity_id must exist for the given entity_type in this org");
                activityUpdates.put("entity_type_id", newEntityTypeId);
                activityUpdates.put("entity_id", newEntityId);
            } else if (newEntityTypeId != null || newEntityId != null) {
                throw new IllegalArgumentException("entity_type_id and entity_id must be provided together when re-linking");
            }
        }
        if (body.containsKey("related_entity_type_id") || body.containsKey("related_entity_id")) {
            UUID relTypeId = body.containsKey("related_entity_type_id") ? parseUuid(body.get("related_entity_type_id") != null ? body.get("related_entity_type_id").toString() : null) : null;
            UUID relId = body.containsKey("related_entity_id") ? parseUuid(body.get("related_entity_id") != null ? body.get("related_entity_id").toString() : null) : null;
            if ((relTypeId != null) != (relId != null))
                throw new IllegalArgumentException("related_entity_type_id and related_entity_id must be provided together or both omitted");
            activityUpdates.put("related_entity_type_id", relTypeId);
            activityUpdates.put("related_entity_id", relId);
        }
        taskRepository.updateTask(orgId, taskId, activityUpdates, taskUpdates, CURRENT_USER_ID);
        return getTaskById(taskId);
    }

    @Transactional
    public boolean deleteTask(UUID taskId) {
        if (taskId == null) return false;
        UUID orgId = getOrgClientId();
        if (!taskRepository.taskExists(orgId, taskId)) return false;
        return taskRepository.deleteTask(orgId, taskId, CURRENT_USER_ID) > 0;
    }

    private CrmTaskSummaryDto mapToSummary(Map<String, Object> r) {
        Boolean isOverdue = r.get("is_overdue") instanceof Boolean b ? b : Boolean.FALSE;
        return new CrmTaskSummaryDto(
                asString(r.get("task_id")),
                asString(r.get("subject")),
                toIsoString(r.get("due_date")),
                asString(r.get("task_type_id")),
                asString(r.get("task_type_display_name")),
                asString(r.get("task_status_id")),
                asString(r.get("task_status_display_name")),
                asString(r.get("task_priority_id")),
                asString(r.get("task_priority_display_name")),
                asString(r.get("assigned_to_user_id")),
                asString(r.get("assigned_to_display_name")),
                asString(r.get("entity_type_id")),
                asString(r.get("entity_id")),
                asString(r.get("entity_display_name")),
                asString(r.get("related_entity_type_id")),
                asString(r.get("related_entity_id")),
                asString(r.get("related_entity_display_name")),
                isOverdue,
                toIsoString(r.get("created_on"))
        );
    }

    private CrmTaskDetailDto mapToDetail(Map<String, Object> r) {
        return new CrmTaskDetailDto(
                asString(r.get("task_id")),
                asString(r.get("subject")),
                toIsoString(r.get("due_date")),
                asString(r.get("task_type_id")),
                asString(r.get("task_type_display_name")),
                asString(r.get("task_status_id")),
                asString(r.get("task_status_display_name")),
                asString(r.get("task_priority_id")),
                asString(r.get("task_priority_display_name")),
                asString(r.get("assigned_to_user_id")),
                asString(r.get("assigned_to_display_name")),
                r.get("reminder_set") instanceof Boolean b ? b : null,
                toIsoString(r.get("reminder_time")),
                asString(r.get("comments")),
                asString(r.get("entity_type_id")),
                asString(r.get("entity_id")),
                asString(r.get("entity_display_name")),
                asString(r.get("related_entity_type_id")),
                asString(r.get("related_entity_id")),
                asString(r.get("related_entity_display_name")),
                asString(r.get("activity_id")),
                toIsoString(r.get("created_on")),
                toIsoString(r.get("modified_on"))
        );
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }
    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof java.time.OffsetDateTime odt) return odt.toString();
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant().atOffset(ZoneOffset.UTC).toString();
        return value.toString();
    }
    private static UUID parseUuid(String s) {
        if (s == null || s.isBlank()) return null;
        try { return UUID.fromString(s); } catch (Exception e) { return null; }
    }
    private static Timestamp parseIsoToTimestamp(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            Instant instant = java.time.OffsetDateTime.parse(s).toInstant();
            return Timestamp.from(instant);
        } catch (Exception e) {
            try {
                LocalDate date = LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE);
                return Timestamp.from(date.atStartOfDay(ZoneOffset.UTC).toInstant());
            } catch (Exception e2) { return null; }
        }
    }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
}
