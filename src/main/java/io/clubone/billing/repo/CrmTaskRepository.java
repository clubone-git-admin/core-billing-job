package io.clubone.billing.repo;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for CRM Tasks list screen: list/ get/ create/ update/ delete tasks.
 * Tasks are stored in crm.activity_task with parent crm.activity (subject, due, assigned_to).
 */
@Repository
public class CrmTaskRepository {

    private final JdbcTemplate jdbc;

    public CrmTaskRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String ENTITY_DISPLAY_SQL = """
        COALESCE(
          (SELECT l.full_name FROM crm.leads l WHERE l.lead_id = a.entity_id AND l.org_client_id = a.org_client_id LIMIT 1),
          (SELECT c.full_name FROM crm.contact c WHERE c.contact_id = a.entity_id AND c.org_client_id = a.org_client_id LIMIT 1),
          (SELECT cs.subject FROM crm.cases cs WHERE cs.case_id = a.entity_id AND cs.org_client_id = a.org_client_id AND COALESCE(cs.is_deleted,false)=false LIMIT 1),
          (SELECT ac.account_name FROM crm.account ac WHERE ac.account_id = a.entity_id AND ac.org_client_id = a.org_client_id LIMIT 1)
        )
        """;

    private static final String LIST_SELECT = """
        SELECT t.task_id, a.subject,
               a.start_date_time AS due_date,
               t.task_type_id, tt.display_name AS task_type_display_name,
               t.task_status_id, ts.code AS task_status_code, ts.display_name AS task_status_display_name,
               t.task_priority_id, tp.display_name AS task_priority_display_name,
               a.assigned_to_user_id,
               TRIM(REGEXP_REPLACE(COALESCE(u.first_name,'') || ' ' || COALESCE(u.middle_name,'') || ' ' || COALESCE(u.last_name,''), ' +', ' ')) AS assigned_to_display_name,
               a.entity_type_id, et.code AS entity_type_code, a.entity_id, """ + ENTITY_DISPLAY_SQL + " AS entity_display_name,\n" + """
               NULL::uuid AS related_entity_type_id, NULL::uuid AS related_entity_id, NULL::text AS related_entity_display_name,
               (a.start_date_time IS NOT NULL AND a.start_date_time::date < CURRENT_DATE
                AND (ts.code IS NULL OR UPPER(ts.code) NOT IN ('COMPLETED','DONE','CLOSED'))) AS is_overdue,
               t.created_on
        FROM crm.activity_task t
        INNER JOIN crm.activity a ON a.activity_id = t.activity_id AND a.org_client_id = t.org_client_id
        LEFT JOIN crm.lu_task_type tt ON tt.task_type_id = t.task_type_id
        LEFT JOIN crm.lu_task_status ts ON ts.task_status_id = t.task_status_id
        LEFT JOIN crm.lu_task_priority tp ON tp.task_priority_id = t.task_priority_id
        LEFT JOIN "access".access_user u ON u.user_id = a.assigned_to_user_id
        LEFT JOIN crm.lu_entity_type et ON et.entity_type_id = a.entity_type_id AND et.org_client_id = a.org_client_id
        WHERE t.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
        """;

    public List<Map<String, Object>> listTasks(UUID orgClientId, UUID assignedToUserId, String view,
                                               String search, UUID taskTypeId, UUID taskStatusId, UUID taskPriorityId,
                                               String sort, String order, int limit, int offset) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (assignedToUserId != null) {
            sql.append(" AND a.assigned_to_user_id = ? ");
            params.add(assignedToUserId);
        }
        if (view != null && !view.isBlank()) {
            switch (view.toLowerCase()) {
                case "today" -> {
                    sql.append(" AND a.start_date_time::date = CURRENT_DATE ");
                }
                case "overdue" -> {
                    sql.append(" AND a.start_date_time::date < CURRENT_DATE ");
                    sql.append(" AND (ts.code IS NULL OR UPPER(ts.code) NOT IN ('COMPLETED','DONE','CLOSED')) ");
                }
                case "week" -> {
                    sql.append(" AND a.start_date_time::date >= CURRENT_DATE AND a.start_date_time::date <= CURRENT_DATE + INTERVAL '7 days' ");
                }
                default -> { /* all: no date filter */ }
            }
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.subject ILIKE ? OR t.comments ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        if (taskTypeId != null) { sql.append(" AND t.task_type_id = ? "); params.add(taskTypeId); }
        if (taskStatusId != null) { sql.append(" AND t.task_status_id = ? "); params.add(taskStatusId); }
        if (taskPriorityId != null) { sql.append(" AND t.task_priority_id = ? "); params.add(taskPriorityId); }
        appendOrderBy(sql, sort, order);
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /** Append ORDER BY from sort/order params. sort: due_date, subject, status, priority, assigned_to, created_on, entity. */
    private void appendOrderBy(StringBuilder sql, String sort, String order) {
        boolean desc = order != null && "desc".equalsIgnoreCase(order.trim());
        String dir = desc ? " DESC" : " ASC";
        String nulls = desc ? " NULLS FIRST" : " NULLS LAST";
        String col;
        if (sort != null && !sort.isBlank()) {
            switch (sort.toLowerCase().trim()) {
                case "subject" -> col = "a.subject";
                case "status" -> col = "ts.display_name";
                case "priority" -> col = "tp.display_name";
                case "assigned_to" -> col = "assigned_to_display_name";
                case "created_on" -> col = "t.created_on";
                case "entity" -> col = "entity_display_name";
                case "due_date", "due" -> col = "a.start_date_time";
                default -> col = "a.start_date_time";
            }
        } else {
            col = "a.start_date_time";
        }
        sql.append(" ORDER BY ").append(col).append(dir).append(nulls);
        if (!"t.created_on".equals(col)) {
            sql.append(", t.created_on DESC NULLS LAST");
        }
        sql.append(" LIMIT ? OFFSET ? ");
    }

    /** Tasks due today or overdue, not completed. For CRM Home "Today's Tasks" widget. */
    public List<Map<String, Object>> listTasksForHome(UUID orgClientId, UUID assignedToUserId, int limit) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (assignedToUserId != null) {
            sql.append(" AND a.assigned_to_user_id = ? ");
            params.add(assignedToUserId);
        }
        sql.append(" AND a.start_date_time::date <= CURRENT_DATE ");
        sql.append(" AND (ts.code IS NULL OR UPPER(ts.code) NOT IN ('COMPLETED','DONE','CLOSED')) ");
        sql.append(" ORDER BY a.start_date_time ASC NULLS LAST, t.created_on DESC NULLS LAST LIMIT ? ");
        params.add(limit);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countTasks(UUID orgClientId, UUID assignedToUserId, String view,
                           String search, UUID taskTypeId, UUID taskStatusId, UUID taskPriorityId) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*) FROM crm.activity_task t
            INNER JOIN crm.activity a ON a.activity_id = t.activity_id AND a.org_client_id = t.org_client_id
            LEFT JOIN crm.lu_task_status ts ON ts.task_status_id = t.task_status_id
            WHERE t.org_client_id = ? AND COALESCE(a.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (assignedToUserId != null) { sql.append(" AND a.assigned_to_user_id = ? "); params.add(assignedToUserId); }
        if (view != null && !view.isBlank()) {
            switch (view.toLowerCase()) {
                case "today" -> sql.append(" AND a.start_date_time::date = CURRENT_DATE ");
                case "overdue" -> {
                    sql.append(" AND a.start_date_time::date < CURRENT_DATE ");
                    sql.append(" AND (ts.code IS NULL OR UPPER(ts.code) NOT IN ('COMPLETED','DONE','CLOSED')) ");
                }
                case "week" -> sql.append(" AND a.start_date_time::date >= CURRENT_DATE AND a.start_date_time::date <= CURRENT_DATE + INTERVAL '7 days' ");
                default -> { }
            }
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.subject ILIKE ? OR t.comments ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        if (taskTypeId != null) { sql.append(" AND t.task_type_id = ? "); params.add(taskTypeId); }
        if (taskStatusId != null) { sql.append(" AND t.task_status_id = ? "); params.add(taskStatusId); }
        if (taskPriorityId != null) { sql.append(" AND t.task_priority_id = ? "); params.add(taskPriorityId); }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String DETAIL_SELECT = """
        SELECT t.task_id, a.subject, a.start_date_time AS due_date,
               t.task_type_id, tt.display_name AS task_type_display_name,
               t.task_status_id, ts.display_name AS task_status_display_name,
               t.task_priority_id, tp.display_name AS task_priority_display_name,
               a.assigned_to_user_id,
               TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS assigned_to_display_name,
               t.reminder_set, t.reminder_time, t.comments,
               a.entity_type_id, a.entity_id, """ + ENTITY_DISPLAY_SQL + " AS entity_display_name,\n" + """
               NULL::uuid AS related_entity_type_id, NULL::uuid AS related_entity_id, NULL::text AS related_entity_display_name,
               t.activity_id, t.created_on, t.modified_on
        FROM crm.activity_task t
        INNER JOIN crm.activity a ON a.activity_id = t.activity_id AND a.org_client_id = t.org_client_id
        LEFT JOIN crm.lu_task_type tt ON tt.task_type_id = t.task_type_id
        LEFT JOIN crm.lu_task_status ts ON ts.task_status_id = t.task_status_id
        LEFT JOIN crm.lu_task_priority tp ON tp.task_priority_id = t.task_priority_id
        LEFT JOIN "access".access_user u ON u.user_id = a.assigned_to_user_id
        WHERE t.org_client_id = ? AND t.task_id = ? AND COALESCE(a.is_deleted, false) = false
        """;

    public Map<String, Object> findTaskById(UUID orgClientId, UUID taskId) {
        List<Map<String, Object>> list = jdbc.queryForList(DETAIL_SELECT, orgClientId, taskId);
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean taskExists(UUID orgClientId, UUID taskId) {
        if (taskId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.activity_task t INNER JOIN crm.activity a ON a.activity_id = t.activity_id WHERE t.org_client_id = ? AND t.task_id = ? AND COALESCE(a.is_deleted, false) = false)",
                Boolean.class, orgClientId, taskId);
        return Boolean.TRUE.equals(b);
    }

    public boolean taskTypeExists(UUID orgClientId, UUID taskTypeId) {
        if (taskTypeId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_task_type WHERE org_client_id = ? AND task_type_id = ? AND is_active = true)",
                Boolean.class, orgClientId, taskTypeId);
        return Boolean.TRUE.equals(b);
    }

    public boolean taskStatusExists(UUID orgClientId, UUID taskStatusId) {
        if (taskStatusId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_task_status WHERE org_client_id = ? AND task_status_id = ? AND is_active = true)",
                Boolean.class, orgClientId, taskStatusId);
        return Boolean.TRUE.equals(b);
    }

    public boolean taskPriorityExists(UUID orgClientId, UUID taskPriorityId) {
        if (taskPriorityId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_task_priority WHERE org_client_id = ? AND task_priority_id = ? AND is_active = true)",
                Boolean.class, orgClientId, taskPriorityId);
        return Boolean.TRUE.equals(b);
    }

    /** Default task status by code (e.g. NOT_STARTED). */
    public UUID resolveTaskStatusIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) return null;
        List<UUID> list = jdbc.query(
                "SELECT task_status_id FROM crm.lu_task_status WHERE org_client_id = ? AND UPPER(TRIM(code)) = UPPER(?) AND is_active = true LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("task_status_id"), orgClientId, code.trim());
        return list.isEmpty() ? null : list.get(0);
    }

    /** First entity type for org (for standalone task activity). */
    public UUID getDefaultEntityTypeId(UUID orgClientId) {
        List<UUID> list = jdbc.query(
                "SELECT entity_type_id FROM crm.lu_entity_type WHERE org_client_id = ? AND is_active = true ORDER BY display_order, display_name LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("entity_type_id"), orgClientId);
        return list.isEmpty() ? null : list.get(0);
    }

    /** Returns true if activity exists and belongs to org. */
    public boolean activityExists(UUID orgClientId, UUID activityId) {
        if (activityId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.activity WHERE org_client_id = ? AND activity_id = ? AND COALESCE(is_deleted, false) = false)",
                Boolean.class, orgClientId, activityId);
        return Boolean.TRUE.equals(b);
    }

    /**
     * Create activity + activity_task, or link to existing activity. Returns task_id.
     * @param existingActivityId if non-null, use this activity (must exist and belong to org) and only insert activity_task.
     * @param entityTypeId required when existingActivityId is null.
     * @param entityId required when existingActivityId is null.
     */
    public UUID insertTask(UUID orgClientId, UUID activityTypeId, UUID activityStatusId, UUID visibilityId,
                           UUID entityTypeId, UUID entityId, UUID relatedEntityTypeId, UUID relatedEntityId, UUID existingActivityId,
                           String subject, Timestamp dueDateTime, UUID assignedToUserId, UUID ownerUserId,
                           UUID taskTypeId, UUID taskStatusId, UUID taskPriorityId,
                           boolean reminderSet, Timestamp reminderTime, String comments, UUID createdBy) {
        UUID activityId;
        if (existingActivityId != null) {
            if (!activityExists(orgClientId, existingActivityId)) throw new IllegalArgumentException("activity_id not found or not accessible");
            activityId = existingActivityId;
        } else {
            if (entityTypeId == null || entityId == null) throw new IllegalArgumentException("entity_type_id and entity_id are required when not linking to an existing activity");
            activityId = UUID.randomUUID();
            jdbc.update("""
                INSERT INTO crm.activity (
                    activity_id, org_client_id, entity_type_id, entity_id,
                    activity_type_id, activity_status_id, activity_visibility_id, activity_outcome_id,
                    subject, description, notes, start_date_time, end_date_time,
                    owner_user_id, assigned_to_user_id, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, ?)
                """, activityId, orgClientId, entityTypeId, entityId,
                    activityTypeId, activityStatusId, visibilityId,
                    subject, dueDateTime, dueDateTime,
                    ownerUserId != null ? ownerUserId : createdBy, assignedToUserId != null ? assignedToUserId : createdBy, createdBy);
        }
        UUID taskId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.activity_task (
                task_id, org_client_id, task_type_id, task_status_id, task_priority_id,
                reminder_set, reminder_time, comments, tags, attributes, activity_id, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]'::jsonb, '{}'::jsonb, ?, ?)
            """, taskId, orgClientId, taskTypeId, taskStatusId, taskPriorityId,
                reminderSet, reminderTime, comments, activityId, createdBy);
        return taskId;
    }

    /**
     * Update task: activity (subject, start_date_time, assigned_to) and activity_task (type, status, priority, reminder, comments).
     */
    public int updateTask(UUID orgClientId, UUID taskId, Map<String, Object> activityUpdates, Map<String, Object> taskUpdates, UUID modifiedBy) {
        Map<String, Object> row = findTaskById(orgClientId, taskId);
        if (row == null) return 0;
        Object activityIdObj = row.get("activity_id");
        if (!(activityIdObj instanceof UUID activityId)) return 0;
        int n = 0;
        if (activityUpdates != null && !activityUpdates.isEmpty()) {
            List<String> setClauses = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            if (activityUpdates.containsKey("subject")) { setClauses.add("subject = ?"); params.add(activityUpdates.get("subject")); }
            if (activityUpdates.containsKey("start_date_time")) { setClauses.add("start_date_time = ?"); params.add(activityUpdates.get("start_date_time")); }
            if (activityUpdates.containsKey("end_date_time")) { setClauses.add("end_date_time = ?"); params.add(activityUpdates.get("end_date_time")); }
            if (activityUpdates.containsKey("assigned_to_user_id")) { setClauses.add("assigned_to_user_id = ?"); params.add(activityUpdates.get("assigned_to_user_id")); }
            if (activityUpdates.containsKey("entity_type_id")) { setClauses.add("entity_type_id = ?"); params.add(activityUpdates.get("entity_type_id")); }
            if (activityUpdates.containsKey("entity_id")) { setClauses.add("entity_id = ?"); params.add(activityUpdates.get("entity_id")); }
            if (activityUpdates.containsKey("related_entity_type_id")) { setClauses.add("related_entity_type_id = ?"); params.add(activityUpdates.get("related_entity_type_id")); }
            if (activityUpdates.containsKey("related_entity_id")) { setClauses.add("related_entity_id = ?"); params.add(activityUpdates.get("related_entity_id")); }
            if (!setClauses.isEmpty()) {
                setClauses.add("modified_by = ?");
                params.add(modifiedBy);
                params.add(orgClientId);
                params.add(activityId);
                n += jdbc.update("UPDATE crm.activity SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND activity_id = ?", params.toArray());
            }
        }
        if (taskUpdates != null && !taskUpdates.isEmpty()) {
            List<String> setClauses = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            if (taskUpdates.containsKey("task_type_id")) { setClauses.add("task_type_id = ?"); params.add(taskUpdates.get("task_type_id")); }
            if (taskUpdates.containsKey("task_status_id")) { setClauses.add("task_status_id = ?"); params.add(taskUpdates.get("task_status_id")); }
            if (taskUpdates.containsKey("task_priority_id")) { setClauses.add("task_priority_id = ?"); params.add(taskUpdates.get("task_priority_id")); }
            if (taskUpdates.containsKey("reminder_set")) { setClauses.add("reminder_set = ?"); params.add(taskUpdates.get("reminder_set")); }
            if (taskUpdates.containsKey("reminder_time")) { setClauses.add("reminder_time = ?"); params.add(taskUpdates.get("reminder_time")); }
            if (taskUpdates.containsKey("comments")) { setClauses.add("comments = ?"); params.add(taskUpdates.get("comments")); }
            if (!setClauses.isEmpty()) {
                setClauses.add("modified_by = ?");
                params.add(modifiedBy);
                params.add(orgClientId);
                params.add(taskId);
                n += jdbc.update("UPDATE crm.activity_task SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND task_id = ?", params.toArray());
            }
        }
        return n;
    }

    /** Soft-delete: mark parent activity as deleted. */
    public int deleteTask(UUID orgClientId, UUID taskId, UUID deletedBy) {
        List<UUID> activityIds = jdbc.query(
                "SELECT activity_id FROM crm.activity_task WHERE org_client_id = ? AND task_id = ?",
                (rs, i) -> (UUID) rs.getObject("activity_id"), orgClientId, taskId);
        if (activityIds.isEmpty()) return 0;
        return jdbc.update(
                "UPDATE crm.activity SET is_deleted = true, modified_by = ? WHERE org_client_id = ? AND activity_id = ?",
                deletedBy, orgClientId, activityIds.get(0));
    }
}
