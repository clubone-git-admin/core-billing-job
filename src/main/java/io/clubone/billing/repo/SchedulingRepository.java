package io.clubone.billing.repo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for scheduling operations.
 */
@Repository
public class SchedulingRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public SchedulingRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Find billing schedules with filtering.
     */
    public List<Map<String, Object>> findSchedules(Boolean isActive, UUID locationId, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                bs.schedule_id,
                bs.schedule_name,
                bs.schedule_type,
                bs.due_date_pattern,
                bs.due_date_cron,
                bs.location_id,
                l.name AS location_name,
                bs.is_active,
                bs.next_run_at,
                bs.last_run_id,
                br.billing_run_code AS last_run_code,
                bs.last_run_on,
                bs.config,
                bs.created_by,
                bs.created_on
            FROM client_subscription_billing.billing_schedule bs
            LEFT JOIN locations.location l ON l.location_id = bs.location_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bs.last_run_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (isActive != null) {
            sql.append(" AND bs.is_active = ?");
            params.add(isActive);
        }

        if (locationId != null) {
            sql.append(" AND bs.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        sql.append(" ORDER BY bs.created_on DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count billing schedules.
     */
    public Integer countSchedules(Boolean isActive, UUID locationId) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_schedule bs
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (isActive != null) {
            sql.append(" AND bs.is_active = ?");
            params.add(isActive);
        }

        if (locationId != null) {
            sql.append(" AND bs.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find schedule by ID.
     */
    public Map<String, Object> findById(UUID scheduleId) {
        String sql = """
            SELECT 
                bs.schedule_id,
                bs.schedule_name,
                bs.schedule_type,
                bs.due_date_pattern,
                bs.due_date_cron,
                bs.location_id,
                l.name AS location_name,
                bs.is_active,
                bs.next_run_at,
                bs.last_run_id,
                br.billing_run_code AS last_run_code,
                bs.last_run_on,
                bs.config,
                bs.created_by,
                bs.created_on
            FROM client_subscription_billing.billing_schedule bs
            LEFT JOIN locations.location l ON l.location_id = bs.location_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bs.last_run_id
            WHERE bs.schedule_id = ?::uuid
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, scheduleId.toString());
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Create a new schedule.
     */
    public UUID createSchedule(Map<String, Object> request) {
        UUID scheduleId = UUID.randomUUID();

        String scheduleName = (String) request.get("schedule_name");
        String scheduleType = (String) request.get("schedule_type");
        String dueDatePattern = (String) request.get("due_date_pattern");
        String dueDateCron = (String) request.get("due_date_cron");
        UUID locationId = request.get("location_id") != null ? UUID.fromString(request.get("location_id").toString()) : null;
        Boolean isActive = (Boolean) request.getOrDefault("is_active", true);
        UUID createdBy = request.get("created_by") != null ? UUID.fromString(request.get("created_by").toString()) : null;

        Map<String, Object> config = (Map<String, Object>) request.getOrDefault("config", Map.of());
        String configJson = null;
        try {
            configJson = objectMapper.writeValueAsString(config);
        } catch (Exception e) {
            // Log error
        }

        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_schedule
            (schedule_id, schedule_name, schedule_type, due_date_pattern, due_date_cron,
             location_id, is_active, config, created_by)
            VALUES (?::uuid, ?, ?, ?, ?, ?::uuid, ?, ?::jsonb, ?::uuid)
            """,
                scheduleId.toString(), scheduleName, scheduleType, dueDatePattern, dueDateCron,
                locationId != null ? locationId.toString() : null, isActive, configJson,
                createdBy != null ? createdBy.toString() : null);

        return scheduleId;
    }

    /**
     * Update schedule.
     */
    public void updateSchedule(UUID scheduleId, Map<String, Object> request) {
        StringBuilder sql = new StringBuilder("UPDATE client_subscription_billing.billing_schedule SET modified_on = now()");
        List<Object> params = new ArrayList<>();

        if (request.containsKey("schedule_name")) {
            sql.append(", schedule_name = ?");
            params.add(request.get("schedule_name"));
        }

        if (request.containsKey("is_active")) {
            sql.append(", is_active = ?");
            params.add(request.get("is_active"));
        }

        if (request.containsKey("config")) {
            try {
                String configJson = objectMapper.writeValueAsString(request.get("config"));
                sql.append(", config = ?::jsonb");
                params.add(configJson);
            } catch (Exception e) {
                // Log error
            }
        }

        sql.append(" WHERE schedule_id = ?::uuid");
        params.add(scheduleId.toString());

        jdbc.update(sql.toString(), params.toArray());
    }

    /**
     * Delete schedule.
     */
    public boolean deleteSchedule(UUID scheduleId) {
        int rows = jdbc.update("DELETE FROM client_subscription_billing.billing_schedule WHERE schedule_id = ?::uuid",
                scheduleId.toString());
        return rows > 0;
    }

    /**
     * Update last run information.
     */
    public void updateLastRun(UUID scheduleId, UUID billingRunId, String billingRunCode) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_schedule
            SET last_run_id = ?::uuid, last_run_code = ?, last_run_on = now()
            WHERE schedule_id = ?::uuid
            """,
                billingRunId != null ? billingRunId.toString() : null, billingRunCode, scheduleId.toString());
    }

    /**
     * Get billing runs created by a schedule.
     */
    public List<Map<String, Object>> getScheduleRuns(UUID scheduleId, Integer limit, Integer offset) {
        // Note: This requires tracking which runs were created by which schedule
        // For now, we'll query runs that match the schedule's pattern
        String sql = """
            SELECT 
                br.billing_run_id,
                br.billing_run_code,
                br.due_date,
                br.started_on,
                br.ended_on
            FROM client_subscription_billing.billing_run br
            WHERE EXISTS (
                SELECT 1 FROM client_subscription_billing.billing_schedule bs
                WHERE bs.schedule_id = ?::uuid
                AND br.location_id = bs.location_id
            )
            ORDER BY br.created_on DESC
            LIMIT ? OFFSET ?
            """;

        return jdbc.queryForList(sql, scheduleId.toString(), limit, offset);
    }
}
