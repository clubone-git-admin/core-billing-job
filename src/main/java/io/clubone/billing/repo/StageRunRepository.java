package io.clubone.billing.repo;

import io.clubone.billing.api.dto.StageRunDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for stage run operations.
 */
@Repository
public class StageRunRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public StageRunRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Find all stages for a billing run.
     */
    public List<StageRunDto> findByBillingRunId(UUID billingRunId) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid
            ORDER BY bsc.stage_sequence ASC, bsr.created_on ASC, bsr.stage_run_id ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString()}, (rs, rowNum) -> mapStageRunRow(rs));
    }

    /**
     * Find stage run by stage_run_id.
     */
    public StageRunDto findById(UUID stageRunId) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.stage_run_id = ?::uuid
            """;
        List<StageRunDto> results = jdbc.query(sql, new Object[]{stageRunId.toString()}, (rs, rowNum) -> mapStageRunRow(rs));
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Find stage by billing run ID and stage code.
     */
    public StageRunDto findByBillingRunIdAndStageCode(UUID billingRunId, String stageCode) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid AND bsc.stage_code = ?
            ORDER BY bsr.created_on DESC
            LIMIT 1
            """;

        List<StageRunDto> results = jdbc.query(sql, new Object[]{billingRunId.toString(), stageCode}, (rs, rowNum) -> mapStageRunRow(rs));

        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Stage runs for a billing run and stage code whose status is not terminal (multiple executions per stage,
     * e.g. MOCK_CHARGE retries). Ordered by {@code created_on} ascending.
     */
    public List<StageRunDto> findNonTerminalByBillingRunIdAndStageCode(UUID billingRunId, String stageCode) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid AND bsc.stage_code = ?
              AND srs.status_code NOT IN ('COMPLETED', 'SKIPPED', 'FAILED', 'CANCELLED')
            ORDER BY bsr.created_on ASC, bsr.stage_run_id ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString(), stageCode}, (rs, rowNum) -> mapStageRunRow(rs));
    }

    /**
     * Find stage run by idempotency key (unique when set).
     */
    public StageRunDto findByIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return null;
        }
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.idempotency_key = ?
            LIMIT 1
            """;
        List<StageRunDto> results = jdbc.query(sql, new Object[]{idempotencyKey}, (rs, rowNum) -> mapStageRunRow(rs));
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * All stage runs for a billing run and stage code, e.g. invoice generation history.
     *
     * @param sortBy    {@code created_on} (default), {@code started_on}/{@code started_at}, {@code ended_on}/{@code ended_at}
     * @param sortOrder {@code asc} or {@code desc}
     */
    public List<StageRunDto> listByBillingRunIdAndStageCode(
            UUID billingRunId, String stageCode, String statusCode, int limit, int offset,
            String sortBy, String sortOrder) {
        StringBuilder sql = new StringBuilder("""
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid AND bsc.stage_code = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        params.add(stageCode);
        if (statusCode != null && !statusCode.isBlank()) {
            sql.append(" AND srs.status_code = ?");
            params.add(statusCode);
        }
        String orderCol = orderColumnForStageList(sortBy);
        String ord = "asc".equalsIgnoreCase(sortOrder) ? "ASC" : "DESC";
        sql.append(" ORDER BY ").append(orderCol).append(" ").append(ord).append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> mapStageRunRow(rs));
    }

    private static String orderColumnForStageList(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "bsr.created_on";
        }
        return switch (sortBy.toLowerCase()) {
            case "started_at", "started_on" -> "bsr.started_on";
            case "ended_at", "ended_on" -> "bsr.ended_on";
            default -> "bsr.created_on";
        };
    }

    public int countByBillingRunIdAndStageCode(UUID billingRunId, String stageCode, String statusCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid AND bsc.stage_code = ?
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        params.add(stageCode);
        if (statusCode != null && !statusCode.isBlank()) {
            sql.append(" AND srs.status_code = ?");
            params.add(statusCode);
        }
        Integer n = jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    /**
     * Shallow-merge {@code patch} into {@code billing_stage_run.summary_json} (top-level keys).
     */
    public void mergeStageRunSummaryJson(UUID stageRunId, Map<String, Object> patch) {
        mergeStageRunSummaryJson(stageRunId, patch, false);
    }

    /**
     * Same as {@link #mergeStageRunSummaryJson(UUID, Map)}, optionally sets {@code is_locked = true} on the row.
     */
    public void mergeStageRunSummaryJson(UUID stageRunId, Map<String, Object> patch, boolean setIsLocked) {
        boolean hasPatch = patch != null && !patch.isEmpty();
        if (!hasPatch && !setIsLocked) {
            return;
        }
        try {
            if (hasPatch && setIsLocked) {
                String patchStr = objectMapper.writeValueAsString(patch);
                jdbc.update(
                        """
                        UPDATE client_subscription_billing.billing_stage_run
                        SET summary_json = COALESCE(summary_json, '{}'::jsonb) || ?::jsonb,
                            is_locked = true,
                            modified_on = now()
                        WHERE stage_run_id = ?::uuid
                        """,
                        patchStr,
                        stageRunId.toString());
            } else if (hasPatch) {
                String patchStr = objectMapper.writeValueAsString(patch);
                jdbc.update(
                        """
                        UPDATE client_subscription_billing.billing_stage_run
                        SET summary_json = COALESCE(summary_json, '{}'::jsonb) || ?::jsonb,
                            modified_on = now()
                        WHERE stage_run_id = ?::uuid
                        """,
                        patchStr,
                        stageRunId.toString());
            } else {
                jdbc.update(
                        """
                        UPDATE client_subscription_billing.billing_stage_run
                        SET is_locked = true, modified_on = now()
                        WHERE stage_run_id = ?::uuid
                        """,
                        stageRunId.toString());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to merge billing_stage_run summary_json: " + e.getMessage(), e);
        }
    }

    /**
     * Update summary_json without changing status (used when starting invoice generation with options).
     */
    public void updateStageRunSummary(UUID stageRunId, Map<String, Object> summaryJson) {
        try {
            if (summaryJson == null) {
                jdbc.update("""
                    UPDATE client_subscription_billing.billing_stage_run
                    SET summary_json = NULL, modified_on = now()
                    WHERE stage_run_id = ?::uuid
                    """, stageRunId.toString());
                return;
            }
            String jsonStr = objectMapper.writeValueAsString(summaryJson);
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET summary_json = ?::jsonb, modified_on = now()
                WHERE stage_run_id = ?::uuid
                """, jsonStr, stageRunId.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to update stage run summary", e);
        }
    }

    /**
     * Persist idempotency key on an existing stage run (when previously null).
     */
    public void updateIdempotencyKey(UUID stageRunId, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return;
        }
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET idempotency_key = ?, modified_on = now()
            WHERE stage_run_id = ?::uuid AND (idempotency_key IS NULL OR idempotency_key = '')
            """, idempotencyKey, stageRunId.toString());
    }

    /**
     * Mark stage run cancelled when {@code CANCELLED} exists in {@code billing_config.stage_run_status}; otherwise failed with reason.
     */
    public void cancelStageRun(UUID stageRunId, String reason) {
        List<UUID> cancelledIds = jdbc.query(
                "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = 'CANCELLED' LIMIT 1",
                (rs, rowNum) -> (UUID) rs.getObject(1));
        if (cancelledIds.isEmpty()) {
            failStageRun(stageRunId, reason != null ? reason : "Cancelled", Map.of("cancelled", true));
            return;
        }
        UUID statusId = cancelledIds.get(0);
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET stage_run_status_id = ?::uuid, ended_on = now(), error_message = ?, modified_on = now()
            WHERE stage_run_id = ?::uuid
            """, statusId.toString(), reason, stageRunId.toString());
    }

    private StageRunDto mapStageRunRow(java.sql.ResultSet rs) throws SQLException {
        UUID id = (UUID) rs.getObject("stage_run_id");
        String stageRunCode = rs.getString("stage_run_code");
        UUID billingRunIdResult = (UUID) rs.getObject("billing_run_id");
        String sc = rs.getString("stage_code");
        String stageDisplayName = rs.getString("stage_display_name");
        Integer stageSequence = rs.getInt("stage_sequence");
        String statusCode = rs.getString("status_code");
        String statusDisplayName = rs.getString("status_display_name");
        OffsetDateTime scheduledFor = rs.getObject("scheduled_for", OffsetDateTime.class);
        OffsetDateTime startedOn = rs.getObject("started_on", OffsetDateTime.class);
        OffsetDateTime endedOn = rs.getObject("ended_on", OffsetDateTime.class);
        Map<String, Object> summaryJson = null;
        String summaryJsonStr = rs.getString("summary_json");
        if (summaryJsonStr != null) {
            try {
                summaryJson = objectMapper.readValue(summaryJsonStr, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) { /* ignore */ }
        }
        String errorMessage = rs.getString("error_message");
        Map<String, Object> errorDetails = null;
        String errorDetailsStr = rs.getString("error_details");
        if (errorDetailsStr != null) {
            try {
                errorDetails = objectMapper.readValue(errorDetailsStr, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) { /* ignore */ }
        }
        Integer attemptNumber = rs.getInt("attempt_number");
        Integer maxAttempts = rs.getInt("max_attempts");
        Boolean isLocked = readNullableBoolean(rs, "is_locked");
        return new StageRunDto(id, stageRunCode, billingRunIdResult, sc, stageDisplayName,
                stageSequence, statusCode, statusDisplayName, scheduledFor, startedOn, endedOn,
                summaryJson, errorMessage, errorDetails, attemptNumber, maxAttempts, isLocked);
    }

    private static Boolean readNullableBoolean(java.sql.ResultSet rs, String column) throws SQLException {
        boolean v = rs.getBoolean(column);
        return rs.wasNull() ? null : v;
    }

    /**
     * Create a new stage run.
     *
     * @param setStartedOnAtInsert when {@code false}, {@code started_on} is left null (for async-queued invoice generation).
     */
    public UUID createStageRun(
            UUID billingRunId, String stageCode, OffsetDateTime scheduledFor,
            String idempotencyKey, UUID createdBy) {
        return createStageRun(billingRunId, stageCode, scheduledFor, idempotencyKey, createdBy, true);
    }

    public UUID createStageRun(
            UUID billingRunId, String stageCode, OffsetDateTime scheduledFor,
            String idempotencyKey, UUID createdBy, boolean setStartedOnAtInsert) {

        UUID stageRunId = UUID.randomUUID();

        // Get stage code ID
        String stageIdSql = "SELECT billing_stage_code_id FROM billing_config.billing_stage_code WHERE stage_code = ?";
        UUID stageId = jdbc.queryForObject(stageIdSql, new Object[]{stageCode}, UUID.class);

        // Get default status (PENDING)
        String statusIdSql = "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = 'PENDING'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        // Generate stage run code
        String stageRunCode = "STG-" + OffsetDateTime.now().getYear() + "-" + String.format("%06d", (int)(Math.random() * 1000000));

        if (setStartedOnAtInsert) {
            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_stage_run
                (stage_run_id, stage_run_code, billing_run_id, stage_code_id, stage_run_status_id,
                  started_on, attempt_number, max_attempts, idempotency_key, created_by)
                VALUES (?::uuid, ?, ?::uuid, ?::uuid, ?::uuid, now(), 1, 1, ?, ?::uuid)
                """,
                    stageRunId.toString(), stageRunCode, billingRunId.toString(),
                    stageId.toString(), statusId.toString(),
                    idempotencyKey, createdBy != null ? createdBy.toString() : null);
        } else {
            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_stage_run
                (stage_run_id, stage_run_code, billing_run_id, stage_code_id, stage_run_status_id,
                  started_on, attempt_number, max_attempts, idempotency_key, created_by)
                VALUES (?::uuid, ?, ?::uuid, ?::uuid, ?::uuid, NULL, 1, 1, ?, ?::uuid)
                """,
                    stageRunId.toString(), stageRunCode, billingRunId.toString(),
                    stageId.toString(), statusId.toString(),
                    idempotencyKey, createdBy != null ? createdBy.toString() : null);
        }

        return stageRunId;
    }

    /**
     * Set stage run status by code (e.g. IDLE, QUEUED, RUNNING, WAITING). Returns false if status code does not exist.
     */
    public boolean trySetStageRunStatusByCode(UUID stageRunId, String statusCode) {
        List<UUID> ids = jdbc.query(
                "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = ? LIMIT 1",
                (rs, rowNum) -> (UUID) rs.getObject(1),
                statusCode);
        if (ids.isEmpty()) {
            return false;
        }
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET stage_run_status_id = ?::uuid, modified_on = now()
            WHERE stage_run_id = ?::uuid
            """, ids.get(0).toString(), stageRunId.toString());
        return true;
    }

    /**
     * Update stage run status to RUNNING.
     */
    public void startStageRun(UUID stageRunId) {
        String statusIdSql = "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = 'RUNNING'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET stage_run_status_id = ?::uuid, started_on = now(), modified_on = now()
            WHERE stage_run_id = ?::uuid
            """,
                statusId.toString(), stageRunId.toString());
    }

    /**
     * Next active pipeline stage after {@code stageCode} (by {@code stage_sequence}), or {@code null} if none.
     */
    public String findNextActiveStageCodeAfter(String stageCode) {
        if (stageCode == null || stageCode.isBlank()) {
            return null;
        }
        List<String> rows = jdbc.query(
                """
                SELECT b2.stage_code
                FROM billing_config.billing_stage_code b1
                JOIN billing_config.billing_stage_code b2
                  ON b2.is_active = true AND b2.stage_sequence > b1.stage_sequence
                WHERE b1.stage_code = ? AND b1.is_active = true
                ORDER BY b2.stage_sequence ASC
                LIMIT 1
                """,
                (rs, rowNum) -> rs.getString(1),
                stageCode);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Complete stage run.
     */
    public void completeStageRun(UUID stageRunId, Map<String, Object> summaryJson) {
        String statusIdSql = "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = 'COMPLETED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        try {
            String jsonStr = summaryJson != null ? objectMapper.writeValueAsString(summaryJson) : null;
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), summary_json = ?::jsonb, modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                    statusId.toString(), jsonStr, stageRunId.toString());
        } catch (Exception e) {
            // Log error
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                        statusId.toString(), stageRunId.toString());
        }
    }

    /**
     * Terminal finish with a specific {@code status_code} (e.g. {@code SKIPPED}, {@code COMPLETED}).
     *
     * @return {@code false} if {@code statusCode} is not present in {@code billing_config.stage_run_status}
     */
    public boolean tryFinishStageRun(UUID stageRunId, String statusCode, Map<String, Object> summaryJson) {
        List<UUID> statusIds = jdbc.query(
                "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = ? LIMIT 1",
                (rs, rowNum) -> (UUID) rs.getObject(1),
                statusCode);
        if (statusIds.isEmpty()) {
            return false;
        }
        UUID statusId = statusIds.get(0);
        try {
            String jsonStr = summaryJson != null ? objectMapper.writeValueAsString(summaryJson) : null;
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), summary_json = ?::jsonb, modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                    statusId.toString(), jsonStr, stageRunId.toString());
        } catch (Exception e) {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                    statusId.toString(), stageRunId.toString());
        }
        return true;
    }

    /**
     * Fail stage run.
     */
    public void failStageRun(UUID stageRunId, String errorMessage, Map<String, Object> errorDetails) {
        String statusIdSql = "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = 'FAILED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        try {
            String errorDetailsStr = errorDetails != null ? objectMapper.writeValueAsString(errorDetails) : null;
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), error_message = ?, error_details = ?::jsonb, modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                    statusId.toString(), errorMessage, errorDetailsStr, stageRunId.toString());
        } catch (Exception e) {
            // Log error
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), error_message = ?, modified_on = now()
                WHERE stage_run_id = ?::uuid
                """,
                        statusId.toString(), errorMessage, stageRunId.toString());
        }
    }
}
