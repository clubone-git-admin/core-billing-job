package io.clubone.billing.repo;

import io.clubone.billing.api.dto.StageRunDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

import io.clubone.billing.security.AccessContext;
import io.clubone.billing.security.TenantContext;
import io.clubone.billing.security.TenantContexts;
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

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    /**
     * Auth-free lookup for async/batch workers: resolve application + location from the stage run row
     * (no {@link AccessContext} / request ThreadLocal required).
     *
     * @return background {@link io.clubone.billing.security.TenantContext}, or null if stage run missing
     */
    public TenantContext resolveBackgroundTenant(UUID stageRunId) {
        if (stageRunId == null) {
            return null;
        }
        List<TenantContext> rows = jdbc.query(
                """
                SELECT bsr.application_id, br.location_id
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN client_subscription_billing.billing_run br
                  ON br.billing_run_id = bsr.billing_run_id
                WHERE bsr.stage_run_id = ?::uuid
                LIMIT 1
                """,
                (rs, i) -> TenantContexts.forBackgroundJob(
                        (UUID) rs.getObject("application_id"),
                        (UUID) rs.getObject("location_id")),
                stageRunId.toString());
        return rows.isEmpty() ? null : rows.get(0);
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
              AND bsr.application_id = ?::uuid
            ORDER BY bsc.stage_sequence ASC, bsr.created_on ASC, bsr.stage_run_id ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString(), requireAppIdStr()}, (rs, rowNum) -> mapStageRunRow(rs));
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
              AND bsr.application_id = ?::uuid
            """;
        List<StageRunDto> results = jdbc.query(sql, new Object[]{stageRunId.toString(), requireAppIdStr()}, (rs, rowNum) -> mapStageRunRow(rs));
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
              AND bsr.application_id = ?::uuid
            ORDER BY bsr.created_on DESC
            LIMIT 1
            """;

        List<StageRunDto> results = jdbc.query(sql, new Object[]{billingRunId.toString(), stageCode, requireAppIdStr()}, (rs, rowNum) -> mapStageRunRow(rs));

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
              AND bsr.application_id = ?::uuid
              AND srs.status_code NOT IN ('COMPLETED', 'SKIPPED', 'FAILED', 'CANCELLED')
            ORDER BY bsr.created_on ASC, bsr.stage_run_id ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString(), stageCode, requireAppIdStr()}, (rs, rowNum) -> mapStageRunRow(rs));
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
              AND bsr.application_id = ?::uuid
            LIMIT 1
            """;
        List<StageRunDto> results = jdbc.query(sql, new Object[]{idempotencyKey, requireAppIdStr()}, (rs, rowNum) -> mapStageRunRow(rs));
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
              AND bsr.application_id = ?::uuid
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        params.add(stageCode);
        params.add(requireAppIdStr());
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
              AND bsr.application_id = ?::uuid
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        params.add(stageCode);
        params.add(requireAppIdStr());
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
                        WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                        """,
                        patchStr,
                        stageRunId.toString(), requireAppIdStr());
            } else if (hasPatch) {
                String patchStr = objectMapper.writeValueAsString(patch);
                jdbc.update(
                        """
                        UPDATE client_subscription_billing.billing_stage_run
                        SET summary_json = COALESCE(summary_json, '{}'::jsonb) || ?::jsonb,
                            modified_on = now()
                        WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                        """,
                        patchStr,
                        stageRunId.toString(), requireAppIdStr());
            } else {
                jdbc.update(
                        """
                        UPDATE client_subscription_billing.billing_stage_run
                        SET is_locked = true, modified_on = now()
                        WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                        """,
                        stageRunId.toString(), requireAppIdStr());
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
                    WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                    """, stageRunId.toString(), requireAppIdStr());
                return;
            }
            String jsonStr = objectMapper.writeValueAsString(summaryJson);
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET summary_json = ?::jsonb, modified_on = now()
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """, jsonStr, stageRunId.toString(), requireAppIdStr());
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
            WHERE stage_run_id = ?::uuid AND application_id = ?::uuid AND (idempotency_key IS NULL OR idempotency_key = '')
            """, idempotencyKey, stageRunId.toString(), requireAppIdStr());
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
            SET stage_run_status_id = ?::uuid,
                ended_on = now(),
                scheduled_for = NULL,
                error_message = ?,
                modified_on = now()
            WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
            """, statusId.toString(), reason, stageRunId.toString(), requireAppIdStr());
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
                  scheduled_for, started_on, attempt_number, max_attempts, idempotency_key, created_by, application_id)
                VALUES (?::uuid, ?, ?::uuid, ?::uuid, ?::uuid, ?::timestamptz, now(), 1, 1, ?, ?::uuid, ?::uuid)
                """,
                    stageRunId.toString(), stageRunCode, billingRunId.toString(),
                    stageId.toString(), statusId.toString(),
                    scheduledFor,
                    idempotencyKey, createdBy != null ? createdBy.toString() : null, requireAppIdStr());
        } else {
            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_stage_run
                (stage_run_id, stage_run_code, billing_run_id, stage_code_id, stage_run_status_id,
                  scheduled_for, started_on, attempt_number, max_attempts, idempotency_key, created_by, application_id)
                VALUES (?::uuid, ?, ?::uuid, ?::uuid, ?::uuid, ?::timestamptz, NULL, 1, 1, ?, ?::uuid, ?::uuid)
                """,
                    stageRunId.toString(), stageRunCode, billingRunId.toString(),
                    stageId.toString(), statusId.toString(),
                    scheduledFor,
                    idempotencyKey, createdBy != null ? createdBy.toString() : null, requireAppIdStr());
        }

        return stageRunId;
    }

    /**
     * Persist / update {@code scheduled_for} (UTC timestamptz) on an existing stage run.
     */
    public void updateScheduledFor(UUID stageRunId, OffsetDateTime scheduledFor) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET scheduled_for = ?::timestamptz, modified_on = now()
            WHERE stage_run_id = ?::uuid
              AND application_id = ?::uuid
            """,
                scheduledFor, stageRunId.toString(), requireAppIdStr());
    }

    /**
     * Reuse a CANCELLED/FAILED stage for a new schedule (avoid duplicate stage rows).
     * Clears terminal fields and sets {@code scheduled_for}; caller must set status to SCHEDULED.
     */
    public void prepareStageRunForReschedule(UUID stageRunId, OffsetDateTime scheduledFor) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET scheduled_for = ?::timestamptz,
                ended_on = NULL,
                started_on = NULL,
                error_message = NULL,
                error_details = NULL,
                modified_on = now()
            WHERE stage_run_id = ?::uuid
              AND application_id = ?::uuid
            """,
                scheduledFor, stageRunId.toString(), requireAppIdStr());
    }

    /**
     * Reuse a CANCELLED/FAILED stage for an immediate re-run (avoid duplicate stage rows).
     * Clears terminal / schedule fields; caller must set status to PENDING/IDLE then enqueue.
     */
    public void prepareStageRunForRerun(UUID stageRunId) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET scheduled_for = NULL,
                ended_on = NULL,
                started_on = NULL,
                error_message = NULL,
                error_details = NULL,
                modified_on = now()
            WHERE stage_run_id = ?::uuid
              AND application_id = ?::uuid
            """,
                stageRunId.toString(), requireAppIdStr());
    }

    /**
     * Cross-tenant atomic claim: due {@code SCHEDULED} rows → {@code QUEUED} (or {@code PENDING}
     * if QUEUED is not seeded) using {@code FOR UPDATE SKIP LOCKED}.
     * Only the winning instance receives each row. Must run inside a transaction.
     */
    public List<StageRunDto> claimDueScheduledStageRuns(OffsetDateTime asOfUtc, int limit) {
        int lim = Math.max(1, Math.min(limit, 200));
        UUID claimStatusId = lookupStageRunStatusId("QUEUED");
        if (claimStatusId == null) {
            claimStatusId = lookupStageRunStatusId("PENDING");
        }
        if (claimStatusId == null) {
            return List.of();
        }
        String sql = """
            WITH due AS (
                SELECT bsr.stage_run_id
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
                WHERE srs.status_code = 'SCHEDULED'
                  AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
                  AND bsr.scheduled_for IS NOT NULL
                  AND bsr.scheduled_for <= ?::timestamptz
                ORDER BY bsr.scheduled_for ASC, bsr.created_on ASC
                LIMIT ?
                FOR UPDATE OF bsr SKIP LOCKED
            ),
            claimed AS (
                UPDATE client_subscription_billing.billing_stage_run bsr
                SET stage_run_status_id = ?::uuid,
                    modified_on = now()
                FROM due
                WHERE bsr.stage_run_id = due.stage_run_id
                RETURNING bsr.stage_run_id
            )
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM claimed c
            JOIN client_subscription_billing.billing_stage_run bsr ON bsr.stage_run_id = c.stage_run_id
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            """;
        return jdbc.query(sql, new Object[]{asOfUtc, lim, claimStatusId.toString()}, (rs, rowNum) -> mapStageRunRow(rs));
    }

    /**
     * Cross-tenant: stale {@code RUNNING} IG/mock-charge rows → {@code QUEUED}/{@code PENDING}
     * when {@code COALESCE(modified_on, started_on)} is older than {@code staleBeforeUtc}.
     * Uses {@code FOR UPDATE SKIP LOCKED}. Live workers must {@link #touchStageRun(UUID)} heartbeat.
     */
    public List<StageRunDto> claimStaleRunningStageRuns(OffsetDateTime staleBeforeUtc, int limit) {
        int lim = Math.max(1, Math.min(limit, 200));
        UUID claimStatusId = lookupStageRunStatusId("QUEUED");
        if (claimStatusId == null) {
            claimStatusId = lookupStageRunStatusId("PENDING");
        }
        if (claimStatusId == null) {
            return List.of();
        }
        String sql = """
            WITH due AS (
                SELECT bsr.stage_run_id
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
                WHERE srs.status_code = 'RUNNING'
                  AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
                  AND COALESCE(bsr.modified_on, bsr.started_on, bsr.created_on) < ?::timestamptz
                ORDER BY COALESCE(bsr.modified_on, bsr.started_on, bsr.created_on) ASC
                LIMIT ?
                FOR UPDATE OF bsr SKIP LOCKED
            ),
            claimed AS (
                UPDATE client_subscription_billing.billing_stage_run bsr
                SET stage_run_status_id = ?::uuid,
                    modified_on = now(),
                    attempt_number = COALESCE(bsr.attempt_number, 1) + 1
                FROM due
                WHERE bsr.stage_run_id = due.stage_run_id
                RETURNING bsr.stage_run_id
            )
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM claimed c
            JOIN client_subscription_billing.billing_stage_run bsr ON bsr.stage_run_id = c.stage_run_id
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            """;
        return jdbc.query(sql, new Object[]{staleBeforeUtc, lim, claimStatusId.toString()}, (rs, rowNum) -> mapStageRunRow(rs));
    }

    /**
     * Cross-tenant: stale {@code QUEUED}/{@code PENDING} rows that never reached a worker
     * (event lost after claim). Locks + bumps {@code modified_on} so only one poller redispatches.
     */
    public List<StageRunDto> claimStaleQueuedStageRuns(OffsetDateTime staleBeforeUtc, int limit) {
        int lim = Math.max(1, Math.min(limit, 200));
        String sql = """
            WITH due AS (
                SELECT bsr.stage_run_id
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
                WHERE srs.status_code IN ('QUEUED', 'PENDING')
                  AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
                  AND bsr.ended_on IS NULL
                  AND (
                      srs.status_code = 'QUEUED'
                      OR jsonb_exists(bsr.summary_json, 'queued_at')
                      OR jsonb_exists(bsr.summary_json, 'scheduled_claimed_at')
                  )
                  AND COALESCE(bsr.modified_on, bsr.created_on) < ?::timestamptz
                ORDER BY COALESCE(bsr.modified_on, bsr.created_on) ASC
                LIMIT ?
                FOR UPDATE OF bsr SKIP LOCKED
            ),
            claimed AS (
                UPDATE client_subscription_billing.billing_stage_run bsr
                SET modified_on = now()
                FROM due
                WHERE bsr.stage_run_id = due.stage_run_id
                RETURNING bsr.stage_run_id
            )
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code AS status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts, bsr.is_locked
            FROM claimed c
            JOIN client_subscription_billing.billing_stage_run bsr ON bsr.stage_run_id = c.stage_run_id
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            """;
        return jdbc.query(sql, new Object[]{staleBeforeUtc, lim}, (rs, rowNum) -> mapStageRunRow(rs));
    }

    /**
     * Worker lease heartbeat — keeps {@code modified_on} fresh so stale-reclaim does not steal a live job.
     * Auth-free (uses row id only); safe for background workers.
     */
    public void touchStageRun(UUID stageRunId) {
        if (stageRunId == null) {
            return;
        }
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET modified_on = now()
            WHERE stage_run_id = ?::uuid
            """, stageRunId.toString());
    }

    private UUID lookupStageRunStatusId(String statusCode) {
        return jdbc.query(
                "SELECT stage_run_status_id FROM billing_config.stage_run_status WHERE status_code = ? LIMIT 1",
                rs -> rs.next() ? (UUID) rs.getObject(1) : null,
                statusCode);
    }

    /**
     * Atomically transition status when current status is one of {@code fromStatusCodes}.
     * Returns {@code true} only when this caller won the row.
     */
    public boolean tryTransitionStageRunStatus(UUID stageRunId, String toStatusCode, String... fromStatusCodes) {
        if (stageRunId == null || toStatusCode == null || fromStatusCodes == null || fromStatusCodes.length == 0) {
            return false;
        }
        UUID toId = lookupStageRunStatusId(toStatusCode);
        if (toId == null) {
            return false;
        }
        StringBuilder inList = new StringBuilder();
        List<Object> params = new ArrayList<>();
        params.add(toId.toString());
        params.add(stageRunId.toString());
        String appId = tryAppIdStr();
        String appPredicate = "";
        if (appId != null) {
            appPredicate = " AND bsr.application_id = ?::uuid ";
            params.add(appId);
        }
        for (int i = 0; i < fromStatusCodes.length; i++) {
            if (i > 0) {
                inList.append(',');
            }
            inList.append('?');
            params.add(fromStatusCodes[i]);
        }
        String sql = """
            UPDATE client_subscription_billing.billing_stage_run bsr
            SET stage_run_status_id = ?::uuid,
                modified_on = now()
            WHERE bsr.stage_run_id = ?::uuid
              %s
              AND bsr.stage_run_status_id IN (
                  SELECT stage_run_status_id FROM billing_config.stage_run_status
                  WHERE status_code IN (%s)
              )
            """.formatted(appPredicate, inList);
        return jdbc.update(sql, params.toArray()) == 1;
    }

    /**
     * Atomically claim a stage run for execution: allowed prior statuses → {@code RUNNING}.
     * Returns {@code true} only when this caller won the row (exactly one row updated).
     */
    public boolean tryClaimStageRunToRunning(UUID stageRunId, String... fromStatusCodes) {
        if (stageRunId == null || fromStatusCodes == null || fromStatusCodes.length == 0) {
            return false;
        }
        UUID runningId = lookupStageRunStatusId("RUNNING");
        if (runningId == null) {
            return false;
        }
        StringBuilder inList = new StringBuilder();
        List<Object> params = new ArrayList<>();
        params.add(runningId.toString());
        params.add(stageRunId.toString());
        String appId = tryAppIdStr();
        String appPredicate = "";
        if (appId != null) {
            appPredicate = " AND bsr.application_id = ?::uuid ";
            params.add(appId);
        }
        for (int i = 0; i < fromStatusCodes.length; i++) {
            if (i > 0) {
                inList.append(',');
            }
            inList.append('?');
            params.add(fromStatusCodes[i]);
        }
        String sql = """
            UPDATE client_subscription_billing.billing_stage_run bsr
            SET stage_run_status_id = ?::uuid,
                started_on = COALESCE(bsr.started_on, now()),
                modified_on = now()
            WHERE bsr.stage_run_id = ?::uuid
              %s
              AND bsr.stage_run_status_id IN (
                  SELECT stage_run_status_id FROM billing_config.stage_run_status
                  WHERE status_code IN (%s)
              )
            """.formatted(appPredicate, inList);
        return jdbc.update(sql, params.toArray()) == 1;
    }

    /** Tenant app id when present; null for cross-tenant background claim helpers. */
    private String tryAppIdStr() {
        try {
            UUID id = AccessContext.applicationId();
            return id != null ? id.toString() : null;
        } catch (Exception ignored) {
            return null;
        }
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
            WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
            """, ids.get(0).toString(), stageRunId.toString(), requireAppIdStr());
        return true;
    }

    /**
     * Stamp {@code started_on} on first enqueue / claim if still null so UI can show Started while QUEUED.
     */
    public void ensureStartedOn(UUID stageRunId) {
        if (stageRunId == null) {
            return;
        }
        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET started_on = COALESCE(started_on, now()), modified_on = now()
            WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
            """, stageRunId.toString(), requireAppIdStr());
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
            WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
            """,
                statusId.toString(), stageRunId.toString(), requireAppIdStr());
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
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                    statusId.toString(), jsonStr, stageRunId.toString(), requireAppIdStr());
        } catch (Exception e) {
            // Log error
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                        statusId.toString(), stageRunId.toString(), requireAppIdStr());
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
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                    statusId.toString(), jsonStr, stageRunId.toString(), requireAppIdStr());
        } catch (Exception e) {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                    statusId.toString(), stageRunId.toString(), requireAppIdStr());
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
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                    statusId.toString(), errorMessage, errorDetailsStr, stageRunId.toString(), requireAppIdStr());
        } catch (Exception e) {
            // Log error
            jdbc.update("""
                UPDATE client_subscription_billing.billing_stage_run
                SET stage_run_status_id = ?::uuid, ended_on = now(), error_message = ?, modified_on = now()
                WHERE stage_run_id = ?::uuid AND application_id = ?::uuid
                """,
                        statusId.toString(), errorMessage, stageRunId.toString(), requireAppIdStr());
        }
    }

    /**
     * Tenant-scoped monitor list for IG / mock-charge jobs (scheduled + in-flight + recent terminal).
     */
    public List<BillingJobMonitorRow> searchJobMonitor(
            String stageCodeOrNull,
            String statusCodeOrNull,
            int limit,
            int offset) {
        int lim = Math.max(1, Math.min(limit, 200));
        int off = Math.max(0, offset);
        StringBuilder sql = new StringBuilder("""
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id, br.billing_run_code,
                   br.due_date,
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   srs.status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on, bsr.modified_on,
                   bsr.attempt_number, bsr.summary_json
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.application_id = ?::uuid
              AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
            """);
        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (stageCodeOrNull != null && !stageCodeOrNull.isBlank()) {
            sql.append(" AND bsc.stage_code = ? ");
            params.add(stageCodeOrNull.trim());
        }
        if (statusCodeOrNull != null && !statusCodeOrNull.isBlank()) {
            String st = statusCodeOrNull.trim();
            if ("ACTIVE".equalsIgnoreCase(st)) {
                // Actionable pipeline statuses (includes WAITING — IG drafts awaiting lock).
                sql.append("""
                     AND srs.status_code IN (
                       'SCHEDULED', 'QUEUED', 'PENDING', 'RUNNING', 'WAITING', 'IDLE'
                     )
                    """);
            } else {
                sql.append(" AND srs.status_code = ? ");
                params.add(st);
            }
        }
        // null/blank status = all statuses for monitor stages (paginated).
        // Leading newline required: Java text-block indent stripping can glue "?ORDER".
        sql.append("""
            
            ORDER BY
              CASE srs.status_code
                WHEN 'RUNNING' THEN 1
                WHEN 'QUEUED' THEN 2
                WHEN 'PENDING' THEN 3
                WHEN 'WAITING' THEN 4
                WHEN 'SCHEDULED' THEN 5
                WHEN 'IDLE' THEN 6
                WHEN 'FAILED' THEN 7
                ELSE 8
              END,
              COALESCE(bsr.modified_on, bsr.scheduled_for, bsr.created_on) DESC
            LIMIT ? OFFSET ?
            """);
        params.add(lim);
        params.add(off);
        return jdbc.query(sql.toString(), params.toArray(), (rs, i) -> mapJobMonitorRow(rs));
    }

    public int countJobMonitor(String stageCodeOrNull, String statusCodeOrNull) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.application_id = ?::uuid
              AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
            """);
        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        if (stageCodeOrNull != null && !stageCodeOrNull.isBlank()) {
            sql.append(" AND bsc.stage_code = ? ");
            params.add(stageCodeOrNull.trim());
        }
        if (statusCodeOrNull != null && !statusCodeOrNull.isBlank()) {
            String st = statusCodeOrNull.trim();
            if ("ACTIVE".equalsIgnoreCase(st)) {
                sql.append("""
                     AND srs.status_code IN (
                       'SCHEDULED', 'QUEUED', 'PENDING', 'RUNNING', 'WAITING', 'IDLE'
                     )
                    """);
            } else {
                sql.append(" AND srs.status_code = ? ");
                params.add(st);
            }
        }
        Integer n = jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    public Map<String, Long> countJobMonitorByStatus() {
        String sql = """
            SELECT srs.status_code, COUNT(1) AS cnt
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.application_id = ?::uuid
              AND bsc.stage_code IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')
            GROUP BY srs.status_code
            """;
        Map<String, Long> out = new HashMap<>();
        jdbc.query(sql, (rs, rowNum) -> {
            out.put(rs.getString("status_code"), rs.getLong("cnt"));
            return null;
        }, requireAppIdStr());
        return out;
    }

    private BillingJobMonitorRow mapJobMonitorRow(java.sql.ResultSet rs) throws SQLException {
        Map<String, Object> summary = null;
        String summaryJsonStr = rs.getString("summary_json");
        if (summaryJsonStr != null) {
            try {
                summary = objectMapper.readValue(summaryJsonStr, new TypeReference<Map<String, Object>>() {});
            } catch (Exception ignored) {
                // ignore
            }
        }
        Integer reclaimCount = null;
        if (summary != null && summary.get("stale_reclaim_count") instanceof Number n) {
            reclaimCount = n.intValue();
        }
        return new BillingJobMonitorRow(
                (UUID) rs.getObject("stage_run_id"),
                rs.getString("stage_run_code"),
                (UUID) rs.getObject("billing_run_id"),
                rs.getString("billing_run_code"),
                rs.getObject("due_date", LocalDate.class),
                rs.getString("stage_code"),
                rs.getString("stage_display_name"),
                rs.getString("status_code"),
                rs.getString("status_display_name"),
                rs.getObject("scheduled_for", OffsetDateTime.class),
                rs.getObject("started_on", OffsetDateTime.class),
                rs.getObject("ended_on", OffsetDateTime.class),
                rs.getObject("modified_on", OffsetDateTime.class),
                rs.getObject("attempt_number") != null ? rs.getInt("attempt_number") : null,
                reclaimCount,
                summary);
    }

    /** Row for billing job monitor list (tenant-scoped). */
    public record BillingJobMonitorRow(
            UUID stageRunId,
            String stageRunCode,
            UUID billingRunId,
            String billingRunCode,
            LocalDate dueDate,
            String stageCode,
            String stageDisplayName,
            String statusCode,
            String statusDisplayName,
            OffsetDateTime scheduledFor,
            OffsetDateTime startedOn,
            OffsetDateTime endedOn,
            OffsetDateTime modifiedOn,
            Integer attemptNumber,
            Integer staleReclaimCount,
            Map<String, Object> summaryJson
    ) {}
}
