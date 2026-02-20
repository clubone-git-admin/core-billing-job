package io.clubone.billing.repo;

import io.clubone.billing.api.dto.StageRunDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN client_subscription_billing.lu_stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid
            ORDER BY bsc.stage_sequence ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString()}, (rs, rowNum) -> {
            UUID stageRunId = (UUID) rs.getObject("stage_run_id");
            String stageRunCode = rs.getString("stage_run_code");
            UUID billingRunIdResult = (UUID) rs.getObject("billing_run_id");
            String stageCode = rs.getString("stage_code");
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
                } catch (Exception e) {
                    // Log error but continue
                }
            }

            String errorMessage = rs.getString("error_message");

            Map<String, Object> errorDetails = null;
            String errorDetailsStr = rs.getString("error_details");
            if (errorDetailsStr != null) {
                try {
                    errorDetails = objectMapper.readValue(errorDetailsStr, new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                    // Log error but continue
                }
            }

            Integer attemptNumber = rs.getInt("attempt_number");
            Integer maxAttempts = rs.getInt("max_attempts");

            return new StageRunDto(
                    stageRunId, stageRunCode, billingRunIdResult, stageCode, stageDisplayName,
                    stageSequence, statusCode, statusDisplayName, scheduledFor,
                    startedOn, endedOn, summaryJson, errorMessage, errorDetails,
                    attemptNumber, maxAttempts
            );
        });
    }

    /**
     * Find stage run by stage_run_id.
     */
    public StageRunDto findById(UUID stageRunId) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN client_subscription_billing.lu_stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.stage_run_id = ?::uuid
            """;
        List<StageRunDto> results = jdbc.query(sql, new Object[]{stageRunId.toString()}, (rs, rowNum) -> {
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
            return new StageRunDto(id, stageRunCode, billingRunIdResult, sc, stageDisplayName,
                    stageSequence, statusCode, statusDisplayName, scheduledFor, startedOn, endedOn,
                    summaryJson, errorMessage, errorDetails, attemptNumber, maxAttempts);
        });
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Find stage by billing run ID and stage code.
     */
    public StageRunDto findByBillingRunIdAndStageCode(UUID billingRunId, String stageCode) {
        String sql = """
            SELECT bsr.stage_run_id, bsr.stage_run_code, bsr.billing_run_id,
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   srs.status_code, srs.display_name AS status_display_name,
                   bsr.scheduled_for, bsr.started_on, bsr.ended_on,
                   bsr.summary_json, bsr.error_message, bsr.error_details,
                   bsr.attempt_number, bsr.max_attempts
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN client_subscription_billing.lu_stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            WHERE bsr.billing_run_id = ?::uuid AND bsc.stage_code = ?
            ORDER BY bsr.created_on DESC
            LIMIT 1
            """;

        List<StageRunDto> results = jdbc.query(sql, new Object[]{billingRunId.toString(), stageCode}, (rs, rowNum) -> {
            UUID stageRunId = (UUID) rs.getObject("stage_run_id");
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
                } catch (Exception e) {
                    // Log error but continue
                }
            }

            String errorMessage = rs.getString("error_message");

            Map<String, Object> errorDetails = null;
            String errorDetailsStr = rs.getString("error_details");
            if (errorDetailsStr != null) {
                try {
                    errorDetails = objectMapper.readValue(errorDetailsStr, new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                    // Log error but continue
                }
            }

            Integer attemptNumber = rs.getInt("attempt_number");
            Integer maxAttempts = rs.getInt("max_attempts");

            return new StageRunDto(
                    stageRunId, stageRunCode, billingRunIdResult, sc, stageDisplayName,
                    stageSequence, statusCode, statusDisplayName, scheduledFor,
                    startedOn, endedOn, summaryJson, errorMessage, errorDetails,
                    attemptNumber, maxAttempts
            );
        });

        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Create a new stage run.
     */
    public UUID createStageRun(
            UUID billingRunId, String stageCode, OffsetDateTime scheduledFor,
            String idempotencyKey, UUID createdBy) {

        UUID stageRunId = UUID.randomUUID();

        // Get stage code ID
        String stageIdSql = "SELECT billing_stage_code_id FROM client_subscription_billing.lu_billing_stage_code WHERE stage_code = ?";
        UUID stageId = jdbc.queryForObject(stageIdSql, new Object[]{stageCode}, UUID.class);

        // Get default status (PENDING)
        String statusIdSql = "SELECT stage_run_status_id FROM client_subscription_billing.lu_stage_run_status WHERE status_code = 'PENDING'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        // Generate stage run code
        String stageRunCode = "STG-" + OffsetDateTime.now().getYear() + "-" + String.format("%06d", (int)(Math.random() * 1000000));

        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_stage_run
            (stage_run_id, stage_run_code, billing_run_id, stage_code_id, stage_run_status_id,
              started_on, attempt_number, max_attempts, idempotency_key, created_by)
            VALUES (?::uuid, ?, ?::uuid, ?::uuid, ?::uuid, now(), 1, 1, ?, ?::uuid)
            """,
                stageRunId.toString(), stageRunCode, billingRunId.toString(),
                stageId.toString(), statusId.toString(),
                idempotencyKey, createdBy != null ? createdBy.toString() : null);

        return stageRunId;
    }

    /**
     * Update stage run status to RUNNING.
     */
    public void startStageRun(UUID stageRunId) {
        String statusIdSql = "SELECT stage_run_status_id FROM client_subscription_billing.lu_stage_run_status WHERE status_code = 'RUNNING'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        jdbc.update("""
            UPDATE client_subscription_billing.billing_stage_run
            SET stage_run_status_id = ?::uuid, started_on = now(), modified_on = now()
            WHERE stage_run_id = ?::uuid
            """,
                statusId.toString(), stageRunId.toString());
    }

    /**
     * Complete stage run.
     */
    public void completeStageRun(UUID stageRunId, Map<String, Object> summaryJson) {
        String statusIdSql = "SELECT stage_run_status_id FROM client_subscription_billing.lu_stage_run_status WHERE status_code = 'COMPLETED'";
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
     * Fail stage run.
     */
    public void failStageRun(UUID stageRunId, String errorMessage, Map<String, Object> errorDetails) {
        String statusIdSql = "SELECT stage_run_status_id FROM client_subscription_billing.lu_stage_run_status WHERE status_code = 'FAILED'";
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
