package io.clubone.billing.repo;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StatusDto;
import io.clubone.billing.api.dto.StageDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for billing run operations.
 */
@Repository
public class BillingRunRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public BillingRunRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Find billing runs with filtering and pagination.
     */
    public List<BillingRunDto> findBillingRuns(
            String statusCode, String currentStageCode, LocalDate dueDateFrom,
            LocalDate dueDateTo, UUID locationId, Integer limit, Integer offset,
            String sortBy, String sortOrder) {

        StringBuilder sql = new StringBuilder("""
            SELECT br.billing_run_id, br.billing_run_code, br.due_date, br.location_id,
                   br.started_on, br.ended_on, br.summary_json,
                   br.created_by, br.created_on, br.modified_on, br.modified_by,
                   br.source_run_id, br.approved_by, br.approved_on, br.approval_notes,
                   brs.status_code AS run_status_code, brs.display_name AS run_status_display,
                   brs.description AS run_status_description,
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   bsc.is_optional AS stage_is_optional,
                   ap.status_code AS approval_status_code, ap.display_name AS approval_status_display,
                   l.name AS location_name,
                   src.billing_run_code AS source_run_code
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN client_subscription_billing.lu_approval_status ap ON ap.approval_status_id = br.approval_status_id
            LEFT JOIN locations.location l ON l.location_id = br.location_id
            LEFT JOIN client_subscription_billing.billing_run src ON src.billing_run_id = br.source_run_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (statusCode != null) {
            sql.append(" AND brs.status_code = ?");
            params.add(statusCode);
        }
        if (currentStageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(currentStageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        // Add sorting
        String validSortBy = validateSortBy(sortBy);
        String validSortOrder = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        sql.append(" ORDER BY br.").append(validSortBy).append(" ").append(validSortOrder);

        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            UUID billingRunId = (UUID) rs.getObject("billing_run_id");
            String billingRunCode = rs.getString("billing_run_code");
            LocalDate dueDate = rs.getObject("due_date", LocalDate.class);
            UUID locationIdResult = (UUID) rs.getObject("location_id");
            String locationName = rs.getString("location_name");
        

            StatusDto billingRunStatus = new StatusDto(
                    rs.getString("run_status_code"),
                    rs.getString("run_status_display"),
                    rs.getString("run_status_description")
            );

            StageDto currentStage = null;
            if (rs.getString("stage_code") != null) {
                currentStage = new StageDto(
                        rs.getString("stage_code"),
                        rs.getString("stage_display_name"),
                        rs.getInt("stage_sequence"),
                        rs.getString("stage_description"),
                        rs.getBoolean("stage_is_optional")
                );
            }

            StatusDto approvalStatus = null;
            if (rs.getString("approval_status_code") != null) {
                approvalStatus = new StatusDto(
                        rs.getString("approval_status_code"),
                        rs.getString("approval_status_display"),
                        null
                );
            }

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

            UUID createdBy = (UUID) rs.getObject("created_by");
            OffsetDateTime createdOn = rs.getObject("created_on", OffsetDateTime.class);
            OffsetDateTime modifiedOn = rs.getObject("modified_on", OffsetDateTime.class);
            UUID sourceRunId = (UUID) rs.getObject("source_run_id");
            String sourceRunCode = rs.getString("source_run_code");
            UUID approvedBy = (UUID) rs.getObject("approved_by");
            OffsetDateTime approvedOn = rs.getObject("approved_on", OffsetDateTime.class);
            String approvalNotes = rs.getString("approval_notes");

            return new BillingRunDto(
                    billingRunId, billingRunCode, dueDate, locationIdResult, locationName,
                     billingRunStatus, currentStage, approvalStatus,
                    startedOn, endedOn, summaryJson, createdBy, createdOn, modifiedOn,
                    sourceRunId, sourceRunCode, approvedBy, approvedOn, approvalNotes,
                    null, null // stages and approvals loaded separately
            );
        });
    }

    /**
     * Count total billing runs matching filters.
     */
    public Integer countBillingRuns(
            String statusCode, String currentStageCode, LocalDate dueDateFrom,
            LocalDate dueDateTo, UUID locationId) {

        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (statusCode != null) {
            sql.append(" AND brs.status_code = ?");
            params.add(statusCode);
        }
        if (currentStageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(currentStageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find billing run by ID.
     */
    public BillingRunDto findById(UUID billingRunId) {
        String sql = """
            SELECT br.billing_run_id, br.billing_run_code, br.due_date, br.location_id,
                   br.started_on, br.ended_on, br.summary_json,
                   br.created_by, br.created_on, br.modified_on, br.modified_by,
                   br.source_run_id, br.approved_by, br.approved_on, br.approval_notes,
                   brs.status_code AS run_status_code, brs.display_name AS run_status_display,
                   brs.description AS run_status_description,
                   bsc.stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   bsc.is_optional AS stage_is_optional,
                   ap.status_code AS approval_status_code, ap.display_name AS approval_status_display,
                   l.name AS location_name,
                   src.billing_run_code AS source_run_code
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.lu_billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN client_subscription_billing.lu_approval_status ap ON ap.approval_status_id = br.approval_status_id
            LEFT JOIN locations.location l ON l.location_id = br.location_id
            LEFT JOIN client_subscription_billing.billing_run src ON src.billing_run_id = br.source_run_id
            WHERE br.billing_run_id = ?::uuid
            """;

        List<BillingRunDto> results = jdbc.query(sql, new Object[]{billingRunId.toString()}, (rs, rowNum) -> {
            UUID id = (UUID) rs.getObject("billing_run_id");
            String billingRunCode = rs.getString("billing_run_code");
            LocalDate dueDate = rs.getObject("due_date", LocalDate.class);
            UUID locationIdResult = (UUID) rs.getObject("location_id");
            String locationName = rs.getString("location_name");

            StatusDto billingRunStatus = new StatusDto(
                    rs.getString("run_status_code"),
                    rs.getString("run_status_display"),
                    rs.getString("run_status_description")
            );

            StageDto currentStage = null;
            if (rs.getString("stage_code") != null) {
                currentStage = new StageDto(
                        rs.getString("stage_code"),
                        rs.getString("stage_display_name"),
                        rs.getInt("stage_sequence"),
                        rs.getString("stage_description"),
                        rs.getBoolean("stage_is_optional")
                );
            }

            StatusDto approvalStatus = null;
            if (rs.getString("approval_status_code") != null) {
                approvalStatus = new StatusDto(
                        rs.getString("approval_status_code"),
                        rs.getString("approval_status_display"),
                        null
                );
            }

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

            UUID createdBy = (UUID) rs.getObject("created_by");
            OffsetDateTime createdOn = rs.getObject("created_on", OffsetDateTime.class);
            OffsetDateTime modifiedOn = rs.getObject("modified_on", OffsetDateTime.class);
            UUID sourceRunId = (UUID) rs.getObject("source_run_id");
            String sourceRunCode = rs.getString("source_run_code");
            UUID approvedBy = (UUID) rs.getObject("approved_by");
            OffsetDateTime approvedOn = rs.getObject("approved_on", OffsetDateTime.class);
            String approvalNotes = rs.getString("approval_notes");

            return new BillingRunDto(
                    id, billingRunCode, dueDate, locationIdResult, locationName,
                    billingRunStatus, currentStage, approvalStatus,
                    startedOn, endedOn, summaryJson, createdBy, createdOn, modifiedOn,
                    sourceRunId, sourceRunCode, approvedBy, approvedOn, approvalNotes,
                    null, null
            );
        });

        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Find billing run by idempotency key.
     */
    public BillingRunDto findByIdempotencyKey(String idempotencyKey) {
        String sql = """
            SELECT br.billing_run_id
            FROM client_subscription_billing.billing_run br
            WHERE br.idempotency_key = ?
            """;

        List<UUID> ids = jdbc.query(sql, new Object[]{idempotencyKey}, (rs, rowNum) ->
                (UUID) rs.getObject("billing_run_id"));

        if (ids.isEmpty()) {
            return null;
        }

        return findById(ids.get(0));
    }

    /**
     * Create a new billing run.
     */
    public UUID createBillingRun(
            LocalDate dueDate, UUID locationId,
            UUID createdBy, String idempotencyKey) {

        UUID billingRunId = UUID.randomUUID();

        // Get default status (INITIATED)
        String statusIdSql = "SELECT billing_run_status_id FROM client_subscription_billing.lu_billing_run_status WHERE status_code = 'INITIATED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        // Get default stage (DUE_PREVIEW)
        String stageIdSql = "SELECT billing_stage_code_id FROM client_subscription_billing.lu_billing_stage_code WHERE stage_code = 'DUE_PREVIEW'";
        UUID stageId = jdbc.queryForObject(stageIdSql, UUID.class);

        // Generate billing run code
        String billingRunCode = "BR-" + LocalDate.now().getYear() + "-" + String.format("%06d", (int)(Math.random() * 1000000));
        jdbc.update("""
        	    INSERT INTO client_subscription_billing.billing_run
        	    (billing_run_id, billing_run_code, due_date, location_id,
        	     billing_run_status_id, current_stage_code_id, started_on, created_by, idempotency_key)
        	    VALUES (?::uuid, ?, ?, ?::uuid, ?::uuid, ?::uuid, ?, ?::uuid, ?)
        	    """,
        	        billingRunId.toString(),
        	        billingRunCode,
        	        dueDate,
        	        locationId != null ? locationId.toString() : null,
        	        statusId.toString(),
        	        stageId.toString(),
        	        OffsetDateTime.now(), // ✅ correct for timestamptz
        	        createdBy != null ? createdBy.toString() : null,
        	        idempotencyKey
        	);


        return billingRunId;
    }

    /**
     * Create a new billing run for due-preview (with optional parent run).
     */
    public UUID createDuePreviewRun(LocalDate dueDate, UUID locationId, UUID createdBy, UUID sourceRunId) {
        UUID billingRunId = UUID.randomUUID();

        String statusIdSql = "SELECT billing_run_status_id FROM client_subscription_billing.lu_billing_run_status WHERE status_code = 'INITIATED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        String stageIdSql = "SELECT billing_stage_code_id FROM client_subscription_billing.lu_billing_stage_code WHERE stage_code = 'DUE_PREVIEW'";
        UUID stageId = jdbc.queryForObject(stageIdSql, UUID.class);

        String billingRunCode = "DP-" + LocalDate.now().getYear() + "-" + String.format("%06d", (int) (Math.random() * 1000000));
        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_run
            (billing_run_id, billing_run_code, due_date, location_id,
             billing_run_status_id, current_stage_code_id, started_on, created_by, source_run_id)
            VALUES (?::uuid, ?, ?::date, ?::uuid, ?::uuid, ?::uuid, ?, ?::uuid, ?::uuid)
            """,
                billingRunId.toString(),
                billingRunCode,
                dueDate,
                locationId != null ? locationId.toString() : null,
                statusId.toString(),
                stageId.toString(),
                OffsetDateTime.now(),
                createdBy != null ? createdBy.toString() : null,
                sourceRunId != null ? sourceRunId.toString() : null
        );
        return billingRunId;
    }

    /**
     * Complete a billing run with summary and set status to COMPLETED_SUCCESS.
     */
    public void completeRunWithSummary(UUID billingRunId, Map<String, Object> summaryJson) {
        String statusIdSql = "SELECT billing_run_status_id FROM client_subscription_billing.lu_billing_run_status WHERE status_code = 'COMPLETED_SUCCESS'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        String summaryJsonStr = null;
        if (summaryJson != null) {
            try {
                summaryJsonStr = objectMapper.writeValueAsString(summaryJson);
            } catch (Exception e) {
                // ignore
            }
        }
        if (summaryJsonStr != null) {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_run
                SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now(), summary_json = ?::jsonb
                WHERE billing_run_id = ?::uuid
                """, statusId.toString(), summaryJsonStr, billingRunId.toString());
        } else {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_run
                SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE billing_run_id = ?::uuid
                """, statusId.toString(), billingRunId.toString());
        }
    }

    /**
     * Update billing run.
     */
    public void updateBillingRun(UUID billingRunId, Map<String, Object> summaryJson, String approvalNotes) {
        StringBuilder sql = new StringBuilder("UPDATE client_subscription_billing.billing_run SET modified_on = now()");
        List<Object> params = new ArrayList<>();

        if (summaryJson != null) {
            try {
                String jsonStr = objectMapper.writeValueAsString(summaryJson);
                sql.append(", summary_json = ?::jsonb");
                params.add(jsonStr);
            } catch (Exception e) {
                // Log error
            }
        }

        if (approvalNotes != null) {
            sql.append(", approval_notes = ?");
            params.add(approvalNotes);
        }

        sql.append(" WHERE billing_run_id = ?::uuid");
        params.add(billingRunId.toString());

        jdbc.update(sql.toString(), params.toArray());
    }

    /**
     * Cancel billing run.
     */
    public void cancelBillingRun(UUID billingRunId) {
        String statusIdSql = "SELECT billing_run_status_id FROM client_subscription_billing.lu_billing_run_status WHERE status_code = 'CANCELLED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        jdbc.update("""
            UPDATE client_subscription_billing.billing_run
            SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
            WHERE billing_run_id = ?::uuid
            """,
                statusId.toString(), billingRunId.toString());
    }

    private String validateSortBy(String sortBy) {
        Set<String> validColumns = Set.of("created_on", "started_on", "ended_on", "due_date", "billing_run_code");
        return validColumns.contains(sortBy) ? sortBy : "created_on";
    }
}
