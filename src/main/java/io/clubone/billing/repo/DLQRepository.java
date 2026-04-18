package io.clubone.billing.repo;

import io.clubone.billing.api.dto.DLQItemDto;
import io.clubone.billing.api.dto.FailureTypeDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for Dead Letter Queue operations.
 */
@Repository
public class DLQRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public DLQRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Find DLQ items with filtering.
     *
     * @param errorType optional exact match on {@code dlq.error_type} (e.g. {@code INVOICE_GENERATION_DRAFT})
     */
    public List<DLQItemDto> findDLQItems(
            UUID billingRunId, UUID stageRunId, String failureTypeCode, String errorType, Boolean resolved,
            Integer limit, Integer offset, String sortBy, String sortOrder) {

        StringBuilder sql = new StringBuilder("""
            SELECT dlq.dlq_id, dlq.dlq_code, dlq.billing_run_id, dlq.stage_run_id, dlq.invoice_id,
                   dlq.subscription_instance_id, dlq.error_type, dlq.error_message,
                   dlq.error_stack_trace, dlq.work_item_json, dlq.created_on,
                   dlq.retry_count, dlq.last_retry_on, dlq.resolved, dlq.resolved_on,
                   dlq.resolved_by, dlq.resolution_notes, dlq.retry_strategy,
                   br.billing_run_code, srs.stage_run_code,
                   i.invoice_number,
                   ft.failure_type_code AS failure_type_code, ft.display_name AS failure_type_display,
                   ft.is_retryable
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            LEFT JOIN client_subscription_billing.billing_stage_run srs ON srs.stage_run_id = dlq.stage_run_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = dlq.invoice_id
            LEFT JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (billingRunId != null) {
            sql.append(" AND dlq.billing_run_id = ?::uuid");
            params.add(billingRunId.toString());
        }
        if (stageRunId != null) {
            sql.append(" AND dlq.stage_run_id = ?::uuid");
            params.add(stageRunId.toString());
        }
        if (failureTypeCode != null) {
            sql.append(" AND ft.failure_type_code = ?");
            params.add(failureTypeCode);
        }
        if (errorType != null && !errorType.isBlank()) {
            sql.append(" AND dlq.error_type = ?");
            params.add(errorType);
        }
        if (resolved != null) {
            sql.append(" AND dlq.resolved = ?");
            params.add(resolved);
        }

        String validSortBy = validateSortBy(sortBy);
        String validSortOrder = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        sql.append(" ORDER BY dlq.").append(validSortBy).append(" ").append(validSortOrder);

        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> mapDLQItem(rs));
    }

    /**
     * Count total DLQ items matching filters.
     */
    public Integer countDLQItems(UUID billingRunId, UUID stageRunId, String failureTypeCode, String errorType, Boolean resolved) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (billingRunId != null) {
            sql.append(" AND dlq.billing_run_id = ?::uuid");
            params.add(billingRunId.toString());
        }
        if (stageRunId != null) {
            sql.append(" AND dlq.stage_run_id = ?::uuid");
            params.add(stageRunId.toString());
        }
        if (failureTypeCode != null) {
            sql.append(" AND ft.failure_type_code = ?");
            params.add(failureTypeCode);
        }
        if (errorType != null && !errorType.isBlank()) {
            sql.append(" AND dlq.error_type = ?");
            params.add(errorType);
        }
        if (resolved != null) {
            sql.append(" AND dlq.resolved = ?");
            params.add(resolved);
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Insert a DLQ row for a failed invoice-generation draft create (async job or manual retry).
     */
    public UUID insertInvoiceGenerationDraftFailure(
            UUID billingRunId,
            UUID stageRunId,
            UUID subscriptionInstanceId,
            String errorType,
            String errorMessage,
            String errorStackTrace,
            Map<String, Object> workItemRoot) {
        UUID dlqId = UUID.randomUUID();
        try {
            String jsonStr = objectMapper.writeValueAsString(workItemRoot);
            jdbc.update("""
                INSERT INTO client_subscription_billing.billing_dead_letter_queue
                (dlq_id, billing_run_id, stage_run_id, invoice_id, subscription_instance_id,
                 error_type, error_message, error_stack_trace, work_item_json, created_on, resolved)
                VALUES (?::uuid, ?::uuid, ?::uuid, NULL, ?::uuid, ?, ?, ?, ?::jsonb, now(), false)
                """,
                    dlqId.toString(),
                    billingRunId.toString(),
                    stageRunId != null ? stageRunId.toString() : null,
                    subscriptionInstanceId != null ? subscriptionInstanceId.toString() : null,
                    errorType,
                    errorMessage,
                    errorStackTrace,
                    jsonStr);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to insert invoice generation draft DLQ row: " + e.getMessage(), e);
        }
        return dlqId;
    }

    /**
     * Unresolved draft-failure DLQ ids for a stage, oldest first (for bulk retry).
     */
    public List<UUID> findUnresolvedIdsByBillingRunStageAndErrorType(
            UUID billingRunId, UUID stageRunId, String errorType) {
        return jdbc.query(
                """
                SELECT dlq_id
                FROM client_subscription_billing.billing_dead_letter_queue
                WHERE billing_run_id = ?::uuid
                  AND stage_run_id = ?::uuid
                  AND error_type = ?
                  AND resolved = false
                ORDER BY created_on ASC, dlq_id ASC
                """,
                (rs, rowNum) -> (UUID) rs.getObject("dlq_id"),
                billingRunId.toString(),
                stageRunId.toString(),
                errorType);
    }

    /**
     * Open invoice-generation DLQ rows for a stage run ({@code INVOICE_GENERATION_DRAFT} / {@code INVOICE_GENERATION_JOB}).
     */
    public int countUnresolvedInvoiceGenerationDlqByStageRun(UUID stageRunId) {
        Integer n = jdbc.queryForObject(
                """
                SELECT COUNT(1)
                FROM client_subscription_billing.billing_dead_letter_queue
                WHERE stage_run_id = ?::uuid
                  AND resolved = false
                  AND error_type IN ('INVOICE_GENERATION_DRAFT', 'INVOICE_GENERATION_JOB')
                """,
                Integer.class,
                stageRunId.toString());
        return n != null ? n : 0;
    }

    /**
     * Find DLQ item by ID.
     */
    public DLQItemDto findById(UUID dlqId) {
        String sql = """
            SELECT dlq.dlq_id, dlq.dlq_code, dlq.billing_run_id, dlq.stage_run_id, dlq.invoice_id,
                   dlq.subscription_instance_id, dlq.error_type, dlq.error_message,
                   dlq.error_stack_trace, dlq.work_item_json, dlq.created_on,
                   dlq.retry_count, dlq.last_retry_on, dlq.resolved, dlq.resolved_on,
                   dlq.resolved_by, dlq.resolution_notes, dlq.retry_strategy,
                   br.billing_run_code, srs.stage_run_code,
                   i.invoice_number,
                   ft.failure_type_code AS failure_type_code, ft.display_name AS failure_type_display,
                   ft.is_retryable
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            LEFT JOIN client_subscription_billing.billing_stage_run srs ON srs.stage_run_id = dlq.stage_run_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = dlq.invoice_id
            LEFT JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            WHERE dlq.dlq_id = ?::uuid
            """;

        List<DLQItemDto> results = jdbc.query(sql, new Object[]{dlqId.toString()}, (rs, rowNum) -> mapDLQItem(rs));
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Update retry count and last retry time.
     */
    public void updateRetry(UUID dlqId, Map<String, Object> retryStrategy) {
        try {
            String retryStrategyStr = retryStrategy != null ? objectMapper.writeValueAsString(retryStrategy) : null;
            jdbc.update("""
                UPDATE client_subscription_billing.billing_dead_letter_queue
                SET retry_count = retry_count + 1, last_retry_on = now(), retry_strategy = ?::jsonb
                WHERE dlq_id = ?::uuid
                """,
                    retryStrategyStr, dlqId.toString());
        } catch (Exception e) {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_dead_letter_queue
                SET retry_count = retry_count + 1, last_retry_on = now()
                WHERE dlq_id = ?::uuid
                """,
                        dlqId.toString());
        }
    }

    /**
     * Resolve DLQ item.
     */
    public void resolve(UUID dlqId, String resolvedBy, String resolutionNotes) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_dead_letter_queue
            SET resolved = true, resolved_on = now(), resolved_by = ?, resolution_notes = ?
            WHERE dlq_id = ?::uuid
            """,
                resolvedBy, resolutionNotes, dlqId.toString());
    }

    /**
     * Shallow-merge into {@code retry_strategy} without incrementing {@code retry_count} (e.g. after a successful draft retry).
     */
    public void mergeRetryStrategy(UUID dlqId, Map<String, Object> patch) {
        if (patch == null || patch.isEmpty()) {
            return;
        }
        try {
            String patchStr = objectMapper.writeValueAsString(patch);
            jdbc.update("""
                UPDATE client_subscription_billing.billing_dead_letter_queue
                SET retry_strategy = COALESCE(retry_strategy, '{}'::jsonb) || ?::jsonb
                WHERE dlq_id = ?::uuid
                """,
                    patchStr, dlqId.toString());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to merge billing_dead_letter_queue.retry_strategy: " + e.getMessage(), e);
        }
    }

    private DLQItemDto mapDLQItem(java.sql.ResultSet rs) throws java.sql.SQLException {
        UUID dlqId = (UUID) rs.getObject("dlq_id");
        String dlqCode = rs.getString("dlq_code");
        UUID billingRunId = (UUID) rs.getObject("billing_run_id");
        String billingRunCode = rs.getString("billing_run_code");
        UUID stageRunId = (UUID) rs.getObject("stage_run_id");
        String stageRunCode = rs.getString("stage_run_code");
        UUID invoiceId = (UUID) rs.getObject("invoice_id");
        String invoiceNumber = rs.getString("invoice_number");
        UUID subscriptionInstanceId = (UUID) rs.getObject("subscription_instance_id");
        String errorType = rs.getString("error_type");
        String errorMessage = rs.getString("error_message");
        String errorStackTrace = rs.getString("error_stack_trace");

        FailureTypeDto failureType = null;
        if (rs.getString("failure_type_code") != null) {
            failureType = new FailureTypeDto(
                    rs.getString("failure_type_code"),
                    rs.getString("failure_type_display"),
                    rs.getBoolean("is_retryable")
            );
        }

        Map<String, Object> workItemJson = null;
        String workItemJsonStr = rs.getString("work_item_json");
        if (workItemJsonStr != null) {
            try {
                workItemJson = objectMapper.readValue(workItemJsonStr, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) {
                // Log error but continue
            }
        }

        OffsetDateTime createdOn = rs.getObject("created_on", OffsetDateTime.class);
        Integer retryCount = rs.getInt("retry_count");
        OffsetDateTime lastRetryOn = rs.getObject("last_retry_on", OffsetDateTime.class);
        Boolean resolved = rs.getBoolean("resolved");
        OffsetDateTime resolvedOn = rs.getObject("resolved_on", OffsetDateTime.class);
        String resolvedBy = rs.getString("resolved_by");
        String resolutionNotes = rs.getString("resolution_notes");

        Map<String, Object> retryStrategy = null;
        String retryStrategyStr = rs.getString("retry_strategy");
        if (retryStrategyStr != null) {
            try {
                retryStrategy = objectMapper.readValue(retryStrategyStr, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) {
                // Log error but continue
            }
        }

        return new DLQItemDto(
                dlqId, dlqCode, billingRunId, billingRunCode, stageRunId, stageRunCode,
                invoiceId, invoiceNumber, subscriptionInstanceId, errorType,
                errorMessage, errorStackTrace, failureType, workItemJson,
                createdOn, retryCount, lastRetryOn, resolved, resolvedOn,
                resolvedBy, resolutionNotes, retryStrategy
        );
    }

    private String validateSortBy(String sortBy) {
        Set<String> validColumns = Set.of("created_on", "retry_count", "last_retry_on", "resolved_on");
        return validColumns.contains(sortBy) ? sortBy : "created_on";
    }
}
