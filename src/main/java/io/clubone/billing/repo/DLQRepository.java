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
     */
    public List<DLQItemDto> findDLQItems(
            UUID billingRunId, String failureTypeCode, Boolean resolved,
            Integer limit, Integer offset, String sortBy, String sortOrder) {

        StringBuilder sql = new StringBuilder("""
            SELECT dlq.dlq_id, dlq.billing_run_id, dlq.stage_run_id, dlq.invoice_id,
                   dlq.subscription_instance_id, dlq.error_type, dlq.error_message,
                   dlq.error_stack_trace, dlq.work_item_json, dlq.created_on,
                   dlq.retry_count, dlq.last_retry_on, dlq.resolved, dlq.resolved_on,
                   dlq.resolved_by, dlq.resolution_notes, dlq.retry_strategy,
                   br.billing_run_code, srs.stage_run_code,
                   i.invoice_number,
                   ft.failure_type_code, ft.display_name AS failure_type_display,
                   ft.is_retryable
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            LEFT JOIN client_subscription_billing.billing_stage_run srs ON srs.stage_run_id = dlq.stage_run_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = dlq.invoice_id
            LEFT JOIN client_subscription_billing.lu_failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (billingRunId != null) {
            sql.append(" AND dlq.billing_run_id = ?::uuid");
            params.add(billingRunId.toString());
        }
        if (failureTypeCode != null) {
            sql.append(" AND ft.failure_type_code = ?");
            params.add(failureTypeCode);
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
    public Integer countDLQItems(UUID billingRunId, String failureTypeCode, Boolean resolved) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN client_subscription_billing.lu_failure_type ft ON ft.failure_type_id = dlq.failure_type_id
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (billingRunId != null) {
            sql.append(" AND dlq.billing_run_id = ?::uuid");
            params.add(billingRunId.toString());
        }
        if (failureTypeCode != null) {
            sql.append(" AND ft.failure_type_code = ?");
            params.add(failureTypeCode);
        }
        if (resolved != null) {
            sql.append(" AND dlq.resolved = ?");
            params.add(resolved);
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find DLQ item by ID.
     */
    public DLQItemDto findById(UUID dlqId) {
        String sql = """
            SELECT dlq.dlq_id, dlq.billing_run_id, dlq.stage_run_id, dlq.invoice_id,
                   dlq.subscription_instance_id, dlq.error_type, dlq.error_message,
                   dlq.error_stack_trace, dlq.work_item_json, dlq.created_on,
                   dlq.retry_count, dlq.last_retry_on, dlq.resolved, dlq.resolved_on,
                   dlq.resolved_by, dlq.resolution_notes, dlq.retry_strategy,
                   br.billing_run_code, srs.stage_run_code,
                   i.invoice_number,
                   ft.failure_type_code, ft.display_name AS failure_type_display,
                   ft.is_retryable
            FROM client_subscription_billing.billing_dead_letter_queue dlq
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id
            LEFT JOIN client_subscription_billing.billing_stage_run srs ON srs.stage_run_id = dlq.stage_run_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = dlq.invoice_id
            LEFT JOIN client_subscription_billing.lu_failure_type ft ON ft.failure_type_id = dlq.failure_type_id
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

    private DLQItemDto mapDLQItem(java.sql.ResultSet rs) throws java.sql.SQLException {
        UUID dlqId = (UUID) rs.getObject("dlq_id");
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
                dlqId, billingRunId, billingRunCode, stageRunId, stageRunCode,
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
