package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Invoice generation run (maps to {@code billing_stage_run} for INVOICE_GENERATION).
 * <p>{@code statusCode} is the raw execution status from the database. {@code leadStatus} follows
 * {@link InvoiceGenerationLifecycle} for UI (rail) — use {@code leadStatus} for polling/stop rules.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationRunResponse(
        UUID invoiceGenerationRunId,
        UUID billingRunId,
        String billingRunCode,
        UUID stageRunId,
        String stageRunCode,
        /** Raw execution status from {@code billing_stage_run} (QUEUED, RUNNING, WAITING, …). */
        String statusCode,
        String statusDisplayName,
        /** Normalized lifecycle for Flutter; see {@link InvoiceGenerationLifecycle}. */
        String leadStatus,
        /** When set (summary or {@code scheduled_for}), a later run is planned; see {@link InvoiceGenerationLifecycle#SCHEDULED}. */
        OffsetDateTime invoiceScheduledFor,
        /** True after lock metadata is present ({@code invoices_locked_at} in summary). */
        Boolean invoicesLocked,
        String mode,
        OffsetDateTime queuedAt,
        OffsetDateTime startedAt,
        OffsetDateTime endedAt,
        OffsetDateTime estimatedCompletionAt,
        InvoiceGenerationProgressDto progress,
        Map<String, Object> summaryJson,
        String errorMessage,
        Integer attemptNumber,
        Integer maxAttempts,
        String pollUrl
) {
}
