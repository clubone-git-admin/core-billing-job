package io.clubone.billing.api.dto.reconciliation;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public final class ReconciliationRequests {

    private ReconciliationRequests() {
    }

    public record RetrySyncRequest(
            @NotBlank String reason,
            @NotBlank String source,
            String requestedBy
    ) {
    }

    public record CreateExceptionRequest(
            @NotBlank String mismatchId,
            @NotBlank String entityId,
            @NotBlank String title,
            String description,
            @NotBlank String severity,
            @NotBlank String stage,
            List<String> tags
    ) {
    }

    public record AssignExceptionRequest(
            @NotBlank String assigneeId
    ) {
    }

    public record UpdateExceptionStatusRequest(
            @NotBlank String status,
            String reason
    ) {
    }

    public record BulkActionRequest(
            @NotBlank String action,
            @NotEmpty List<String> exceptionIds,
            String status,
            String assigneeId,
            String note
    ) {
    }

    public record AddNoteRequest(
            @NotBlank String note
    ) {
    }

    public record JournalValidateRequest(
            @NotEmpty List<String> journalIds
    ) {
    }

    public record TriggerBankReconcileRequest(
            @NotBlank String bankAccountId,
            String statementId,
            BigDecimal toleranceAmount
    ) {
    }

    public record ManualMatchRequest(
            @NotBlank String bankEntryRef,
            @NotBlank String systemTxnRef,
            String notes
    ) {
    }

    public record FlagExceptionRequest(
            @NotBlank String bankEntryRef,
            @NotBlank String reasonCode,
            String reasonText
    ) {
    }

    public record LinkRefundRequest(
            @NotBlank String invoiceId
    ) {
    }

    public record ReportQueryRequest(
            @NotNull Object filters
    ) {
    }

    public record ReportExportRequest(
            @NotNull Object filters,
            @NotBlank String format
    ) {
    }

    public record ConfigUpdateRequest(
            @NotNull Object config
    ) {
    }

    /** Publish a frozen enterprise config bundle (see {@code reconciliation.reco_config_publish}). */
    public record PublishRecoConfigRequest(
            @NotBlank String versionLabel,
            Object snapshot,
            String publishedBy,
            String notes
    ) {
    }

    /** Attach a published config bundle to a run ({@code reconciliation_run.reco_config_publish_id}). */
    public record AttachRunConfigPublishRequest(
            @NotBlank String recoConfigPublishId
    ) {
    }

    /** Create or replace matching rule graph (header + criteria + optional scope/outcome rows). */
    public record EnterpriseMatchingRuleWriteRequest(
            @NotNull Map<String, Object> matchingRule,
            @NotNull @NotEmpty List<Map<String, Object>> criteria,
            List<Map<String, Object>> scopes,
            List<Map<String, Object>> outcomes
    ) {
    }

    public record MatchingRuleActivePatchRequest(
            @NotNull Boolean active
    ) {
    }

    /** Single-row upsert body for GL mapping or schedule (camelCase keys aligned with list/detail APIs). */
    public record EnterpriseConfigRowRequest(
            @NotNull Map<String, Object> row
    ) {
    }

    /** Optional sample payload for rule test stub. */
    public record MatchingRuleTestRequest(
            Map<String, Object> samplePayload
    ) {
    }
}
