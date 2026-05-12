package io.clubone.billing.api.dto.reconciliation;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;

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
}
