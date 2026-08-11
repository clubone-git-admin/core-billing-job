package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for a single due preview run history record.
 * When {@code mixed_currency} is true, {@code totalAmount} is null — use {@code by_currency}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DuePreviewRunHistoryDto(
    @JsonProperty("run_id") UUID runId,
    @JsonProperty("run_code") String runCode,
    @JsonProperty("generated_at") OffsetDateTime generatedAt,
    @JsonProperty("status") String status,
    @JsonProperty("filename") String filename,
    @JsonProperty("invoices") Integer invoices,
    @JsonProperty("totalAmount") BigDecimal totalAmount,
    @JsonProperty("isMarkReady") Boolean isMarkReady,
    @JsonProperty("mixed_currency") Boolean mixedCurrency,
    @JsonProperty("currencies") List<String> currencies,
    @JsonProperty("by_currency") Map<String, Object> byCurrency
) {
    /** Backward-compatible ctor used by older call sites / tests. */
    public DuePreviewRunHistoryDto(
            UUID runId,
            String runCode,
            OffsetDateTime generatedAt,
            String status,
            String filename,
            Integer invoices,
            BigDecimal totalAmount,
            Boolean isMarkReady) {
        this(runId, runCode, generatedAt, status, filename, invoices, totalAmount, isMarkReady,
                null, null, null);
    }
}
