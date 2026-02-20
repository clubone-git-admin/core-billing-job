package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * DTO for a single due preview run history record.
 * Fields: run_id, run_code, generated_at, status, filename (S3), invoices, totalAmount, isMarkReady.
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
    @JsonProperty("isMarkReady") Boolean isMarkReady
) {}
