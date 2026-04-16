package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for billing run information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingRunDto(
    UUID billingRunId,
    String billingRunCode,
    LocalDate dueDate,
    UUID locationId,
    String locationName,
    StatusDto billingRunStatus,
    StageDto currentStage,
    StatusDto approvalStatus,
    OffsetDateTime startedOn,
    OffsetDateTime endedOn,
    Map<String, Object> summaryJson,
    UUID createdBy,
    OffsetDateTime createdOn,
    OffsetDateTime modifiedOn,
    UUID sourceRunId,
    String sourceRunCode,
    UUID approvedBy,
    OffsetDateTime approvedOn,
    String approvalNotes,
    @JsonProperty("stage_history")
    List<StageRunDto> stageHistory,
    List<ApprovalDto> approvals
) {
    @JsonProperty("status")
    public String status() {
        return billingRunStatus != null ? billingRunStatus.statusCode() : null;
    }

    @JsonProperty("current_stage")
    public String currentStageCode() {
        return currentStage != null ? currentStage.stageCode() : null;
    }

    @JsonProperty("completedOn")
    public OffsetDateTime completedOn() {
        return endedOn;
    }

    @JsonProperty("completed_on")
    public OffsetDateTime completedOnSnake() {
        return endedOn;
    }

    @JsonProperty("finishedOn")
    public OffsetDateTime finishedOn() {
        return endedOn;
    }

    @JsonProperty("finished_on")
    public OffsetDateTime finishedOnSnake() {
        return endedOn;
    }

    @JsonProperty("invoices_count")
    public Integer invoicesCountSnake() {
        return intFromSummary("invoices_count", "invoicesCount", "invoiceCount");
    }

    @JsonProperty("invoicesCount")
    public Integer invoicesCountCamel() {
        return intFromSummary("invoices_count", "invoicesCount", "invoiceCount");
    }

    @JsonProperty("invoiceCount")
    public Integer invoiceCount() {
        return intFromSummary("invoiceCount", "invoicesCount", "invoices_count");
    }

    @JsonProperty("total_amount")
    public Double totalAmountSnake() {
        return numberFromSummary("total_amount", "totalAmount");
    }

    @JsonProperty("totalAmount")
    public Double totalAmountCamel() {
        return numberFromSummary("totalAmount", "total_amount");
    }

    @JsonProperty("failure_count")
    public Integer failureCountSnake() {
        return intFromSummary("failure_count", "failureCount", "failuresCount");
    }

    @JsonProperty("failureCount")
    public Integer failureCountCamel() {
        return intFromSummary("failureCount", "failure_count", "failuresCount");
    }

    @JsonProperty("failuresCount")
    public Integer failuresCount() {
        return intFromSummary("failuresCount", "failureCount", "failure_count");
    }

    private Integer intFromSummary(String... keys) {
        if (summaryJson == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            Object v = summaryJson.get(key);
            if (v instanceof Number n) {
                return n.intValue();
            }
            if (v != null) {
                try {
                    return Integer.parseInt(String.valueOf(v));
                } catch (Exception ignored) {
                    // continue to next key
                }
            }
        }
        return null;
    }

    private Double numberFromSummary(String... keys) {
        if (summaryJson == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            Object v = summaryJson.get(key);
            if (v instanceof Number n) {
                return n.doubleValue();
            }
            if (v != null) {
                try {
                    return Double.parseDouble(String.valueOf(v));
                } catch (Exception ignored) {
                    // continue to next key
                }
            }
        }
        return null;
    }
}
