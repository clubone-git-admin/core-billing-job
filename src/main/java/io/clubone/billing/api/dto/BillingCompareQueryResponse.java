package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.math.BigDecimal;
import java.util.List;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingCompareQueryResponse(
        Summary summary,
        List<Row> rows,
        Integer page,
        Integer pageSize,
        Integer totalPages,
        Integer totalRows,
        Boolean hasNext
) {
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record Summary(
            Integer totalRecords,
            Integer matchedRecords,
            Integer changedRecords,
            Integer leftOnlyRecords,
            Integer rightOnlyRecords,
            BigDecimal leftAmount,
            BigDecimal rightAmount,
            BigDecimal deltaAmount
    ) {}

    /**
     * Per-side mirrored snapshot fields for the compare grid (§1.2.1).
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SideSnapshot(
            String invoiceId,
            String clientName,
            String clientRole,
            String agreementName,
            String agreementStatus,
            String cycleNo,
            String locationName,
            BigDecimal amount,
            String invoiceStatus
    ) {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Row(
            /** Center identity: subscription when known (UUID string). */
            String subscriptionInstanceId,
            /** Center identity: invoice UUID string when known. */
            @JsonIgnore
            String invoiceId,
            /** Join / legacy key (often same as subscription id). */
            String entityKey,
            /** Preferred center label for compare grid (fallback still handled by FE). */
            String subscriptionPlan,
            /** Compatibility alias used by some clients. */
            String planName,
            SideSnapshot left,
            SideSnapshot right,
            /** Legacy flat fields for FE fallback. */
            @JsonIgnore
            String clientName,
            @JsonIgnore
            String agreementName,
            @JsonIgnore
            String locationName,
            @JsonIgnore
            String leftStatus,
            @JsonIgnore
            String rightStatus,
            @JsonIgnore
            BigDecimal leftAmount,
            @JsonIgnore
            BigDecimal rightAmount,
            @JsonIgnore
            BigDecimal deltaAmount,
            List<String> changedFields,
            String severity
    ) {}
}
