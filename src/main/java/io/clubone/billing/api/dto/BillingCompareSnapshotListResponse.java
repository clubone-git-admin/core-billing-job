package io.clubone.billing.api.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record BillingCompareSnapshotListResponse(
        List<Item> data,
        Integer total,
        Integer limit,
        Integer offset
) {
    public record Item(
            UUID runId,
            String runCode,
            String stageCode,
            String status,
            LocalDate dueDate,
            Integer recordCount,
            BigDecimal amountTotal
    ) {}
}
