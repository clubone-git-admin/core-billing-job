package io.clubone.billing.api.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record ActualChargeListItemDto(
        UUID actualChargeRunId,
        String stageRunCode,
        OffsetDateTime startedOn,
        OffsetDateTime endedOn,
        String statusCode,
        Integer totalSelected,
        BigDecimal totalAmountSelected,
        Integer failedCount) {}
