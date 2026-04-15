package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MockChargeListItemDto(
        UUID mockChargeRunId,
        String runCode,
        OffsetDateTime startedAt,
        OffsetDateTime endedAt,
        String statusCode,
        Integer candidatesProcessed,
        BigDecimal totalAmount,
        Integer blockedCount
) {}
