package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record MockChargeTrendsResponse(
        /** Count-weighted: {@code eligible_count / total_candidates} (same as {@code readiness_score} in summary). */
        List<Double> readiness,
        /** {@code blocked_count / total_candidates}. */
        List<Double> blockedRatio,
        /**
         * Amount-weighted share of invoice total that passed mock validation: {@code eligible_amount / total_amount}
         * in {@code [0,1]} — a proxy for payment-provider / capture confidence. Persisted as {@code provider_confidence}
         * in stage summary; trends recompute from amounts if missing (older runs).
         */
        List<Double> providerConfidence,
        List<UUID> mockChargeRunIds
) {}
