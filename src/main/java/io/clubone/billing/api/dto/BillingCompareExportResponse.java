package io.clubone.billing.api.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.time.OffsetDateTime;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record BillingCompareExportResponse(
        String exportId,
        String status,
        String downloadUrl,
        OffsetDateTime expiresAt
) {
}
