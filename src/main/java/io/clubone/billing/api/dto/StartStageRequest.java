package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Request DTO for starting a stage.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StartStageRequest(
    OffsetDateTime scheduledFor,
    String idempotencyKey,
    Map<String, Object> config
) {
}
