package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingJobMonitorSummaryDto(
        long scheduled,
        long queued,
        long running,
        long failed,
        long cancelled
) {}
