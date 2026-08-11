package io.clubone.billing.service.actualcharge;

import io.clubone.billing.security.TenantContext;

import java.util.UUID;

/** Published after commit so async worker and logs can correlate billing run + stage execution. */
public record ActualChargeQueuedEvent(UUID stageRunId, UUID billingRunId, TenantContext tenantContext) {
    public static ActualChargeQueuedEvent of(UUID stageRunId, UUID billingRunId) {
        return new ActualChargeQueuedEvent(stageRunId, billingRunId, TenantContext.get());
    }
}
