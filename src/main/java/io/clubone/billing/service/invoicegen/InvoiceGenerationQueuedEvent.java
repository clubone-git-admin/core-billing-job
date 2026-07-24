package io.clubone.billing.service.invoicegen;

import io.clubone.billing.security.TenantContext;

import java.util.UUID;

/**
 * Published after POST accepts async invoice generation; processed after transaction commit.
 *
 * <p>{@code tenantContext} is captured on the request thread when present. The async listener
 * falls back to resolving tenant from the stage-run row when null (auth-free background path).
 */
public record InvoiceGenerationQueuedEvent(UUID stageRunId, TenantContext tenantContext) {

    public static InvoiceGenerationQueuedEvent of(UUID stageRunId) {
        return new InvoiceGenerationQueuedEvent(stageRunId, TenantContext.get());
    }
}
