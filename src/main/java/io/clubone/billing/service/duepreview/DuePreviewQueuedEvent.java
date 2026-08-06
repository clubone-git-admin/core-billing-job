package io.clubone.billing.service.duepreview;

import io.clubone.billing.security.TenantContext;

import java.util.UUID;

/**
 * Published after POST accepts async due-preview generation; processed after transaction commit.
 */
public record DuePreviewQueuedEvent(UUID stageRunId, TenantContext tenantContext) {

    public static DuePreviewQueuedEvent of(UUID stageRunId) {
        return new DuePreviewQueuedEvent(stageRunId, TenantContext.get());
    }
}
