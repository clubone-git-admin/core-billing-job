package io.clubone.billing.service.invoicegen;

import java.util.UUID;

/**
 * Published after POST accepts async invoice generation; processed after transaction commit.
 */
public record InvoiceGenerationQueuedEvent(UUID stageRunId) {
}
