package io.clubone.billing.repo;

import java.util.UUID;

/**
 * Resolves a subscription billing history row to its parent invoice and run (for void/revert, etc.).
 */
public record SbhInvoiceLink(UUID subscriptionBillingHistoryId, UUID invoiceId, UUID billingRunId) {}
