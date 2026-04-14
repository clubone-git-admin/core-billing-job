package io.clubone.billing.api.dto;

/**
 * HTTP mapping for POST /api/billing/invoice-generation/runs (202 Accepted vs 200 OK replay).
 */
public enum InvoiceGenerationStartHttpStatus {
    /** New async job accepted — poll GET /runs/{id}. */
    ACCEPTED_202,
    /** Idempotent replay, already running, or already queued — body is current state. */
    OK_200
}
