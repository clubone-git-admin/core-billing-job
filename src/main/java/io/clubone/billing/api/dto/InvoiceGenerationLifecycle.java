package io.clubone.billing.api.dto;

/**
 * Normalized invoice-generation lifecycle for clients (Flutter rail) when due preview is approved.
 * <p>{@code leadStatus} is derived from execution {@code billing_stage_run} status plus schedule hints in
 * {@code summary_json}; {@code statusCode} on API responses remains the raw execution status from the DB.</p>
 * <ul>
 *   <li>{@link #STAGE_IDLE} — after due preview approval, before the user runs generation (execution {@code IDLE})</li>
 *   <li>{@link #PENDING} — legacy / generic: no async execution yet (execution {@code PENDING}, not scheduled ahead, no queue)</li>
 *   <li>{@link #SCHEDULED} — a later run is planned: execution {@code SCHEDULED}, or execution {@code PENDING} with
 *       {@code invoiceScheduledFor} in the future and no {@code queued_at} in summary</li>
 *   <li>{@link #RUNNING} — user clicked Run and work is in flight: execution {@code QUEUED} or {@code RUNNING}</li>
 *   <li>{@link #WAITING} — async generation finished; awaiting POST .../invoice-generation/lock (execution {@code WAITING})</li>
 *   <li>{@link #COMPLETED} — after lock (execution {@code COMPLETED}; {@code invoicesLocked} true when summary has lock metadata)</li>
 *   <li>{@link #FAILED} / {@link #CANCELLED} — pass through from execution status</li>
 * </ul>
 */
public final class InvoiceGenerationLifecycle {

    /** Value {@code IDLE} for {@code leadStatus} / execution status after due preview approval. */
    public static final String STAGE_IDLE = "IDLE";
    public static final String PENDING = "PENDING";
    public static final String SCHEDULED = "SCHEDULED";
    public static final String RUNNING = "RUNNING";
    public static final String WAITING = "WAITING";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    public static final String CANCELLED = "CANCELLED";

    private InvoiceGenerationLifecycle() {}
}
