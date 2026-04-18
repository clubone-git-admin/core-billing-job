package io.clubone.billing.service.invoicegen;

/**
 * DLQ rows for invoice generation: per-row draft failures ({@link #ERROR_TYPE_DRAFT}) or
 * job-level issues ({@link #ERROR_TYPE_JOB}) with {@code work_item_json.source} discriminator.
 */
public final class InvoiceGenerationDraftDlqConstants {

    /** Per subscription/candidate row — retry via draft-failure APIs when {@code work_item_json} has a candidate. */
    public static final String ERROR_TYPE_DRAFT = "INVOICE_GENERATION_DRAFT";
    /** Billing run / due-preview query / unexpected — not retried by draft-failure APIs; for ops visibility. */
    public static final String ERROR_TYPE_JOB = "INVOICE_GENERATION_JOB";

    /** @deprecated use {@link #ERROR_TYPE_DRAFT} */
    @Deprecated
    public static final String ERROR_TYPE = ERROR_TYPE_DRAFT;

    public static final String WORK_ITEM_SOURCE_DRAFT = "INVOICE_GENERATION_DRAFT";
    public static final String WORK_ITEM_SOURCE_JOB = "INVOICE_GENERATION_JOB";
    public static final int WORK_ITEM_VERSION = 1;

    private InvoiceGenerationDraftDlqConstants() {}
}
