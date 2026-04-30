package io.clubone.billing.dashboard;

/**
 * Stable JSON keys and dashboard copy used by {@code GET /api/v1/billing/dashboard/overview}
 * and related clients (Flutter billing dashboard). Centralized to avoid scattered literals.
 */
public final class DashboardApiConstants {

    private DashboardApiConstants() {}

    public static final String JSON_SUMMARY = "summary";
    public static final String JSON_CHARTS = "charts";
    public static final String JSON_TRENDS = "trends";
    public static final String JSON_BILLED_COLLECTED = "billed_collected";
    public static final String JSON_SEGMENTS = "segments";
    public static final String JSON_FUNNEL = "funnel";
    public static final String JSON_STAGES = "stages";
    public static final String JSON_ALERTS = "alerts";
    public static final String JSON_PAYMENT_METHOD_SPLIT = "payment_method_split";
    public static final String JSON_COLLECTION_BY_GATEWAY = "collection_by_gateway";
    public static final String JSON_AR_AGING = "ar_aging";
    public static final String JSON_INVOICE_STATUS = "invoice_status";

    /** Default alert titles when building {@code charts.alerts} from aggregates. */
    public static final String ALERT_TITLE_DLQ = "DLQ items";

    public static final String ALERT_TITLE_PAYMENT_FAILURES = "Payment failures";

    /** Weights for {@code recovery_potential} heuristic (tunable in one place). */
    public static final double RECOVERY_AT_RISK_WEIGHT = 0.55;

    public static final double RECOVERY_DLQ_UNIT_AMOUNT = 500.0;
}
