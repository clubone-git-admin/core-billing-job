package io.clubone.billing.api.v1.reports;

import java.util.Locale;
import java.util.Set;

/**
 * Frontend report catalog slugs (kebab-case) for {@code GET /api/v1/billing/reports/{report-slug}}.
 */
public final class BillingReportSlugs {

    public static final String BILL_RUN_SUMMARY = "bill-run-summary";
    public static final String BILL_RUN_STATUS = "bill-run-status";
    public static final String BILLING_EXCEPTIONS = "billing-exceptions";
    public static final String FAILED_BILLING = "failed-billing";
    public static final String INVOICE_GENERATION = "invoice-generation";
    public static final String PRE_BILL = "pre-bill";
    public static final String POST_BILL = "post-bill";
    public static final String REBILL = "rebill";
    public static final String PAYMENT_COLLECTION = "payment-collection";
    public static final String OUTSTANDING_BALANCE = "outstanding-balance";
    public static final String AR_REPORTS = "ar-reports";
    public static final String REVENUE_BY_LOCATION = "revenue-by-location";
    public static final String LOCATION_BILLING = "location-billing";
    /** @deprecated use {@link #PROMOTION_ADJUSTMENTS} for new UIs */
    @Deprecated
    public static final String PRORATION_ADJUSTMENT = "proration-adjustment";

    public static final String PROMOTION_ADJUSTMENTS = "promotion-adjustments";
    public static final String SUBSCRIPTION_ACTIVITY = "subscription-activity";

    private static final Set<String> ALL =
            Set.of(
                    BILL_RUN_SUMMARY,
                    BILL_RUN_STATUS,
                    BILLING_EXCEPTIONS,
                    FAILED_BILLING,
                    INVOICE_GENERATION,
                    PRE_BILL,
                    POST_BILL,
                    REBILL,
                    PAYMENT_COLLECTION,
                    OUTSTANDING_BALANCE,
                    AR_REPORTS,
                    REVENUE_BY_LOCATION,
                    LOCATION_BILLING,
                    PRORATION_ADJUSTMENT,
                    PROMOTION_ADJUSTMENTS,
                    SUBSCRIPTION_ACTIVITY);

    private BillingReportSlugs() {}

    /** Normalizes kebab/underscore; returns null if unknown. */
    public static String resolve(String pathSegment) {
        if (pathSegment == null) {
            return null;
        }
        String t = pathSegment.trim().toLowerCase(Locale.ROOT);
        t = t.replace('_', '-');
        if (ALL.contains(t)) {
            return t;
        }
        return null;
    }
}
