package io.clubone.billing.repo;

/**
 * Shared, schema-aligned JOIN fragments for billing report queries so client payment
 * method and gateway dimensions come from configured tables (not literals).
 */
final class BillingReportSql {

    private BillingReportSql() {}

    /**
     * Resolves {@code subscription_instance} from {@code sbh} or {@code invoice_entity},
     * then plan → {@code client_payment_method} → gateway and method type lookup tables.
     */
    static final String SBH_TO_PAYMENT_DIMENSIONS =
            "LEFT JOIN LATERAL ("
                    + "SELECT COALESCE("
                    + "sbh.subscription_instance_id,"
                    + " (SELECT ie0.subscription_instance_id::uuid"
                    + "  FROM transactions.invoice_entity ie0"
                    + "  WHERE ie0.invoice_id = sbh.invoice_id"
                    + "    AND ie0.subscription_instance_id IS NOT NULL"
                    + "    AND COALESCE(ie0.is_active, true) = true"
                    + "  ORDER BY ie0.created_on ASC NULLS LAST"
                    + "  LIMIT 1)) AS subscription_instance_id"
                    + ") sub_res ON true"
                    + " "
                    + "LEFT JOIN client_subscription_billing.subscription_instance si"
                    + "  ON si.subscription_instance_id = sub_res.subscription_instance_id"
                    + " "
                    + "LEFT JOIN client_subscription_billing.subscription_plan sp"
                    + "  ON sp.subscription_plan_id = si.subscription_plan_id"
                    + "  AND COALESCE(sp.is_active, true) = true"
                    + " "
                    + "LEFT JOIN client_payments.client_payment_method cpm"
                    + "  ON cpm.client_payment_method_id = sp.client_payment_method_id"
                    + "  AND COALESCE(cpm.is_active, true) = true"
                    + " "
                    + "LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm"
                    + "  ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id"
                    + " "
                    + "LEFT JOIN payment_gateway.payment_gateway pgw"
                    + "  ON pgw.payment_gateway_id = pgsm.payment_gateway_id"
                    + " "
                    + "LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt"
                    + "  ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id";

    /**
     * Convert a transactional amount to the tenant reporting currency.
     * <p>
     * Order: locked {@code amount_reporting} → identity (same currency) → approved FX
     * (direct, then inverse). Returns NULL when conversion is impossible so callers never
     * paint CAD/GBP/etc. as USD by falling back to the native total.
     *
     * @param amountReportingExpr nullable locked reporting amount (usually {@code i.amount_reporting})
     * @param nativeAmountExpr    transactional amount
     * @param currencyExpr        ISO currency (text/char)
     * @param applicationIdExpr   tenant uuid expression (e.g. {@code br.application_id})
     * @param asOfExpr            timestamptz / timestamp for rate as-of
     */
    static String toReportingAmount(
            String amountReportingExpr,
            String nativeAmountExpr,
            String currencyExpr,
            String applicationIdExpr,
            String asOfExpr) {
        String ccy = "UPPER(TRIM(COALESCE(" + currencyExpr + ", '')))";
        String rep =
                "(SELECT UPPER(TRIM(bts.reporting_currency_code)) "
                        + "FROM billing_config.billing_tenant_settings bts "
                        + "WHERE bts.application_id = "
                        + applicationIdExpr
                        + " LIMIT 1)";
        String amt = "COALESCE((" + nativeAmountExpr + ")::numeric, 0)";
        String asOf = "COALESCE((" + asOfExpr + ")::timestamptz, NOW())";
        return "CASE "
                + "WHEN ("
                + amountReportingExpr
                + ") IS NOT NULL THEN ("
                + amountReportingExpr
                + ")::numeric "
                + "WHEN "
                + ccy
                + " = '' OR "
                + rep
                + " IS NULL THEN NULL "
                + "WHEN "
                + ccy
                + " = "
                + rep
                + " THEN "
                + amt
                + " "
                + "ELSE "
                + amt
                + " * COALESCE( "
                + "  (SELECT fx.rate FROM billing_config.fx_rate fx "
                + "   WHERE fx.application_id = "
                + applicationIdExpr
                + "     AND fx.from_currency = "
                + ccy
                + "     AND fx.to_currency = "
                + rep
                + "     AND fx.is_active = true AND fx.approval_status = 'APPROVED' "
                + "     AND fx.as_of <= "
                + asOf
                + "   ORDER BY fx.as_of DESC LIMIT 1), "
                + "  (SELECT CASE WHEN fx.rate = 0 THEN NULL ELSE 1.0 / fx.rate END "
                + "   FROM billing_config.fx_rate fx "
                + "   WHERE fx.application_id = "
                + applicationIdExpr
                + "     AND fx.from_currency = "
                + rep
                + "     AND fx.to_currency = "
                + ccy
                + "     AND fx.is_active = true AND fx.approval_status = 'APPROVED' "
                + "     AND fx.as_of <= "
                + asOf
                + "   ORDER BY fx.as_of DESC LIMIT 1)"
                + ") END";
    }

    /** Same as {@link #toReportingAmount} wrapped with {@code COALESCE(..., 0)} for SUM-friendly use. */
    static String reportingMoney(
            String amountReportingExpr,
            String nativeAmountExpr,
            String currencyExpr,
            String applicationIdExpr,
            String asOfExpr) {
        return "COALESCE("
                + toReportingAmount(
                        amountReportingExpr, nativeAmountExpr, currencyExpr, applicationIdExpr, asOfExpr)
                + ", 0)";
    }

    /** Invoice row → reporting money (uses invoice application_id / fx_as_of). */
    static String invoiceReportingMoney(String invoiceAlias) {
        String a = invoiceAlias;
        return reportingMoney(
                a + ".amount_reporting",
                a + ".total_amount",
                a + ".currency_code::text",
                a + ".application_id",
                "COALESCE(" + a + ".fx_as_of, " + a + ".created_on)");
    }

    /**
     * SBH/invoice pair → reporting money. Prefer invoice locked amount; convert SBH native via FX.
     *
     * @param sbhAlias history alias (needs {@code invoice_total_amount}, optional {@code currency_code})
     * @param invAlias invoice alias (may be null-safe LEFT JOIN)
     * @param appIdExpr tenant application id
     * @param asOfExpr rate as-of
     */
    static String sbhReportingMoney(String sbhAlias, String invAlias, String appIdExpr, String asOfExpr) {
        return reportingMoney(
                invAlias + ".amount_reporting",
                "COALESCE(" + sbhAlias + ".invoice_total_amount, " + invAlias + ".total_amount)",
                "COALESCE("
                        + invAlias
                        + ".currency_code::text, "
                        + sbhAlias
                        + ".currency_code::text)",
                appIdExpr,
                asOfExpr);
    }
}
