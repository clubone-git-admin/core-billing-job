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
}
