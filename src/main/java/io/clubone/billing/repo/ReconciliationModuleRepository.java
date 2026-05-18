package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class ReconciliationModuleRepository {
    /**
     * {@code client_payment_intent.amount} and {@code client_payment_transaction.amount} are stored in minor units
     * (e.g. 82482 = 824.82 major) for reconciliation against {@code transactions.invoice.total_amount}.
     */
    private static final String CPT_AMT_MAJOR_EXPR = "COALESCE(cpt.amount, 0)::numeric / 100.0";

    /**
     * Total captured amount for {@code i.invoice_id} across all intents/transactions (major units).
     * Must match dashboard aggregates; avoids false {@code MISSING_PAYMENT} when one intent row has no txn
     * while another intent on the same invoice is paid.
     */
    private static final String INVOICE_CAPTURED_PAID_MAJOR_EXPR = """
            COALESCE((
                SELECT COALESCE(SUM(cpt_sub.amount), 0)::numeric / 100.0
                FROM client_payments.client_payment_intent cpi_sub
                LEFT JOIN client_payments.client_payment_transaction cpt_sub
                    ON cpt_sub.client_payment_intent_id = cpi_sub.client_payment_intent_id
                WHERE cpi_sub.invoice_id = i.invoice_id
            ), 0)""";

    private static final String INVOICE_HAS_ANY_CAPTURED_TXN_EXPR = """
            EXISTS (
                SELECT 1
                FROM client_payments.client_payment_intent cpi_sub
                JOIN client_payments.client_payment_transaction cpt_sub
                    ON cpt_sub.client_payment_intent_id = cpi_sub.client_payment_intent_id
                WHERE cpi_sub.invoice_id = i.invoice_id
            )""";

    /**
     * Placeholders: {@code __RECON_HAS_TXN__}, {@code __RECON_PAID_MAJOR__}, {@code __RECON_SCOPE_FILTER__}.
     * Do not split SQL with a text-block delimiter immediately after {@code WHEN NOT (}; that prematurely ends the Java text block.
     */
    private static final String INSERT_MISMATCH_SNAPSHOT_SQL = """
                INSERT INTO reconciliation.reconciliation_run_snapshot (
                    reconciliation_run_snapshot_id,
                    reconciliation_run_id,
                    snapshot_reference_code,
                    reconciliation_stage_id,
                    reconciliation_status_id,
                    severity_id,
                    entity_id,
                    entity_reference_code,
                    mismatch_reference_code,
                    expected_amount,
                    actual_amount,
                    reason_text,
                    attributes_json,
                    event_at,
                    created_on,
                    reconciliation_entity_type_id,
                    reconciliation_reason_id
                )
                SELECT
                    gen_random_uuid(),
                    ?::uuid,
                    'SN-' || replace(gen_random_uuid()::text, '-', ''),
                    (SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = 'INVOICE_TO_PAYMENT'),
                    (SELECT reconciliation_status_id FROM reconciliation.lu_reconciliation_status WHERE code = CASE
                        WHEN NOT (__RECON_HAS_TXN__) THEN 'UNRECONCILED'
                        WHEN ABS(COALESCE(i.total_amount, 0)::numeric - (__RECON_PAID_MAJOR__)) > 0.01 THEN 'PARTIAL'
                        ELSE 'RECONCILED' END),
                    (SELECT severity_id FROM reconciliation.lu_severity WHERE code = CASE
                        WHEN ABS(COALESCE(i.total_amount, 0)::numeric - (__RECON_PAID_MAJOR__)) > 1000 THEN 'HIGH' ELSE 'MEDIUM' END),
                    i.invoice_id,
                    COALESCE(i.invoice_number, CAST(i.invoice_id AS text)),
                    'MM-' || replace(CAST(i.invoice_id AS text), '-', ''),
                    i.total_amount,
                    (__RECON_PAID_MAJOR__),
                    'Invoice amount vs captured transaction',
                    jsonb_build_object(
                        'source', 'reconciliation_run',
                        'version', 2,
                        'reconciliationType',
                        CASE WHEN EXISTS (
                            SELECT 1 FROM client_subscription_billing.subscription_billing_schedule sbs
                            WHERE sbs.invoice_id = i.invoice_id
                        ) THEN 'SUBSCRIPTION' ELSE 'CASH' END,
                        'legacyReason', CASE WHEN NOT (__RECON_HAS_TXN__) THEN 'MISSING_PAYMENT' ELSE 'PAYMENT_PARTIAL' END
                    ),
                    COALESCE(i.created_on, now()),
                    now(),
                    (SELECT reconciliation_entity_type_id FROM reconciliation.lu_reconciliation_entity_type WHERE code = 'INVOICE' LIMIT 1),
                    (SELECT reconciliation_reason_id FROM reconciliation.lu_reconciliation_reason WHERE code = CASE
                        WHEN NOT (__RECON_HAS_TXN__) THEN 'PAYMENT_NOT_RECEIVED' ELSE 'AMOUNT_VARIANCE' END LIMIT 1)
                FROM transactions.invoice i
                __RECON_SCOPE_FILTER__
                """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReconciliationModuleRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> getWorkspace(String entityId, String entityType, String recoRunId, boolean recompute, String tenantId) {
        UUID invoiceId = resolveInvoiceId(entityId);
        if (invoiceId == null) {
            return workspaceNotFound(entityId, entityType);
        }
        String runRef = recoRunId == null || recoRunId.isBlank() ? null : recoRunId.trim();
        UUID runUuid = lookupRunUuidOrNull(runRef);
        boolean subscription = isSubscriptionInvoice(invoiceId);
        Map<String, Object> chain = buildSingleInvoiceChain(runRef, runUuid, invoiceId, subscription);
        Map<String, Object> out = buildWorkspaceResponseFromChain(chain, entityId, entityType, runRef, subscription);
        if (runRef != null) {
            Map<String, Object> snap = normalizeWorkspaceRunSnapshot(loadRunSnapshotForEntity(runRef, entityId));
            out.put("runSnapshot", snap);
            if (recompute) {
                out.put("snapshotSync", syncRunSnapshotWithLiveIfChanged(runRef, tenantId, liveRowFromChain(chain), snap));
            } else {
                out.put("snapshotSync", Map.of("performed", false, "reason", "recompute_disabled"));
            }
        } else {
            out.put("runSnapshot", Map.of());
            out.put("snapshotSync", Map.of("performed", false, "reason", "no_reco_run_id"));
        }
        return out;
    }

    private Map<String, Object> workspaceNotFound(String entityId, String entityType) {
        Map<String, Object> lookup = new LinkedHashMap<>();
        lookup.put("entityId", entityId);
        lookup.put("entityType", entityType == null ? "INVOICE" : entityType);
        Map<String, Object> notFound = new LinkedHashMap<>();
        notFound.put("lookup", lookup);
        notFound.put("reconciliationStatus", "NOT_FOUND");
        notFound.put("invoice", Map.of());
        notFound.put("paymentIntent", Map.of());
        notFound.put("paymentTransaction", Map.of());
        notFound.put("paymentIntents", List.of());
        notFound.put("runSnapshots", List.of());
        notFound.put("refunds", List.of());
        notFound.put("glPosting", Map.of("entries", List.of()));
        notFound.put("exceptions", List.of());
        notFound.put("stages", List.of());
        notFound.put("snapshotSync", Map.of("performed", false, "reason", "entity_not_found"));
        return notFound;
    }

    private UUID resolveInvoiceId(String entityId) {
        if (entityId == null || entityId.isBlank()) {
            return null;
        }
        List<UUID> ids = jdbc.query("""
                SELECT i.invoice_id
                FROM transactions.invoice i
                WHERE i.invoice_number = ? OR CAST(i.invoice_id AS text) = ?
                LIMIT 1
                """, (rs, rn) -> rs.getObject(1, UUID.class), entityId.trim(), entityId.trim());
        return ids.isEmpty() ? null : ids.get(0);
    }

    private UUID lookupRunUuidOrNull(String recoRunId) {
        if (recoRunId == null || recoRunId.isBlank()) {
            return null;
        }
        List<UUID> ids = jdbc.query(
                "SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ? LIMIT 1",
                (rs, rn) -> rs.getObject(1, UUID.class),
                recoRunId.trim());
        return ids.isEmpty() ? null : ids.get(0);
    }

    private boolean isSubscriptionInvoice(UUID invoiceId) {
        Boolean exists = jdbc.queryForObject("""
                SELECT EXISTS (
                    SELECT 1 FROM client_subscription_billing.subscription_billing_schedule sbs
                    WHERE sbs.invoice_id = ?
                )
                """, Boolean.class, invoiceId);
        return Boolean.TRUE.equals(exists);
    }

    private Map<String, Object> buildSingleInvoiceChain(String recoRunId, UUID runUuid, UUID invoiceId, boolean subscription) {
        Map<String, Object> root = subscription
                ? loadSubscriptionScheduleRootForInvoice(invoiceId)
                : loadCashInvoiceRootForInvoice(invoiceId);
        UUID runFinancialPeriodId = null;
        UUID runLevelId = null;
        String runPeriodCode = null;
        if (recoRunId != null && !recoRunId.isBlank()) {
            List<Map<String, Object>> scopeRows = jdbc.queryForList("""
                    SELECT rr.financial_period_id, rr.level_id,
                           COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS period_code
                    FROM reconciliation.reconciliation_run rr
                    LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                    WHERE rr.run_reference_code = ?
                    """, recoRunId.trim());
            if (!scopeRows.isEmpty()) {
                runFinancialPeriodId = (UUID) scopeRows.get(0).get("financial_period_id");
                runLevelId = (UUID) scopeRows.get(0).get("level_id");
                Object pc = scopeRows.get(0).get("period_code");
                runPeriodCode = pc == null ? null : String.valueOf(pc);
            }
        }
        List<Map<String, Object>> chains = assembleChains(
                recoRunId == null ? "" : recoRunId,
                runUuid,
                runFinancialPeriodId,
                runLevelId,
                runPeriodCode,
                List.of(root),
                List.of(invoiceId),
                subscription);
        return chains.isEmpty() ? Map.of() : chains.get(0);
    }

    private Map<String, Object> loadSubscriptionScheduleRootForInvoice(UUID invoiceId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    sbs.billing_schedule_id AS "billingScheduleId",
                    sbs.subscription_instance_id AS "subscriptionInstanceId",
                    sbs.subscription_plan_id AS "subscriptionPlanId",
                    sbs.invoice_id AS "invoiceId",
                    sbs.billing_date AS "billingDate",
                    sbs.billed_on AS "billedOn",
                    sbs.billing_period_start AS "billingPeriodStart",
                    sbs.billing_period_end AS "billingPeriodEnd",
                    sbs.final_amount AS "scheduleFinalAmount",
                    sbs.cycle_number AS "cycleNumber",
                    sbs.label AS "scheduleLabel",
                    sbs.period_label AS "periodLabel",
                    sbs.billing_run_id AS "scheduleBillingRunId",
                    sbs.isactive AS "scheduleIsActive",
                    bss.status_code AS "billingScheduleStatusCode",
                    bss.display_name AS "billingScheduleStatusName",
                    i.invoice_number AS "invoiceNumber",
                    i.total_amount AS "invoiceTotalAmount",
                    i.created_on AS "invoiceCreatedOn"
                FROM client_subscription_billing.subscription_billing_schedule sbs
                JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
                JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                WHERE sbs.invoice_id = ?
                ORDER BY sbs.billing_schedule_id
                LIMIT 1
                """, invoiceId);
        if (!rows.isEmpty()) {
            return rows.get(0);
        }
        Map<String, Object> fallback = new LinkedHashMap<>();
        fallback.put("invoiceId", invoiceId.toString());
        return fallback;
    }

    private Map<String, Object> loadCashInvoiceRootForInvoice(UUID invoiceId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    i.invoice_id AS "invoiceId",
                    i.invoice_number AS "invoiceNumber",
                    i.total_amount AS "invoiceTotalAmount",
                    i.created_on AS "invoiceCreatedOn",
                    lis.status_name AS "invoiceStatusName"
                FROM transactions.invoice i
                LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                WHERE i.invoice_id = ?
                """, invoiceId);
        return rows.isEmpty() ? Map.of("invoiceId", invoiceId.toString()) : rows.get(0);
    }

    private Map<String, Object> buildWorkspaceResponseFromChain(
            Map<String, Object> chain,
            String entityId,
            String entityType,
            String recoRunId,
            boolean subscription
    ) {
        Map<String, Object> lookup = new LinkedHashMap<>();
        lookup.put("entityId", entityId);
        lookup.put("entityType", entityType == null ? "INVOICE" : entityType);

        @SuppressWarnings("unchecked")
        Map<String, Object> invoiceDetail = (Map<String, Object>) chain.getOrDefault("invoice", Map.of());
        Map<String, Object> invoice = new LinkedHashMap<>(invoiceDetail);
        if (!invoice.containsKey("invoiceAmount") && invoice.get("totalAmount") != null) {
            invoice.put("invoiceAmount", invoice.get("totalAmount"));
        }
        if (!invoice.containsKey("status") && invoice.get("statusName") != null) {
            invoice.put("status", invoice.get("statusName"));
        }

        Map<String, Object> paymentIntent = latestPaymentIntentSummary(chain);
        Map<String, Object> paymentTransaction = latestPaymentTransactionSummary(chain);

        double expected = num(invoice.get("totalAmount") != null ? invoice.get("totalAmount") : invoice.get("invoiceAmount"));
        double actual = totalCapturedMajorFromChain(chain);
        Map<String, Object> liveRow = liveRowFromChain(chain, expected, actual);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lookup", lookup);
        out.put("reconciliationStatus", deriveInvoiceToPaymentStatus(liveRow));
        out.put("recoRunId", recoRunId);
        out.put("reconciliationType", chain.getOrDefault("reconciliationType", subscription ? "SUBSCRIPTION" : "CASH"));
        out.put("expectedAmount", expected);
        out.put("actualAmount", actual);
        out.put("varianceAmount", expected - actual);
        out.put("invoice", invoice);
        out.put("paymentIntent", paymentIntent);
        out.put("paymentTransaction", paymentTransaction);
        out.put("billingSchedule", chain.get("billingSchedule"));
        out.put("paymentIntents", chain.getOrDefault("paymentIntents", List.of()));
        out.put("subscriptionBillingHistory", chain.getOrDefault("subscriptionBillingHistory", List.of()));
        out.put("refunds", chain.getOrDefault("refunds", List.of()));
        out.put("glPosting", chain.getOrDefault("glPosting", Map.of("entries", List.of())));
        out.put("runSnapshots", chain.getOrDefault("runSnapshots", List.of()));
        out.put("stages", chain.getOrDefault("stages", List.of()));
        out.put("exceptions", loadWorkspaceExceptions(entityId, parseUuidLenient(invoice.get("invoiceId")), recoRunId));
        if (recoRunId != null) {
            out.put("run", loadWorkspaceRunHeader(recoRunId));
        }
        return out;
    }

    private Map<String, Object> liveRowFromChain(Map<String, Object> chain, double expected, double actual) {
        Map<String, Object> row = new HashMap<>();
        row.put("invoice_amount", expected);
        row.put("txn_amount", actual);
        row.put("client_payment_transaction_id", latestPaymentTransactionSummary(chain).get("paymentTxnId"));
        return row;
    }

    private Map<String, Object> liveRowFromChain(Map<String, Object> chain) {
        @SuppressWarnings("unchecked")
        Map<String, Object> invoice = (Map<String, Object>) chain.getOrDefault("invoice", Map.of());
        double expected = num(invoice.get("totalAmount") != null ? invoice.get("totalAmount") : invoice.get("invoiceAmount"));
        return liveRowFromChain(chain, expected, totalCapturedMajorFromChain(chain));
    }

    private static double totalCapturedMajorFromChain(Map<String, Object> chain) {
        double sum = 0d;
        for (Object ipObj : listOrEmpty(chain.get("paymentIntents"))) {
            if (!(ipObj instanceof Map<?, ?> ip)) {
                continue;
            }
            for (Object txObj : listOrEmpty(ip.get("paymentTransactions"))) {
                if (!(txObj instanceof Map<?, ?> tx)) {
                    continue;
                }
                Object amt = tx.get("amount");
                if (amt == null) {
                    continue;
                }
                double v = num(amt);
                sum += v > 1000 && v == Math.floor(v) ? v / 100.0 : v;
            }
        }
        return sum;
    }

    private static List<?> listOrEmpty(Object o) {
        if (o instanceof List<?> list) {
            return list;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> latestPaymentIntentSummary(Map<String, Object> chain) {
        List<Map<String, Object>> payloads = (List<Map<String, Object>>) chain.getOrDefault("paymentIntents", List.of());
        if (payloads.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> latest = payloads.get(payloads.size() - 1);
        Map<String, Object> intent = (Map<String, Object>) latest.getOrDefault("intent", Map.of());
        Map<String, Object> summary = new LinkedHashMap<>();
        Object id = intent.get("client_payment_intent_id");
        summary.put("paymentIntentId", id == null ? null : String.valueOf(id));
        Object amt = intent.get("amount");
        if (amt != null) {
            double v = num(amt);
            summary.put("amount", v > 1000 && v == Math.floor(v) ? v / 100.0 : v);
        }
        summary.put("status", intent.get("intent_status_text"));
        summary.put("paymentGatewayName", intent.get("payment_gateway_name"));
        summary.put("currencyCode", intent.get("payment_currency_code"));
        summary.put("paymentMethodTypeCode", intent.get("payment_method_type_code"));
        summary.put("paymentMethodTypeName", intent.get("payment_method_type_name"));
        summary.put("paymentTypeCode", intent.get("intent_payment_type_code"));
        summary.put("paymentTypeName", intent.get("intent_payment_type_name"));
        return summary;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> latestPaymentTransactionSummary(Map<String, Object> chain) {
        Map<String, Object> latestTxn = null;
        for (Object ipObj : listOrEmpty(chain.get("paymentIntents"))) {
            if (!(ipObj instanceof Map<?, ?> ip)) {
                continue;
            }
            for (Object txObj : listOrEmpty(ip.get("paymentTransactions"))) {
                if (txObj instanceof Map<?, ?> tx) {
                    latestTxn = (Map<String, Object>) tx;
                }
            }
        }
        if (latestTxn == null) {
            return Map.of();
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        Object id = latestTxn.get("client_payment_transaction_id");
        summary.put("paymentTxnId", id == null ? null : String.valueOf(id));
        summary.put("gatewayTransactionId", latestTxn.get("payment_gateway_order_id"));
        Object amt = latestTxn.get("amount");
        if (amt != null) {
            double v = num(amt);
            summary.put("amount", v > 1000 && v == Math.floor(v) ? v / 100.0 : v);
        }
        summary.put("gatewayTransactionStatusCode", latestTxn.get("gateway_transaction_status_code"));
        summary.put("gatewayTransactionStatusName", latestTxn.get("gateway_transaction_status_name"));
        @SuppressWarnings("unchecked")
        Map<String, Object> gateway = (Map<String, Object>) latestTxn.getOrDefault("gateway", Map.of());
        summary.put("gateway", gateway);
        return summary;
    }

    private Map<String, Object> loadWorkspaceRunHeader(String recoRunId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rr.run_reference_code AS "reconciliationRunId",
                    rr.run_type AS "runType",
                    st.code AS "runStatus",
                    tm.code AS "triggerMode",
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS "financialPeriod",
                    rr.filters_json->>'currency' AS "currency",
                    CAST(rr.level_id AS text) AS "scopeLevelId",
                    lv_scope."name" AS "scopeLevelName",
                    rr.started_on AS "startedAt",
                    rr.ended_on AS "endedAt",
                    rr.created_on AS "createdAt"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                JOIN reconciliation.lu_reconciliation_trigger_mode tm ON tm.reconciliation_trigger_mode_id = rr.reconciliation_trigger_mode_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = rr.level_id
                WHERE rr.run_reference_code = ?
                """, recoRunId);
        return rows.isEmpty() ? Map.of("reconciliationRunId", recoRunId, "found", false) : rows.get(0);
    }

    private List<Map<String, Object>> loadWorkspaceExceptions(String entityId, UUID invoiceId, String recoRunId) {
        StringBuilder sql = new StringBuilder("""
                SELECT
                    re.exception_reference_code AS "exceptionId",
                    re.exception_reference_code AS "exceptionRef",
                    re.title,
                    re.description,
                    es.code AS "status",
                    sev.code AS "severity",
                    stg.code AS "stage",
                    re.created_on AS "createdAt",
                    re.resolved_at AS "resolvedAt",
                    re.sla_due_at AS "slaDueAt",
                    CAST(re.assignee_user_id AS text) AS "assigneeId"
                FROM reconciliation.reconciliation_exception re
                JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                JOIN reconciliation.lu_severity sev ON sev.severity_id = re.severity_id
                JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = re.reconciliation_stage_id
                WHERE (re.entity_reference_code = ? OR CAST(re.entity_id AS text) = ?)
                """);
        List<Object> args = new ArrayList<>();
        args.add(entityId);
        args.add(invoiceId == null ? entityId : invoiceId.toString());
        if (recoRunId != null && !recoRunId.isBlank()) {
            sql.append("""
                     AND (
                        re.reconciliation_run_id = (
                            SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM reconciliation.reconciliation_run_snapshot s
                            JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                            WHERE s.reconciliation_run_snapshot_id = re.reconciliation_run_snapshot_id
                              AND rr.run_reference_code = ?
                        )
                     )
                    """);
            args.add(recoRunId);
            args.add(recoRunId);
        }
        sql.append(" ORDER BY re.created_on DESC LIMIT 20");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    private Map<String, Object> normalizeWorkspaceRunSnapshot(Map<String, Object> snap) {
        if (!Boolean.TRUE.equals(snap.get("found"))) {
            return snap;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("found", true);
        putSnapshotAlias(out, "snapshotReferenceCode", snap);
        putSnapshotAlias(out, "expectedAmount", snap);
        putSnapshotAlias(out, "actualAmount", snap);
        putSnapshotAlias(out, "reconciliationStatus", snap, "reco_status");
        putSnapshotAlias(out, "severity", snap);
        putSnapshotAlias(out, "mismatchReferenceCode", snap, "mismatch_reference_code");
        putSnapshotAlias(out, "reasonCode", snap, "reason_code");
        putSnapshotAlias(out, "reasonText", snap, "reason_text");
        putSnapshotAlias(out, "eventAt", snap, "event_at");
        Object reconType = snap.get("reconciliationType");
        if (reconType == null && snap.get("attributes_json") instanceof Map<?, ?> attrs) {
            reconType = attrs.get("reconciliationType");
        }
        if (reconType != null) {
            out.put("reconciliationType", reconType);
        }
        return out;
    }

    private static void putSnapshotAlias(Map<String, Object> out, String camelKey, Map<String, Object> snap, String... snakeKeys) {
        Object val = snap.get(camelKey);
        if (val == null) {
            for (String sk : snakeKeys) {
                if (snap.containsKey(sk)) {
                    val = snap.get(sk);
                    break;
                }
            }
        }
        if (val == null && snakeKeys.length == 0) {
            String snake = camelToSnake(camelKey);
            val = snap.get(snake);
        }
        out.put(camelKey, val);
        if (snakeKeys.length > 0) {
            out.put(snakeKeys[0], val);
        } else {
            out.put(camelToSnake(camelKey), val);
        }
    }

    private static String camelToSnake(String camel) {
        return camel.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    private static String deriveInvoiceToPaymentStatus(Map<String, Object> row) {
        boolean hasTxn = row.get("client_payment_transaction_id") != null;
        double inv = num(row.get("invoice_amount"));
        double txn = num(row.get("txn_amount"));
        if (!hasTxn) {
            return "UNRECONCILED";
        }
        if (Math.abs(inv - txn) > 0.01d) {
            return "PARTIAL";
        }
        return "RECONCILED";
    }

    private static Object firstNonNull(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            if (map.containsKey(key) && map.get(key) != null) {
                return map.get(key);
            }
        }
        return null;
    }

    private static double num(Object o) {
        if (o == null) {
            return 0d;
        }
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(o));
        } catch (NumberFormatException e) {
            return 0d;
        }
    }

    private static UUID toUuid(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("reconciliation_run_snapshot_id is required");
        }
        if (o instanceof UUID u) {
            return u;
        }
        return UUID.fromString(String.valueOf(o));
    }

    private Map<String, Object> loadRunSnapshotForEntity(String recoRunId, String entityId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    s.reconciliation_run_snapshot_id,
                    s.snapshot_reference_code AS "snapshotReferenceCode",
                    s.expected_amount AS "expectedAmount",
                    s.actual_amount AS "actualAmount",
                    rcs.code AS "reconciliationStatus",
                    rcs.code AS reco_status,
                    sev.code AS severity,
                    s.mismatch_reference_code AS "mismatchReferenceCode",
                    lr.code AS "reasonCode",
                    s.reason_text AS "reasonText",
                    s.event_at AS "eventAt",
                    s.attributes_json AS "attributesJson"
                FROM reconciliation.reconciliation_run_snapshot s
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                LEFT JOIN reconciliation.lu_severity sev ON sev.severity_id = s.severity_id
                LEFT JOIN reconciliation.lu_reconciliation_reason lr ON lr.reconciliation_reason_id = s.reconciliation_reason_id
                WHERE rr.run_reference_code = ?
                  AND (s.entity_reference_code = ? OR CAST(s.entity_id AS text) = ?)
                ORDER BY s.created_on DESC
                LIMIT 1
                """, recoRunId, entityId, entityId);
        if (rows.isEmpty()) {
            return Map.of("found", false);
        }
        Map<String, Object> r = new HashMap<>(rows.get(0));
        r.put("found", true);
        return r;
    }

    private Map<String, Object> syncRunSnapshotWithLiveIfChanged(String recoRunId, String tenantId, Map<String, Object> liveRow, Map<String, Object> snapshot) {
        Map<String, Object> result = new HashMap<>();
        result.put("performed", true);
        if (!Boolean.TRUE.equals(snapshot.get("found"))) {
            result.put("updated", false);
            result.put("reason", "NO_SNAPSHOT_FOR_RUN");
            return result;
        }
        double liveExpected = num(liveRow.get("invoice_amount"));
        double liveActual = num(liveRow.get("txn_amount"));
        String liveStatus = deriveInvoiceToPaymentStatus(liveRow);

        double snapExpected = num(firstNonNull(snapshot, "expectedAmount", "expected_amount"));
        double snapActual = num(firstNonNull(snapshot, "actualAmount", "actual_amount"));
        Object snapStatusObj = firstNonNull(snapshot, "reconciliationStatus", "reco_status");
        String snapStatus = snapStatusObj == null ? "" : String.valueOf(snapStatusObj);

        boolean unchanged = nearlyEqual(snapExpected, liveExpected) && nearlyEqual(snapActual, liveActual) && snapStatus.equals(liveStatus);
        if (unchanged) {
            result.put("updated", false);
            result.put("reason", "live_matches_snapshot");
            return result;
        }

        UUID snapshotPk = toUuid(snapshot.get("reconciliation_run_snapshot_id"));
        String reasonCode;
        if (liveRow.get("client_payment_transaction_id") == null) {
            reasonCode = "MISSING_PAYMENT";
        } else if ("RECONCILED".equals(liveStatus)) {
            reasonCode = "MATCHED";
        } else {
            reasonCode = "PAYMENT_PARTIAL";
        }
        String reasonText = "Live recompute: invoice vs payment transaction";

        UUID reasonPk = null;
        if (!"MATCHED".equals(reasonCode)) {
            String dbReason = "MISSING_PAYMENT".equals(reasonCode) ? "PAYMENT_NOT_RECEIVED" : "AMOUNT_VARIANCE";
            List<UUID> ids = jdbc.query(
                    "SELECT reconciliation_reason_id FROM reconciliation.lu_reconciliation_reason WHERE code = ? LIMIT 1",
                    (rs, rn) -> rs.getObject(1, UUID.class),
                    dbReason);
            reasonPk = ids.isEmpty() ? null : ids.get(0);
        }

        jdbc.update("""
                        UPDATE reconciliation.reconciliation_run_snapshot s
                        SET expected_amount = ?,
                            actual_amount = ?,
                            reconciliation_status_id = (
                                SELECT reconciliation_status_id FROM reconciliation.lu_reconciliation_status WHERE code = ?
                            ),
                            severity_id = (
                                SELECT severity_id FROM reconciliation.lu_severity
                                WHERE code = CASE WHEN ABS(? - ?) > 1000 THEN 'HIGH' ELSE 'MEDIUM' END
                            ),
                            reconciliation_reason_id = ?,
                            reason_text = ?,
                            event_at = now()
                        WHERE s.reconciliation_run_snapshot_id = ?
                        """,
                liveExpected, liveActual, liveStatus, liveExpected, liveActual, reasonPk, reasonText, snapshotPk);

        UUID runPk = getRunInternalIdByReferenceCode(recoRunId);
        UUID tenantUuid = parseTenantUuidOrNull(tenantId);
        String exceptionRef = upsertOpenDriftException(runPk, snapshotPk, snapshot, liveRow, liveExpected, liveActual, liveStatus, snapExpected, snapActual, snapStatus, tenantUuid);
        result.put("updated", true);
        result.put("exceptionReferenceCode", exceptionRef);
        result.put("previousExpected", snapExpected);
        result.put("previousActual", snapActual);
        result.put("previousStatus", snapStatus);
        result.put("currentExpected", liveExpected);
        result.put("currentActual", liveActual);
        result.put("currentStatus", liveStatus);
        return result;
    }

    private static boolean nearlyEqual(double a, double b) {
        return Math.abs(a - b) < 0.01d;
    }

    private static UUID parseTenantUuidOrNull(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(tenantId);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private String upsertOpenDriftException(
            UUID runId,
            UUID snapshotPk,
            Map<String, Object> snapshot,
            Map<String, Object> liveRow,
            double liveExpected,
            double liveActual,
            String liveStatus,
            double prevExpected,
            double prevActual,
            String prevStatus,
            UUID tenantUuid
    ) {
        List<String> existingRows = jdbc.query("""
                        SELECT re.exception_reference_code
                        FROM reconciliation.reconciliation_exception re
                        JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                        WHERE re.reconciliation_run_snapshot_id = ?
                          AND es.code = 'OPEN'
                        ORDER BY re.created_on DESC
                        LIMIT 1
                        """, (rs, rn) -> rs.getString(1), snapshotPk);
        String existing = existingRows.isEmpty() ? null : existingRows.get(0);
        String desc = "Snapshot drift after live workspace recompute. Was: expected=%s actual=%s status=%s; now: expected=%s actual=%s status=%s"
                .formatted(prevExpected, prevActual, prevStatus, liveExpected, liveActual, liveStatus);
        if (existing != null && !existing.isBlank()) {
            jdbc.update("""
                            UPDATE reconciliation.reconciliation_exception
                            SET description = ?,
                                modified_on = now()
                            WHERE exception_reference_code = ?
                            """, desc, existing);
            return existing;
        }
        String exceptionCode = "EX-" + Math.abs(UUID.randomUUID().hashCode());
        Object invoicePk = liveRow.get("invoice_id");
        String entityRef = liveRow.get("invoice_number") != null ? String.valueOf(liveRow.get("invoice_number")) : String.valueOf(invoicePk);
        jdbc.update("""
                        INSERT INTO reconciliation.reconciliation_exception
                        (exception_reference_code, reconciliation_run_id, reconciliation_run_snapshot_id, mismatch_reference_code,
                         entity_id, entity_reference_code, title, description, reconciliation_stage_id, severity_id,
                         exception_status_id, tags_json, application_id, reconciliation_entity_type_id, created_on, modified_on)
                        VALUES (
                            ?, ?, ?, ?,
                            ?::uuid, ?, ?, ?,
                            (SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = 'INVOICE_TO_PAYMENT'),
                            (SELECT severity_id FROM reconciliation.lu_severity WHERE code = CASE WHEN ABS(? - ?) > 1000 THEN 'HIGH' ELSE 'MEDIUM' END),
                            (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code = 'OPEN'),
                            '[]'::jsonb,
                            ?,
                            (SELECT reconciliation_entity_type_id FROM reconciliation.lu_reconciliation_entity_type WHERE code = 'INVOICE' LIMIT 1),
                            now(), now()
                        )
                        """,
                exceptionCode,
                runId,
                snapshotPk,
                snapshot.get("mismatch_reference_code"),
                invoicePk,
                entityRef,
                "Reconciliation snapshot drift",
                desc,
                liveExpected,
                liveActual,
                tenantUuid);
        return exceptionCode;
    }

    public Map<String, Object> getDashboardSummary(Map<String, Object> filters) {
        String recoRunId = asString(filters.get("recoRunId"));
        if (recoRunId != null && !recoRunId.isBlank()) {
            return loadDashboardSummaryFromRun(recoRunId, filters);
        }
        return emptyFinancialHealthSummary("NO_SNAPSHOT");
    }

    /**
     * Financial health dashboard is snapshot-first: without {@code recoRunId}, do not substitute live totals.
     */
    private Map<String, Object> emptyFinancialHealthSummary(String hint) {
        Map<String, Object> kpis = new HashMap<>();
        kpis.put("totalInvoices", 0);
        kpis.put("totalExpectedAmount", 0.0);
        kpis.put("totalActualAmount", 0.0);
        kpis.put("totalVarianceAmount", 0.0);
        kpis.put("variancePct", 0.0);
        kpis.put("unreconciledCount", 0);
        kpis.put("partialCount", 0);
        kpis.put("reconciledCount", 0);
        kpis.put("criticalExceptions", 0);
        Map<String, Object> out = new HashMap<>();
        out.put("hint", hint);
        out.put("kpis", kpis);
        out.put("chainSummary", List.of());
        out.put("varianceDrivers", List.of());
        out.put("statusBreakdown", List.of());
        Map<String, Object> zeroAgg = emptyScopeAgg();
        out.put("reconciliationScope", Map.of(
                "subscription", Map.of("kpis", buildKpiBlock(zeroAgg, "SUBSCRIPTION"), "chainStages", buildSubscriptionChainStages(zeroAgg, emptyScheduleAgg())),
                "cash", Map.of("kpis", buildKpiBlock(zeroAgg, "CASH"), "chainStages", buildCashChainStages(zeroAgg))
        ));
        out.put("varianceByReconciliationType", buildVarianceByReconciliationType(zeroAgg, zeroAgg, 5, true));
        out.put("meta", Map.of(
                "dashboardLayoutVersion", 2,
                "dashboardLayoutSpecVersion", "1.0",
                "varianceDriverLimit", 5,
                "includeVarianceChart", true
        ));
        return out;
    }

    private static Map<String, Object> emptyScopeAgg() {
        Map<String, Object> m = new HashMap<>();
        m.put("total_invoices", 0);
        m.put("total_expected_amount", 0.0);
        m.put("total_actual_amount", 0.0);
        m.put("mismatch_count", 0);
        m.put("intent_rows", 0);
        m.put("txn_rows", 0);
        return m;
    }

    private static Map<String, Object> emptyScheduleAgg() {
        Map<String, Object> m = new HashMap<>();
        m.put("schedule_count", 0);
        m.put("total_expected_amount", 0.0);
        m.put("total_actual_amount", 0.0);
        m.put("reconciled_schedule_count", 0);
        return m;
    }

    public List<Map<String, Object>> getDashboardMismatches(Map<String, Object> filters, int page, int pageSize) {
        String recoRunId = asString(filters.get("recoRunId"));
        if (recoRunId == null || recoRunId.isBlank()) {
            return List.of();
        }
        return getDashboardMismatchesFromRun(recoRunId, filters, page, pageSize);
    }

    public Map<String, Object> getDashboardTrendsPayload(Map<String, Object> filters) {
        String recoRunId = asString(filters.get("recoRunId"));
        if (recoRunId == null || recoRunId.isBlank()) {
            return Map.of(
                    "series", List.of(),
                    "seriesByReconciliationType", Map.of("SUBSCRIPTION", List.of(), "CASH", List.of())
            );
        }
        String singleType = asString(filters.get("reconciliationType"));
        Map<String, Object> out = new HashMap<>();
        if (singleType != null && !singleType.isBlank()) {
            out.put("series", getDashboardTrendsFromRun(recoRunId, filters, singleType.trim().toUpperCase()));
        } else {
            out.put("series", getDashboardTrendsFromRun(recoRunId, filters, null));
        }
        out.put("seriesByReconciliationType", Map.of(
                "SUBSCRIPTION", getDashboardTrendsFromRun(recoRunId, filters, "SUBSCRIPTION"),
                "CASH", getDashboardTrendsFromRun(recoRunId, filters, "CASH")
        ));
        return out;
    }

    public List<Map<String, Object>> getWorkspaceTimeline(String entityId, String recoRunId) {
        UUID invoiceId = resolveInvoiceId(entityId);
        if (invoiceId == null) {
            return List.of();
        }
        String entityRef = entityId == null ? "" : entityId.trim();
        List<Map<String, Object>> events = new ArrayList<>();

        events.addAll(jdbc.queryForList("""
                SELECT
                    i.created_on AS "eventAt",
                    'BILLING' AS source,
                    'INVOICE_CREATED' AS "eventType",
                    COALESCE(lis.status_name, 'SUCCESS') AS status,
                    i.invoice_number AS "referenceId",
                    'Invoice ' || COALESCE(i.invoice_number, CAST(i.invoice_id AS text)) || ' created' AS details
                FROM transactions.invoice i
                LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                WHERE i.invoice_id = ?
                """, invoiceId));

        events.addAll(jdbc.queryForList("""
                SELECT
                    cpi.created_on AS "eventAt",
                    'PAYMENTS' AS source,
                    'PAYMENT_INTENT_CREATED' AS "eventType",
                    COALESCE(pis.payment_intent_status, 'UNKNOWN') AS status,
                    CAST(cpi.client_payment_intent_id AS text) AS "referenceId",
                    'Payment intent created' AS details
                FROM client_payments.client_payment_intent cpi
                LEFT JOIN client_payments.lu_payment_intent_status pis ON pis.payment_intent_status_id = cpi.intent_status_id
                WHERE cpi.invoice_id = ?
                """, invoiceId));

        events.addAll(jdbc.queryForList("""
                SELECT
                    cpt.created_on AS "eventAt",
                    'PAYMENTS' AS source,
                    'PAYMENT_CAPTURED' AS "eventType",
                    COALESCE(pgts.status_code, 'UNKNOWN') AS status,
                    COALESCE(cpt.payment_gateway_order_id, CAST(cpt.client_payment_transaction_id AS text)) AS "referenceId",
                    'Payment captured' AS details
                FROM client_payments.client_payment_transaction cpt
                JOIN client_payments.client_payment_intent cpi ON cpi.client_payment_intent_id = cpt.client_payment_intent_id
                LEFT JOIN payment_gateway.lu_payment_gateway_transaction_status pgts
                    ON pgts.payment_gateway_transaction_status_id = cpt.payment_gateway_transaction_status_id
                WHERE cpi.invoice_id = ?
                """, invoiceId));

        if (recoRunId != null && !recoRunId.isBlank()) {
            events.addAll(jdbc.queryForList("""
                    SELECT
                        s.event_at AS "eventAt",
                        'RECONCILIATION' AS source,
                        'RUN_SNAPSHOT' AS "eventType",
                        rcs.code AS status,
                        s.snapshot_reference_code AS "referenceId",
                        COALESCE(s.reason_text, 'Reconciliation snapshot') AS details
                    FROM reconciliation.reconciliation_run_snapshot s
                    JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                    JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                    LEFT JOIN reconciliation.lu_reconciliation_entity_type et ON et.reconciliation_entity_type_id = s.reconciliation_entity_type_id
                    WHERE rr.run_reference_code = ?
                      AND (et.code = 'INVOICE' OR et.code IS NULL)
                      AND (s.entity_id = ? OR s.entity_reference_code = ?)
                    """, recoRunId.trim(), invoiceId, entityRef));
        }

        events.addAll(jdbc.queryForList("""
                SELECT
                    re.created_on AS "eventAt",
                    'RECONCILIATION' AS source,
                    'EXCEPTION_OPENED' AS "eventType",
                    es.code AS status,
                    re.exception_reference_code AS "referenceId",
                    COALESCE(re.title, re.description, 'Exception') AS details
                FROM reconciliation.reconciliation_exception re
                JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                WHERE re.entity_id = ? OR re.entity_reference_code = ?
                """, invoiceId, entityRef));

        events.addAll(jdbc.queryForList("""
                SELECT
                    en.created_on AS "eventAt",
                    'RECONCILIATION' AS source,
                    'EXCEPTION_NOTE' AS "eventType",
                    'INFO' AS status,
                    re.exception_reference_code AS "referenceId",
                    LEFT(en.note_text, 500) AS details
                FROM reconciliation.exception_note en
                JOIN reconciliation.reconciliation_exception re ON re.reconciliation_exception_id = en.reconciliation_exception_id
                WHERE re.entity_id = ? OR re.entity_reference_code = ?
                ORDER BY en.created_on DESC
                LIMIT 10
                """, invoiceId, entityRef));

        events.sort(Comparator.comparing(
                m -> m.get("eventAt") == null ? "" : String.valueOf(m.get("eventAt")),
                Comparator.nullsLast(Comparator.reverseOrder())));
        return events.size() <= 50 ? events : events.subList(0, 50);
    }

    public List<Map<String, Object>> getWorkspacePayloads(String entityId, String source) {
        return List.of(Map.of("source", source == null ? "all" : source, "entityId", entityId, "payload", "{}"));
    }

    public Map<String, Object> createReconciliationRun(String runType, String triggerMode, String requestedBy, String tenantId, Object filters) {
        return createReconciliationRun(runType, triggerMode, requestedBy, tenantId, filters, false, null);
    }

    /**
     * @param deferExceptionLifecycle when {@code true}, skips {@link #syncExceptionsWithMismatchSnapshots} so callers
     *                                  (e.g. resync) can run lifecycle once with a source run id.
     * @param priorRunVersionForSequence when non-null (e.g. resync), the new run's {@code filters_json.runVersion} is
     *                                     {@code max(existing max for tenant + financial period, prior) + 1} so the
     *                                     sequence stays monotonic and strictly after the source run.
     */
    public Map<String, Object> createReconciliationRun(
            String runType,
            String triggerMode,
            String requestedBy,
            String tenantId,
            Object filters,
            boolean deferExceptionLifecycle,
            Integer priorRunVersionForSequence
    ) {
        return createReconciliationRunInternal(runType, triggerMode, requestedBy, tenantId, filters, deferExceptionLifecycle, priorRunVersionForSequence);
    }

    public Map<String, Object> createReconciliationRun(String runType, String triggerMode, String requestedBy, String tenantId, Object filters, boolean deferExceptionLifecycle) {
        return createReconciliationRunInternal(runType, triggerMode, requestedBy, tenantId, filters, deferExceptionLifecycle, null);
    }

    private Map<String, Object> createReconciliationRunInternal(
            String runType,
            String triggerMode,
            String requestedBy,
            String tenantId,
            Object filters,
            boolean deferExceptionLifecycle,
            Integer priorRunVersionForSequence
    ) {
        Map<String, Object> filterMap = normalizeFiltersObject(filters);
        filterMap.remove("recoRunId");
        enrichRunCatalogFields(filterMap);
        UUID financialPeriodId = resolveOrEnsureFinancialPeriodId(asString(filterMap.get("financialPeriod")));
        UUID levelId = resolveLevelIdFromFilters(filterMap);
        UUID locationId = firstLocationIdFromFilters(filterMap);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        UUID requestedByUuid = parseOptionalUuid(requestedBy);
        int runVersion = computeNextRunVersion(tenantUuid, financialPeriodId, priorRunVersionForSequence);
        filterMap.put("runVersion", runVersion);
        String refCode = "RR-" + Math.abs(UUID.randomUUID().hashCode());
        if (locationId != null) {
            filterMap.putIfAbsent("locationIds", List.of(locationId.toString()));
        }
        jdbc.update("""
                INSERT INTO reconciliation.reconciliation_run
                (run_reference_code, run_type, reconciliation_run_status_id, reconciliation_trigger_mode_id, application_id, requested_by, financial_period_id, level_id, filters_json, started_on, created_on, modified_on)
                VALUES (
                    ?,
                    ?,
                    (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code='RUNNING'),
                    (SELECT reconciliation_trigger_mode_id FROM reconciliation.lu_reconciliation_trigger_mode WHERE code=?),
                    ?,
                    ?,
                    ?,
                    ?,
                    ?::jsonb,
                    now(), now(), now()
                )
                """, refCode, runType, triggerMode == null ? "MANUAL" : triggerMode.toUpperCase(), tenantUuid, requestedByUuid, financialPeriodId, levelId, toJson(filterMap));
        UUID runUuid = getRunInternalIdByReferenceCode(refCode);
        try {
            Map<String, Object> summary = computeDashboardSummaryLive(filterMap);
            jdbc.update("""
                    UPDATE reconciliation.reconciliation_run
                    SET summary_json = ?::jsonb, modified_on = now()
                    WHERE run_reference_code = ?
                    """, objectMapper.writeValueAsString(summary), refCode);
            int inserted = insertMismatchSnapshotsForRun(runUuid, filterMap);
            jdbc.update("""
                    UPDATE reconciliation.reconciliation_run
                    SET reconciliation_run_status_id = (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code = 'COMPLETED'),
                        ended_on = now(),
                        modified_on = now()
                    WHERE run_reference_code = ?
                    """, refCode);
            Map<String, Object> out = new HashMap<>();
            out.put("reconciliationRunId", refCode);
            out.put("status", "COMPLETED");
            out.put("snapshotRowsInserted", inserted);
            out.put("runVersion", runVersion);
            if (!deferExceptionLifecycle) {
                out.putAll(syncExceptionsWithMismatchSnapshots(runUuid, refCode, tenantUuid, null, null));
            }
            int glLinked = linkGlEntriesToRun(refCode);
            out.put("glEntriesLinked", glLinked);
            return out;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to persist run summary", e);
        }
    }

    /**
     * Re-sync from an existing run: loads {@code filters_json} + {@code run_type} + trigger mode from the source run,
     * then runs the same pipeline as {@link #createReconciliationRun} so a new {@code run_reference_code}
     * is issued, live source data is queried for that scope, new mismatch snapshots are written for the new run, and
     * the new run is marked completed. Source run rows are unchanged.
     */
    public Map<String, Object> resyncReconciliationRun(String sourceRecoRunId, String requestedBy, String tenantId) {
        if (sourceRecoRunId == null || sourceRecoRunId.isBlank()) {
            return Map.of("reconciliationRunId", "", "status", "INVALID", "message", "reconciliationRunId required");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rr.run_type AS "runType",
                    tm.code AS "triggerMode",
                    rr.filters_json::text AS "filtersJsonText"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_trigger_mode tm ON tm.reconciliation_trigger_mode_id = rr.reconciliation_trigger_mode_id
                WHERE rr.run_reference_code = ?
                """, sourceRecoRunId);
        if (rows.isEmpty()) {
            return Map.of("reconciliationRunId", sourceRecoRunId, "status", "NOT_FOUND", "message", "Run not found");
        }
        Map<String, Object> row0 = rows.get(0);
        Map<String, Object> filterMap = new HashMap<>(parseRunFiltersJson(row0.get("filtersJsonText")));
        filterMap.remove("recoRunId");
        int sourceRunVersion = parseStoredRunVersion(filterMap);
        filterMap.put("clonedFromReconciliationRunId", sourceRecoRunId);
        String runType = row0.get("runType") != null ? String.valueOf(row0.get("runType")).trim() : "FULL";
        if (runType.isEmpty()) {
            runType = "FULL";
        }
        String triggerMode = row0.get("triggerMode") != null ? String.valueOf(row0.get("triggerMode")).trim() : "MANUAL";
        if (triggerMode.isEmpty()) {
            triggerMode = "MANUAL";
        }
        Map<String, Object> created = createReconciliationRun(runType, triggerMode, requestedBy, tenantId, filterMap, true, sourceRunVersion);
        Map<String, Object> out = new HashMap<>(created);
        out.put("sourceReconciliationRunId", sourceRecoRunId);
        out.put("sourceRunVersion", sourceRunVersion);
        String newRef = Objects.toString(created.get("reconciliationRunId"), null);
        if (newRef != null && !newRef.isBlank() && "COMPLETED".equals(created.get("status"))) {
            UUID sourceRunUuid = getRunInternalIdByReferenceCode(sourceRecoRunId);
            UUID newRunUuid = getRunInternalIdByReferenceCode(newRef);
            UUID tenantUuid = parseOptionalUuid(tenantId);
            Map<String, Object> lifecycle = syncExceptionsWithMismatchSnapshots(
                    newRunUuid, newRef, tenantUuid, sourceRunUuid, sourceRecoRunId);
            out.putAll(lifecycle);
        }
        return out;
    }

    /**
     * Aligns operational exceptions with mismatch snapshots for a run (plain create or after resync persistence).
     * Supersedes duplicate open rows for the same key, carries forward ongoing issues, auto-resolves cleared mismatches
     * (when {@code sourceRunUuid} is set), links globally matching opens on plain create, inserts new opens, and
     * appends {@code reconciliation.audit_log} rows (plus existing {@code exception_note} where applicable).
     */
    private Map<String, Object> syncExceptionsWithMismatchSnapshots(
            UUID runUuid,
            String runRefCode,
            UUID tenantUuid,
            UUID sourceRunUuid,
            String sourceRefCode
    ) {
        int carried = 0;
        int autoResolved = 0;
        int created = 0;
        int superseded = 0;
        final boolean isResync = sourceRunUuid != null;
        final String srcRef = sourceRefCode != null ? sourceRefCode : "";

        List<Map<String, Object>> newSnapshots = jdbc.queryForList("""
                SELECT
                    s.reconciliation_run_snapshot_id AS "snapshotId",
                    s.entity_id AS "entityId",
                    s.reconciliation_stage_id AS "stageId",
                    s.expected_amount AS "expectedAmount",
                    s.actual_amount AS "actualAmount",
                    s.mismatch_reference_code AS "mismatchRef",
                    s.entity_reference_code AS "entityRef",
                    COALESCE(s.attributes_json->>'reconciliationType', '') AS "recoType",
                    s.severity_id AS "severityId",
                    lr.code AS "reasonCode",
                    s.reason_text AS "reasonText"
                FROM reconciliation.reconciliation_run_snapshot s
                LEFT JOIN reconciliation.lu_reconciliation_reason lr ON lr.reconciliation_reason_id = s.reconciliation_reason_id
                WHERE s.reconciliation_run_id = ?
                """, runUuid);

        Map<String, Map<String, Object>> keyToNewSnap = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> looseEntityStageToNewSnaps = new LinkedHashMap<>();
        for (Map<String, Object> snap : newSnapshots) {
            UUID entityId = toUuid(snap.get("entityId"));
            UUID stageId = toUuid(snap.get("stageId"));
            if (entityId == null || stageId == null) {
                continue;
            }
            String key = exceptionMatchKey(entityId, stageId, Objects.toString(snap.get("recoType"), ""));
            keyToNewSnap.putIfAbsent(key, snap);
            String loose = entityId + "|" + stageId;
            looseEntityStageToNewSnaps.computeIfAbsent(loose, k -> new ArrayList<>()).add(snap);
        }

        List<Map<String, Object>> oldPrimaries = new ArrayList<>();
        if (isResync) {
            List<Map<String, Object>> oldActive = jdbc.queryForList("""
                    SELECT
                        re.reconciliation_exception_id AS "exceptionPk",
                        re.exception_reference_code AS "exceptionRef",
                        re.entity_id AS "entityId",
                        re.reconciliation_stage_id AS "stageId",
                        COALESCE(NULLIF(TRIM(s.attributes_json->>'reconciliationType'), ''), '') AS "recoType",
                        s.expected_amount AS "prevExpected",
                        s.actual_amount AS "prevActual",
                        re.created_on AS "createdOn"
                    FROM reconciliation.reconciliation_exception re
                    JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                    LEFT JOIN reconciliation.reconciliation_run_snapshot s ON s.reconciliation_run_snapshot_id = re.reconciliation_run_snapshot_id
                    WHERE re.reconciliation_run_id = ?
                      AND es.code IN ('OPEN', 'IN_PROGRESS', 'ESCALATED')
                    ORDER BY re.created_on ASC
                    """, sourceRunUuid);
            Map<String, List<Map<String, Object>>> oldByKey = new LinkedHashMap<>();
            for (Map<String, Object> ex : oldActive) {
                UUID eid = toUuid(ex.get("entityId"));
                UUID sid = toUuid(ex.get("stageId"));
                if (eid == null || sid == null) {
                    continue;
                }
                String k = exceptionMatchKey(eid, sid, Objects.toString(ex.get("recoType"), ""));
                oldByKey.computeIfAbsent(k, x -> new ArrayList<>()).add(ex);
            }
            for (List<Map<String, Object>> group : oldByKey.values()) {
                if (group.isEmpty()) {
                    continue;
                }
                if (group.size() == 1) {
                    oldPrimaries.add(group.get(0));
                    continue;
                }
                group.sort(Comparator.comparing(ReconciliationModuleRepository::createdOnMillis));
                Map<String, Object> primary = group.get(0);
                UUID canonPk = toUuid(primary.get("exceptionPk"));
                String canonRef = Objects.toString(primary.get("exceptionRef"), "");
                oldPrimaries.add(primary);
                for (int i = 1; i < group.size(); i++) {
                    Map<String, Object> dup = group.get(i);
                    UUID dupPk = toUuid(dup.get("exceptionPk"));
                    String dupRef = Objects.toString(dup.get("exceptionRef"), "");
                    if (dupPk == null || dupRef.isBlank() || canonPk == null || canonRef.isBlank()) {
                        continue;
                    }
                    superseded += supersedeDuplicateOpenException(
                            dupPk,
                            dupRef,
                            canonPk,
                            canonRef,
                            "Consolidated duplicate open rows on source run " + srcRef + ".",
                            tenantUuid);
                }
            }
        }

        Set<String> newSnapshotKeysMatched = new LinkedHashSet<>();

        for (Map<String, Object> ex : oldPrimaries) {
            UUID entityId = toUuid(ex.get("entityId"));
            UUID stageId = toUuid(ex.get("stageId"));
            if (entityId == null || stageId == null) {
                continue;
            }
            String key = exceptionMatchKey(entityId, stageId, Objects.toString(ex.get("recoType"), ""));
            Map<String, Object> newSnap = keyToNewSnap.get(key);
            if (newSnap == null && Objects.toString(ex.get("recoType"), "").isBlank()) {
                String loose = entityId + "|" + stageId;
                List<Map<String, Object>> cands = looseEntityStageToNewSnaps.get(loose);
                if (cands != null && cands.size() == 1) {
                    newSnap = cands.get(0);
                }
            }
            UUID exPk = toUuid(ex.get("exceptionPk"));
            String exRef = Objects.toString(ex.get("exceptionRef"), null);
            if (exPk == null || exRef == null) {
                continue;
            }
            if (newSnap != null) {
                UUID newSnapPk = toUuid(newSnap.get("snapshotId"));
                if (newSnapPk == null) {
                    continue;
                }
                jdbc.update("""
                        UPDATE reconciliation.reconciliation_exception
                        SET latest_reconciliation_run_id = ?,
                            reconciliation_run_id = ?,
                            reconciliation_run_snapshot_id = ?,
                            occurrence_count = COALESCE(occurrence_count, 1) + 1,
                            last_seen_at = now(),
                            origin_reconciliation_run_id = COALESCE(origin_reconciliation_run_id, reconciliation_run_id),
                            modified_on = now()
                        WHERE reconciliation_exception_id = ?
                        """,
                        runUuid,
                        runUuid,
                        newSnapPk,
                        exPk);
                double prevVar = varianceFromAmounts(ex.get("prevExpected"), ex.get("prevActual"));
                double newVar = varianceFromAmounts(newSnap.get("expectedAmount"), newSnap.get("actualAmount"));
                if (!nearlyEqual(prevVar, newVar)) {
                    String note = "Variance changed (%s): previous delta %.2f → now %.2f."
                            .formatted(isResync ? ("resync " + srcRef + " → " + runRefCode) : ("run " + runRefCode), prevVar, newVar);
                    addExceptionNote(exRef, note);
                    writeLifecycleAuditLog(tenantUuid, "EXCEPTION_VARIANCE_CHANGED", exRef, Map.of(
                            "previousVariance", prevVar,
                            "newVariance", newVar,
                            "runReferenceCode", runRefCode,
                            "isResync", isResync));
                }
                writeLifecycleAuditLog(tenantUuid, "EXCEPTION_CARRIED_FORWARD", exRef, Map.of(
                        "runReferenceCode", runRefCode,
                        "isResync", isResync));
                String matchedKey = exceptionMatchKey(
                        entityId,
                        stageId,
                        Objects.toString(newSnap.get("recoType"), ""));
                newSnapshotKeysMatched.add(matchedKey);
                carried++;
            } else {
                if (!isResync) {
                    continue;
                }
                String autoMsg = "Auto-resolved: invoice mismatch no longer present after resync (new run " + runRefCode + ").";
                jdbc.update("""
                        UPDATE reconciliation.reconciliation_exception
                        SET exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code = 'AUTO_RESOLVED'),
                            is_auto_resolved = true,
                            resolved_at = now(),
                            latest_reconciliation_run_id = ?,
                            resolution_note = CASE
                                WHEN resolution_note IS NULL OR LENGTH(TRIM(resolution_note)) = 0 THEN ?
                                ELSE resolution_note || E'\n' || ?
                            END,
                            modified_on = now()
                        WHERE reconciliation_exception_id = ?
                        """,
                        runUuid,
                        autoMsg,
                        autoMsg,
                        exPk);
                addExceptionNote(exRef, "System: mismatch cleared after resync (source run " + srcRef + " → " + runRefCode + ").");
                writeLifecycleAuditLog(tenantUuid, "EXCEPTION_AUTO_RESOLVED", exRef, Map.of(
                        "sourceRunReferenceCode", srcRef,
                        "newRunReferenceCode", runRefCode));
                autoResolved++;
            }
        }

        for (Map.Entry<String, Map<String, Object>> e : keyToNewSnap.entrySet()) {
            if (newSnapshotKeysMatched.contains(e.getKey())) {
                continue;
            }
            Map<String, Object> snap = e.getValue();
            UUID newSnapPk = toUuid(snap.get("snapshotId"));
            UUID entityId = toUuid(snap.get("entityId"));
            UUID stageId = toUuid(snap.get("stageId"));
            if (newSnapPk == null || entityId == null || stageId == null) {
                continue;
            }
            String snapReco = Objects.toString(snap.get("recoType"), "");
            List<Map<String, Object>> global = findTenantActiveExceptionsForEntityAndStage(entityId, stageId, tenantUuid);
            List<Map<String, Object>> matching = global.stream()
                    .filter(row -> exceptionRowMatchesSnapshotReco(row, entityId, stageId, snapReco))
                    .collect(Collectors.toList());
            if (matching.size() > 1) {
                matching.sort(Comparator.comparing(ReconciliationModuleRepository::createdOnMillis));
                Map<String, Object> canon = matching.get(0);
                UUID canonPk = toUuid(canon.get("exceptionPk"));
                String canonRef = Objects.toString(canon.get("exceptionRef"), "");
                for (int i = 1; i < matching.size(); i++) {
                    Map<String, Object> dup = matching.get(i);
                    UUID dupPk = toUuid(dup.get("exceptionPk"));
                    String dupRef = Objects.toString(dup.get("exceptionRef"), "");
                    if (dupPk == null || dupRef.isBlank() || canonPk == null || canonRef.isBlank()) {
                        continue;
                    }
                    superseded += supersedeDuplicateOpenException(
                            dupPk,
                            dupRef,
                            canonPk,
                            canonRef,
                            "Consolidated duplicate open rows before attaching to run " + runRefCode + ".",
                            tenantUuid);
                }
            }
            if (!matching.isEmpty()) {
                Map<String, Object> primary = matching.get(0);
                UUID exPk = toUuid(primary.get("exceptionPk"));
                String exRef = Objects.toString(primary.get("exceptionRef"), null);
                if (exPk == null || exRef == null) {
                    continue;
                }
                jdbc.update("""
                        UPDATE reconciliation.reconciliation_exception
                        SET latest_reconciliation_run_id = ?,
                            reconciliation_run_id = ?,
                            reconciliation_run_snapshot_id = ?,
                            occurrence_count = COALESCE(occurrence_count, 1) + 1,
                            last_seen_at = now(),
                            origin_reconciliation_run_id = COALESCE(origin_reconciliation_run_id, reconciliation_run_id),
                            modified_on = now()
                        WHERE reconciliation_exception_id = ?
                        """,
                        runUuid,
                        runUuid,
                        newSnapPk,
                        exPk);
                double prevVar = varianceFromAmounts(primary.get("prevExpected"), primary.get("prevActual"));
                double newVar = varianceFromAmounts(snap.get("expectedAmount"), snap.get("actualAmount"));
                if (!nearlyEqual(prevVar, newVar)) {
                    String note = "Variance changed (%s): previous delta %.2f → now %.2f."
                            .formatted(isResync ? ("resync " + srcRef + " → " + runRefCode) : ("run " + runRefCode), prevVar, newVar);
                    addExceptionNote(exRef, note);
                    writeLifecycleAuditLog(tenantUuid, "EXCEPTION_VARIANCE_CHANGED", exRef, Map.of(
                            "previousVariance", prevVar,
                            "newVariance", newVar,
                            "runReferenceCode", runRefCode,
                            "isResync", isResync));
                }
                writeLifecycleAuditLog(tenantUuid, "EXCEPTION_CARRIED_FORWARD", exRef, Map.of(
                        "runReferenceCode", runRefCode,
                        "isResync", isResync));
                newSnapshotKeysMatched.add(e.getKey());
                carried++;
                continue;
            }
            String exceptionCode = "EX-" + Math.abs(UUID.randomUUID().hashCode());
            String entityRef = snap.get("entityRef") != null ? String.valueOf(snap.get("entityRef")) : String.valueOf(entityId);
            String desc = Objects.toString(snap.get("reasonText"), "Invoice amount vs captured transaction");
            jdbc.update("""
                            INSERT INTO reconciliation.reconciliation_exception (
                                exception_reference_code,
                                reconciliation_run_id,
                                reconciliation_run_snapshot_id,
                                mismatch_reference_code,
                                entity_id, entity_reference_code, title, description,
                                reconciliation_stage_id, severity_id, exception_status_id, tags_json, application_id,
                                reconciliation_entity_type_id,
                                origin_reconciliation_run_id, latest_reconciliation_run_id,
                                first_detected_at, last_seen_at, occurrence_count, created_on, modified_on
                            ) VALUES (
                                ?, ?, ?, ?,
                                ?::uuid, ?, 'Invoice reconciliation mismatch', ?,
                                ?, ?, (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code = 'OPEN'),
                                '[]'::jsonb, ?,
                                (SELECT reconciliation_entity_type_id FROM reconciliation.lu_reconciliation_entity_type WHERE code = 'INVOICE' LIMIT 1),
                                ?, ?, now(), now(), 1, now(), now()
                            )
                            """,
                    exceptionCode,
                    runUuid,
                    newSnapPk,
                    snap.get("mismatchRef"),
                    entityId,
                    entityRef,
                    desc,
                    stageId,
                    snap.get("severityId"),
                    tenantUuid,
                    runUuid,
                    runUuid);
            String openNote = isResync
                    ? ("Opened from resync run " + runRefCode + " (new mismatch vs prior run " + srcRef + ").")
                    : ("Opened from reconciliation run " + runRefCode + ".");
            addExceptionNote(exceptionCode, openNote);
            writeLifecycleAuditLog(tenantUuid, "EXCEPTION_OPENED_FROM_RUN", exceptionCode, Map.of(
                    "runReferenceCode", runRefCode,
                    "isResync", isResync));
            created++;
        }

        return Map.of(
                "exceptionLifecycleCarriedForward", carried,
                "exceptionLifecycleAutoResolved", autoResolved,
                "exceptionLifecycleCreated", created,
                "exceptionLifecycleSuperseded", superseded
        );
    }

    private void writeLifecycleAuditLog(UUID tenantUuid, String actionCode, String exceptionReferenceCode, Map<String, Object> details) {
        try {
            jdbc.update("""
                    INSERT INTO reconciliation.audit_log (application_id, action_code, entity_type, entity_reference_code, details_json)
                    VALUES (?, ?, 'RECONCILIATION_EXCEPTION', ?, ?::jsonb)
                    """,
                    tenantUuid,
                    actionCode,
                    exceptionReferenceCode,
                    objectMapper.writeValueAsString(details == null ? Map.of() : details));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize lifecycle audit details", e);
        }
    }

    private int supersedeDuplicateOpenException(
            UUID duplicatePk,
            String duplicateRef,
            UUID canonicalPk,
            String canonicalRef,
            String suffix,
            UUID tenantUuid
    ) {
        String note = "Superseded: consolidated to " + canonicalRef + ". " + suffix;
        jdbc.update("""
                UPDATE reconciliation.reconciliation_exception
                SET exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code = 'SUPERSEDED'),
                    superseded_by_exception_id = ?,
                    resolved_at = now(),
                    resolution_note = CASE
                        WHEN resolution_note IS NULL OR LENGTH(TRIM(resolution_note)) = 0 THEN ?
                        ELSE resolution_note || E'\n' || ?
                    END,
                    modified_on = now()
                WHERE reconciliation_exception_id = ?
                """,
                canonicalPk,
                note,
                note,
                duplicatePk);
        addExceptionNote(duplicateRef, note);
        writeLifecycleAuditLog(tenantUuid, "EXCEPTION_SUPERSEDED", duplicateRef, Map.of(
                "canonicalExceptionReference", canonicalRef,
                "canonicalExceptionId", canonicalPk.toString()));
        return 1;
    }

    private static long createdOnMillis(Map<String, Object> row) {
        Object c = row.get("createdOn");
        if (c instanceof Timestamp t) {
            return t.getTime();
        }
        if (c instanceof java.util.Date d) {
            return d.getTime();
        }
        return 0L;
    }

    private List<Map<String, Object>> findTenantActiveExceptionsForEntityAndStage(UUID entityId, UUID stageId, UUID tenantUuid) {
        return jdbc.queryForList("""
                SELECT
                    re.reconciliation_exception_id AS "exceptionPk",
                    re.exception_reference_code AS "exceptionRef",
                    re.entity_id AS "entityId",
                    re.reconciliation_stage_id AS "stageId",
                    COALESCE(NULLIF(TRIM(s.attributes_json->>'reconciliationType'), ''), '') AS "recoType",
                    re.created_on AS "createdOn",
                    s.expected_amount AS "prevExpected",
                    s.actual_amount AS "prevActual"
                FROM reconciliation.reconciliation_exception re
                JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                LEFT JOIN reconciliation.reconciliation_run_snapshot s ON s.reconciliation_run_snapshot_id = re.reconciliation_run_snapshot_id
                WHERE re.entity_id = ?
                  AND re.reconciliation_stage_id = ?
                  AND es.code IN ('OPEN', 'IN_PROGRESS', 'ESCALATED')
                  AND (re.application_id IS NOT DISTINCT FROM ?)
                ORDER BY re.created_on ASC
                """, entityId, stageId, tenantUuid);
    }

    private static boolean exceptionRowMatchesSnapshotReco(Map<String, Object> exRow, UUID entityId, UUID stageId, String snapReco) {
        String sr = snapReco == null ? "" : snapReco.trim();
        String er = Objects.toString(exRow.get("recoType"), "").trim();
        if (!er.isEmpty() && !sr.isEmpty()) {
            return exceptionMatchKey(entityId, stageId, er).equals(exceptionMatchKey(entityId, stageId, sr));
        }
        if (er.isEmpty()) {
            return true;
        }
        return exceptionMatchKey(entityId, stageId, er).equals(exceptionMatchKey(entityId, stageId, sr));
    }

    private static String exceptionMatchKey(UUID entityId, UUID stageId, String recoType) {
        return entityId + "|" + stageId + "|" + (recoType == null ? "" : recoType.trim());
    }

    private static double varianceFromAmounts(Object expected, Object actual) {
        return num(expected) - num(actual);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeFiltersObject(Object filters) {
        if (filters == null) {
            return new HashMap<>();
        }
        if (filters instanceof Map<?, ?> m) {
            return new HashMap<>((Map<String, Object>) m);
        }
        return new HashMap<>();
    }

    private UUID getRunInternalIdByReferenceCode(String refCode) {
        return jdbc.queryForObject("""
                SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                """, UUID.class, refCode);
    }

    /**
     * Invoice linked to {@code subscription_billing_schedule} → SUBSCRIPTION; else CASH / POS-style.
     * Same KPI + dual-scope dashboard layout (v2) for run snapshot persistence.
     */
    private static final String SQL_INVOICE_IS_SUBSCRIPTION = """
            EXISTS (
                SELECT 1 FROM client_subscription_billing.subscription_billing_schedule sbs
                WHERE sbs.invoice_id = i.invoice_id
            )""";

    /**
     * CASH + SUBSCRIPTION reconciliation scope: only invoices whose {@code transactions.lu_invoice_status.status_name}
     * is one of the collectible / issued lifecycle states (excludes e.g. {@code PENDING} drafts and {@code VOID}).
     */
    private static final String SQL_INVOICE_STATUS_IN_RECONCILIATION_SCOPE = """
            EXISTS (
                SELECT 1 FROM transactions.lu_invoice_status inv_scope
                WHERE inv_scope.invoice_status_id = i.invoice_status_id
                  AND UPPER(TRIM(COALESCE(inv_scope.status_name, ''))) IN (
                      'ISSUED',
                      'PARTIALLY_PAID',
                      'PARTIALLY PAID',
                      'PAID',
                      'DUE'
                  )
            )""";

    /**
     * Same KPI logic as getDashboardSummary but without recoRunId branching (live only).
     */
    private Map<String, Object> computeDashboardSummaryLive(Map<String, Object> filters) {
        Map<String, Object> combinedAgg = computeScopeInvoicePaymentAgg(filters, (Boolean) null);
        double expected = ((Number) combinedAgg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actualAmount = ((Number) combinedAgg.getOrDefault("total_actual_amount", 0)).doubleValue();
        double variance = expected - actualAmount;
        int mismatchCount = ((Number) combinedAgg.getOrDefault("mismatch_count", 0)).intValue();
        int totalInvoices = ((Number) combinedAgg.getOrDefault("total_invoices", 0)).intValue();
        int reconciledCount = Math.max(0, totalInvoices - mismatchCount);

        Map<String, Object> subAgg = computeScopeInvoicePaymentAgg(filters, true);
        Map<String, Object> cashAgg = computeScopeInvoicePaymentAgg(filters, false);
        Map<String, Object> scheduleAgg = computeBillingScheduleAgg(filters);

        Map<String, Object> subKpis = buildKpiBlock(subAgg, "SUBSCRIPTION");
        Map<String, Object> cashKpis = buildKpiBlock(cashAgg, "CASH");
        Map<String, Object> glPack = computeGlPostingMetricsPackForFilters(filters);
        int driverLimit = parsePositiveInt(filters.get("varianceDriverLimit"), 5);
        List<Map<String, Object>> subUnreconciled = listUnreconciledItemsLive(filters, true, driverLimit);
        List<Map<String, Object>> cashUnreconciled = listUnreconciledItemsLive(filters, false, driverLimit);
        List<Map<String, Object>> subStages = attachUnreconciledItemsToStages(
                withGlPostingStage(
                        buildSubscriptionChainStages(subAgg, scheduleAgg), 8,
                        scopeGlMetrics(glPack, "subscription"),
                        scopeInvoiceMismatch(subKpis)),
                subUnreconciled);
        List<Map<String, Object>> cashStages = attachUnreconciledItemsToStages(
                withGlPostingStage(
                        buildCashChainStages(cashAgg), 5,
                        scopeGlMetrics(glPack, "cash"),
                        scopeInvoiceMismatch(cashKpis)),
                cashUnreconciled);

        boolean includeChart = parseBooleanDefault(filters.get("includeVarianceChart"), true);
        Map<String, Object> varianceByType = buildVarianceByReconciliationType(subAgg, cashAgg, driverLimit, includeChart);

        Map<String, Object> summary = new HashMap<>();
        summary.put("kpis", Map.of("totalInvoices", totalInvoices, "totalExpectedAmount", expected, "totalActualAmount", actualAmount,
                "totalVarianceAmount", variance, "variancePct", expected == 0 ? 0 : Math.abs(variance) * 100.0 / expected,
                "unreconciledCount", mismatchCount, "partialCount", mismatchCount, "reconciledCount", reconciledCount, "criticalExceptions", 0));
        summary.put("chainSummary", List.of(Map.of("stage", "INVOICE_TO_PAYMENT", "total", totalInvoices, "mismatch", mismatchCount,
                "mismatchPct", totalInvoices == 0 ? 0 : (mismatchCount * 100.0 / totalInvoices))));
        summary.put("varianceDrivers", buildVarianceDrivers(mismatchCount, Math.abs(variance), subUnreconciled, cashUnreconciled));
        summary.put("statusBreakdown", buildStatusBreakdown(reconciledCount, mismatchCount));
        summary.put("reconciliationScope", Map.of(
                "subscription", buildReconciliationScopeBlock(subKpis, subStages, subUnreconciled),
                "cash", buildReconciliationScopeBlock(cashKpis, cashStages, cashUnreconciled)
        ));
        summary.put("varianceByReconciliationType", varianceByType);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("dashboardLayoutVersion", 2);
        meta.put("dashboardLayoutSpecVersion", "1.0");
        meta.put("varianceDriverLimit", driverLimit);
        meta.put("includeVarianceChart", includeChart);
        meta.put("subscriptionCashClassification", "SUBSCRIPTION = invoice_id in client_subscription_billing.subscription_billing_schedule; else CASH");
        meta.put("invoiceStatusScope", List.of("ISSUED", "PARTIALLY_PAID", "PAID", "DUE"));
        meta.put("glPosting", glPack);
        summary.put("meta", meta);
        return summary;
    }

    private Map<String, Object> buildReconciliationScopeBlock(
            Map<String, Object> kpis,
            List<Map<String, Object>> chainStages,
            List<Map<String, Object>> unreconciledItems
    ) {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kpis", kpis);
        block.put("chainStages", chainStages);
        block.put("unreconciledItems", unreconciledItems);
        int total = ((Number) kpis.getOrDefault("totalInvoices", 0)).intValue();
        int unreconciled = ((Number) kpis.getOrDefault("unreconciledCount", 0)).intValue();
        block.put("paidSummary", Map.of(
                "totalUnits", total,
                "reconciledUnits", Math.max(0, total - unreconciled),
                "unreconciledUnits", unreconciled,
                "paidPct", total == 0 ? 0.0 : Math.max(0, total - unreconciled) * 100.0 / total
        ));
        return block;
    }

    private List<Map<String, Object>> buildVarianceDrivers(
            int mismatchCount,
            double varianceAmount,
            List<Map<String, Object>> subUnreconciled,
            List<Map<String, Object>> cashUnreconciled
    ) {
        if (mismatchCount <= 0) {
            return List.of();
        }
        List<Map<String, Object>> items = new ArrayList<>();
        items.addAll(subUnreconciled);
        items.addAll(cashUnreconciled);
        return List.of(Map.of(
                "driver", "Amount mismatch",
                "count", mismatchCount,
                "amount", varianceAmount,
                "items", items
        ));
    }

    /**
     * Invoices (or subscription schedules) in scope where invoice total does not match captured payment.
     */
    private List<Map<String, Object>> listUnreconciledItemsLive(Map<String, Object> filters, boolean subscriptionScope, int limit) {
        int maxRows = Math.max(1, Math.min(limit, 100));
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String scope = subscriptionScope
                ? (" AND " + SQL_INVOICE_IS_SUBSCRIPTION)
                : (" AND NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ")");
        String reconType = subscriptionScope ? "SUBSCRIPTION" : "CASH";
        String scheduleLateral = subscriptionScope
                ? """
                 LEFT JOIN LATERAL (
                    SELECT sbs.billing_schedule_id,
                           COALESCE(sbs.label, sbs.period_label) AS schedule_label
                    FROM client_subscription_billing.subscription_billing_schedule sbs
                    WHERE sbs.invoice_id = f.invoice_id
                    ORDER BY sbs.billing_schedule_id
                    LIMIT 1
                ) sch ON true
                """
                : "";
        String scheduleSelect = subscriptionScope
                ? """
                    CAST(sch.billing_schedule_id AS text) AS "billingScheduleId",
                    sch.schedule_label AS "scheduleLabel",
                """
                : """
                    NULL::text AS "billingScheduleId",
                    NULL::text AS "scheduleLabel",
                """;
        args.add(maxRows);
        return jdbc.queryForList("""
                WITH invoice_facts AS (
                    SELECT
                        i.invoice_id,
                        i.invoice_number,
                        MAX(i.total_amount) AS expected_amount,
                        COALESCE(SUM(cpt.amount), 0)::numeric / 100.0 AS captured_amount
                    FROM transactions.invoice i
                    LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                    LEFT JOIN client_payments.client_payment_transaction cpt
                        ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + scope + """
                    GROUP BY i.invoice_id, i.invoice_number
                )
                SELECT
                """ + scheduleSelect + """
                    CAST(f.invoice_id AS text) AS "invoiceId",
                    f.invoice_number AS "invoiceNumber",
                    f.expected_amount AS "expectedAmount",
                    f.captured_amount AS "actualAmount",
                    (f.expected_amount - f.captured_amount) AS "varianceAmount",
                    CASE WHEN f.expected_amount = 0 THEN 0
                         ELSE ABS(f.expected_amount - f.captured_amount) * 100.0 / f.expected_amount END AS "variancePct",
                    'AMOUNT_VARIANCE' AS "reasonCode",
                    'Invoice amount vs captured payment' AS "reasonText",
                    'UNRECONCILED' AS "reconciliationStatus",
                    false AS "paid",
                    '""" + reconType + "' AS \"reconciliationType\"\n" + """
                FROM invoice_facts f
                """ + scheduleLateral + """
                WHERE ABS(f.expected_amount - f.captured_amount) > 0.01
                ORDER BY ABS(f.expected_amount - f.captured_amount) DESC
                LIMIT ?
                """, args.toArray());
    }

    /**
     * Mismatch rows materialized on the run (same source as {@code GET /dashboard/mismatches}).
     */
    private List<Map<String, Object>> listUnreconciledItemsFromRun(String recoRunId, String reconciliationType, int limit) {
        if (recoRunId == null || recoRunId.isBlank()) {
            return List.of();
        }
        int maxRows = Math.max(1, Math.min(limit, 100));
        List<Object> args = new ArrayList<>();
        args.add(recoRunId.trim());
        StringBuilder extra = new StringBuilder();
        if (reconciliationType != null && !reconciliationType.isBlank()) {
            String u = reconciliationType.trim().toUpperCase();
            if ("SUBSCRIPTION".equals(u)) {
                extra.append("""
                         AND (
                            s.attributes_json->>'reconciliationType' IN ('SUBSCRIPTION')
                            OR (s.attributes_json->>'reconciliationType') IS NULL
                        )
                        """);
            } else if ("CASH".equals(u)) {
                extra.append(" AND s.attributes_json->>'reconciliationType' = 'CASH' ");
            }
        }
        args.add(maxRows);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    s.snapshot_reference_code AS "snapshotId",
                    COALESCE(s.attributes_json->>'reconciliationType', 'SUBSCRIPTION') AS "reconciliationType",
                    CAST(s.entity_id AS text) AS "invoiceId",
                    COALESCE(s.entity_reference_code, CAST(s.entity_id AS text)) AS "invoiceNumber",
                    COALESCE(s.expected_amount, 0) AS "expectedAmount",
                    COALESCE(s.actual_amount, 0) AS "actualAmount",
                    (COALESCE(s.expected_amount, 0) - COALESCE(s.actual_amount, 0)) AS "varianceAmount",
                    CASE WHEN COALESCE(s.expected_amount, 0) = 0 THEN 0
                         ELSE ABS(COALESCE(s.expected_amount, 0) - COALESCE(s.actual_amount, 0))
                              * 100.0 / COALESCE(s.expected_amount, 1) END AS "variancePct",
                    COALESCE(lr.code, 'AMOUNT_VARIANCE') AS "reasonCode",
                    COALESCE(s.reason_text, 'Invoice amount vs captured payment') AS "reasonText",
                    rcs.code AS "reconciliationStatus",
                    false AS "paid",
                    NULL::text AS "billingScheduleId",
                    NULL::text AS "scheduleLabel"
                FROM reconciliation.reconciliation_run_snapshot s
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                LEFT JOIN reconciliation.lu_reconciliation_reason lr ON lr.reconciliation_reason_id = s.reconciliation_reason_id
                WHERE rr.run_reference_code = ?
                  AND rcs.code <> 'RECONCILED'
                """ + extra + """
                ORDER BY ABS(COALESCE(s.expected_amount, 0) - COALESCE(s.actual_amount, 0)) DESC
                LIMIT ?
                """, args.toArray());
        return rows;
    }

    @SuppressWarnings("unchecked")
    private void patchUnreconciledItemsOnDashboard(String recoRunId, Map<String, Object> parsed) {
        Object scope = parsed.get("reconciliationScope");
        if (!(scope instanceof Map<?, ?> scopeMap)) {
            return;
        }
        int limit = 25;
        Object meta = parsed.get("meta");
        if (meta instanceof Map<?, ?> metaMap && metaMap.get("varianceDriverLimit") != null) {
            limit = parsePositiveInt(metaMap.get("varianceDriverLimit"), 25);
        }
        List<Map<String, Object>> subItems = listUnreconciledItemsFromRun(recoRunId, "SUBSCRIPTION", limit);
        List<Map<String, Object>> cashItems = listUnreconciledItemsFromRun(recoRunId, "CASH", limit);
        patchScopeUnreconciledItems((Map<String, Object>) scopeMap.get("subscription"), subItems);
        patchScopeUnreconciledItems((Map<String, Object>) scopeMap.get("cash"), cashItems);
        refreshVarianceDriversWithItems(parsed, subItems, cashItems);
    }

    @SuppressWarnings("unchecked")
    private void patchScopeUnreconciledItems(Map<String, Object> scopePart, List<Map<String, Object>> items) {
        if (scopePart == null) {
            return;
        }
        scopePart.put("unreconciledItems", items);
        Object kpis = scopePart.get("kpis");
        if (kpis instanceof Map<?, ?> kpisMap) {
            int total = ((Number) ((Map<String, Object>) kpisMap).getOrDefault("totalInvoices", 0)).intValue();
            int unreconciled = ((Number) ((Map<String, Object>) kpisMap).getOrDefault("unreconciledCount", 0)).intValue();
            scopePart.put("paidSummary", Map.of(
                    "totalUnits", total,
                    "reconciledUnits", Math.max(0, total - unreconciled),
                    "unreconciledUnits", unreconciled,
                    "paidPct", total == 0 ? 0.0 : Math.max(0, total - unreconciled) * 100.0 / total
            ));
        }
        if (scopePart.get("chainStages") instanceof List<?>) {
            scopePart.put("chainStages", attachUnreconciledItemsToStages(
                    (List<Map<String, Object>>) scopePart.get("chainStages"), items));
        }
    }

    @SuppressWarnings("unchecked")
    private void refreshVarianceDriversWithItems(
            Map<String, Object> parsed,
            List<Map<String, Object>> subItems,
            List<Map<String, Object>> cashItems
    ) {
        List<Map<String, Object>> all = new ArrayList<>();
        all.addAll(subItems);
        all.addAll(cashItems);
        if (all.isEmpty()) {
            return;
        }
        Object drivers = parsed.get("varianceDrivers");
        if (!(drivers instanceof List<?> driverList) || driverList.isEmpty()) {
            parsed.put("varianceDrivers", List.of(Map.of(
                    "driver", "Amount mismatch",
                    "count", all.size(),
                    "amount", all.stream()
                            .mapToDouble(r -> Math.abs(((Number) r.getOrDefault("varianceAmount", 0)).doubleValue()))
                            .sum(),
                    "items", all
            )));
            return;
        }
        Object first = driverList.get(0);
        if (!(first instanceof Map<?, ?> driverMap)) {
            return;
        }
        Map<String, Object> driver = new LinkedHashMap<>((Map<String, Object>) driverMap);
        driver.put("items", all);
        parsed.put("varianceDrivers", List.of(driver));
    }

    private List<Map<String, Object>> attachUnreconciledItemsToStages(
            List<Map<String, Object>> stages,
            List<Map<String, Object>> unreconciledItems
    ) {
        if (unreconciledItems == null || unreconciledItems.isEmpty()) {
            return stages;
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> stage : stages) {
            String stageKey = String.valueOf(stage.get("stageKey"));
            if ("INVOICE".equals(stageKey)
                    || "BILLING_SCHEDULE".equals(stageKey)
                    || "PAYMENT_TRANSACTION".equals(stageKey)) {
                Map<String, Object> copy = new LinkedHashMap<>(stage);
                copy.put("unreconciledItems", unreconciledItems);
                copy.put("unreconciledCount", unreconciledItems.size());
                Object drill = copy.get("drillDown");
                Map<String, Object> drillDown = drill instanceof Map<?, ?> m
                        ? new LinkedHashMap<>((Map<String, Object>) m)
                        : new LinkedHashMap<>();
                drillDown.putIfAbsent("route", "DASHBOARD_MISMATCHES");
                copy.put("drillDown", drillDown);
                out.add(copy);
            } else {
                out.add(stage);
            }
        }
        return out;
    }

    private List<Map<String, Object>> buildStatusBreakdown(int reconciledCount, int mismatchCount) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("status", "RECONCILED", "count", reconciledCount));
        if (mismatchCount > 0) {
            rows.add(Map.of("status", "UNRECONCILED", "count", mismatchCount));
        }
        return rows;
    }

    private static int scopeInvoiceMismatch(Map<String, Object> scopeKpis) {
        return ((Number) scopeKpis.getOrDefault("unreconciledCount", 0)).intValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> scopeGlMetrics(Map<String, Object> glPack, String scopeKey) {
        Object scoped = glPack.get(scopeKey);
        if (scoped instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        Object run = glPack.get("run");
        if (run instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        return glPack;
    }

    private Map<String, Object> computeScopeInvoicePaymentAgg(Map<String, Object> filters, boolean subscription) {
        return computeScopeInvoicePaymentAgg(filters, Boolean.valueOf(subscription));
    }

    /**
     * Per-invoice aggregates (avoids double-counting invoice amounts when multiple payment rows exist).
     */
    private Map<String, Object> computeScopeInvoicePaymentAgg(Map<String, Object> filters, Boolean subscriptionScope) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String scope = "";
        if (subscriptionScope != null) {
            scope = subscriptionScope
                    ? (" AND " + SQL_INVOICE_IS_SUBSCRIPTION)
                    : (" AND NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ")");
        }
        String invoiceFactsCte = """
                WITH invoice_facts AS (
                    SELECT
                        i.invoice_id,
                        MAX(i.total_amount) AS expected_amount,
                        COALESCE(SUM(cpt.amount), 0)::numeric / 100.0 AS captured_amount
                    FROM transactions.invoice i
                    LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                    LEFT JOIN client_payments.client_payment_transaction cpt
                        ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + scope + """
                    GROUP BY i.invoice_id
                )
                """;
        Map<String, Object> facts = jdbc.queryForMap(invoiceFactsCte + """
                SELECT
                    COUNT(*)::int AS total_invoices,
                    COALESCE(SUM(expected_amount), 0) AS total_expected_amount,
                    COALESCE(SUM(captured_amount), 0) AS total_actual_amount,
                    COUNT(*) FILTER (
                        WHERE ABS(expected_amount - captured_amount) > 0.01
                    )::int AS mismatch_count
                FROM invoice_facts
                """, args.toArray());
        Map<String, Object> paymentRows = jdbc.queryForMap("""
                SELECT
                    COUNT(DISTINCT cpi.client_payment_intent_id) AS intent_rows,
                    COUNT(DISTINCT cpt.client_payment_transaction_id) AS txn_rows
                FROM transactions.invoice i
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt
                    ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + scope, args.toArray());
        Map<String, Object> out = new HashMap<>();
        out.putAll(facts);
        out.putAll(paymentRows);
        return out;
    }

    /**
     * Per {@code subscription_billing_schedule} totals for the billing-schedule pipeline tile.
     */
    private Map<String, Object> computeBillingScheduleAgg(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String sql = """
                WITH schedule_facts AS (
                    SELECT
                        sbs.billing_schedule_id,
                        MAX(i.total_amount) AS expected_amount,
                        COALESCE(SUM(cpt.amount), 0)::numeric / 100.0 AS captured_amount
                    FROM client_subscription_billing.subscription_billing_schedule sbs
                    JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
                    LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                    LEFT JOIN client_payments.client_payment_transaction cpt
                        ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + """
                    GROUP BY sbs.billing_schedule_id
                )
                SELECT
                    COUNT(*)::int AS schedule_count,
                    COALESCE(SUM(expected_amount), 0) AS total_expected_amount,
                    COALESCE(SUM(captured_amount), 0) AS total_actual_amount,
                    COUNT(*) FILTER (
                        WHERE ABS(expected_amount - captured_amount) <= 0.01
                    )::int AS reconciled_schedule_count
                FROM schedule_facts
                """;
        return jdbc.queryForMap(sql, args.toArray());
    }

    private Map<String, Object> buildKpiBlock(Map<String, Object> agg, String scopeHint) {
        int total = ((Number) agg.getOrDefault("total_invoices", 0)).intValue();
        double expected = ((Number) agg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actual = ((Number) agg.getOrDefault("total_actual_amount", 0)).doubleValue();
        int mismatch = ((Number) agg.getOrDefault("mismatch_count", 0)).intValue();
        double variance = expected - actual;
        int reconciled = Math.max(0, total - mismatch);
        Map<String, Object> m = new HashMap<>();
        m.put("totalInvoices", total);
        m.put("totalExpectedAmount", expected);
        m.put("totalActualAmount", actual);
        m.put("totalVarianceAmount", variance);
        m.put("variancePct", expected == 0 ? 0.0 : Math.abs(variance) * 100.0 / expected);
        m.put("unreconciledCount", mismatch);
        m.put("partialCount", mismatch);
        m.put("reconciledCount", reconciled);
        m.put("criticalExceptions", 0);
        if ("CASH".equals(scopeHint) && total == 0) {
            m.put("status", "NODATA");
        } else if (mismatch == 0) {
            m.put("status", "OK");
        } else {
            m.put("status", "VARIANCE");
        }
        return m;
    }

    private List<Map<String, Object>> buildSubscriptionChainStages(Map<String, Object> agg, Map<String, Object> scheduleAgg) {
        int total = ((Number) agg.getOrDefault("total_invoices", 0)).intValue();
        int mismatch = ((Number) agg.getOrDefault("mismatch_count", 0)).intValue();
        double expected = ((Number) agg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actual = ((Number) agg.getOrDefault("total_actual_amount", 0)).doubleValue();
        int intents = ((Number) agg.getOrDefault("intent_rows", 0)).intValue();
        int txns = ((Number) agg.getOrDefault("txn_rows", 0)).intValue();
        double paidPct = reconciliationPaidPct(total, mismatch);
        int scheduleCount = toInt(scheduleAgg.get("schedule_count"));
        int reconciledSchedules = toInt(scheduleAgg.get("reconciled_schedule_count"));
        int scheduleMismatch = Math.max(0, scheduleCount - reconciledSchedules);
        double scheduleExpected = ((Number) scheduleAgg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double scheduleActual = ((Number) scheduleAgg.getOrDefault("total_actual_amount", 0)).doubleValue();
        double schedulePaidPct = reconciliationPaidPct(scheduleCount, scheduleMismatch);
        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(chainStage("BILLING_SCHEDULE", 1, "Billing Schedule", scheduleCount, scheduleMismatch,
                scheduleExpected, scheduleActual, schedulePaidPct));
        stages.add(chainStage("INVOICE", 2, "Invoice", total, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_INTENT", 3, "Payment Intent", intents, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_TRANSACTION", 4, "Payment Transaction", txns, mismatch, expected, actual, paidPct));
        stages.add(chainStage("GATEWAY", 5, "Gateway", txns, mismatch, expected, actual, paidPct));
        stages.add(chainStage("SETTLEMENT_BANK", 6, "Settlement (Bank)", txns, mismatch, expected, actual, paidPct));
        stages.add(chainStage("REFUND", 7, "Refunds", 0, 0, 0, 0, 100.0));
        stages.add(chainStage("GL_POSTING", 8, "GL Posting", total, 0, expected, actual, paidPct));
        return stages;
    }

    private List<Map<String, Object>> buildCashChainStages(Map<String, Object> agg) {
        int total = ((Number) agg.getOrDefault("total_invoices", 0)).intValue();
        int mismatch = ((Number) agg.getOrDefault("mismatch_count", 0)).intValue();
        double expected = ((Number) agg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actual = ((Number) agg.getOrDefault("total_actual_amount", 0)).doubleValue();
        int intents = ((Number) agg.getOrDefault("intent_rows", 0)).intValue();
        int txns = ((Number) agg.getOrDefault("txn_rows", 0)).intValue();
        double paidPct = reconciliationPaidPct(total, mismatch);
        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(chainStage("INVOICE", 1, "Invoice", total, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_INTENT", 2, "Payment Intent", intents, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_TRANSACTION", 3, "Payment Transaction", txns, mismatch, expected, actual, paidPct));
        stages.add(chainStage("REFUND", 4, "Refund", 0, 0, 0, 0, 100.0));
        stages.add(chainStage("GL_POSTING", 5, "GL Posting", total, 0, expected, actual, paidPct));
        return stages;
    }

    private Map<String, Object> chainStage(
            String stageKey,
            int order,
            String title,
            long countMetric,
            int mismatch,
            double expected,
            double actual,
            double paidPct
    ) {
        double mismatchPct = countMetric == 0 ? 0.0 : mismatch * 100.0 / countMetric;
        String status;
        if (mismatch == 0) {
            status = "MATCHED";
        } else if (mismatch >= countMetric && countMetric > 0) {
            status = "MISMATCH";
        } else {
            status = "PARTIAL";
        }
        String emphasis = mismatchPct > 10 ? "DANGER" : (mismatch > 0 ? "WARNING" : "NEUTRAL");
        Map<String, Object> tile = new HashMap<>();
        tile.put("stageKey", stageKey);
        tile.put("order", order);
        tile.put("title", title);
        tile.put("status", status);
        tile.put("statusDisplay", humanizeStatus(status));
        tile.put("mismatch", mismatch);
        tile.put("mismatchPct", mismatchPct);
        List<Map<String, Object>> metrics = new ArrayList<>();
        metrics.add(metricRow("Volume", countMetric, countMetric, emphasis));
        metrics.add(metricRow("Expected", expected, expected, "NEUTRAL"));
        String amountEmphasis = Math.abs(expected - actual) > 0.01 ? "WARNING" : "NEUTRAL";
        metrics.add(metricRow("Actual", actual, actual, amountEmphasis));
        metrics.add(metricRow("Paid %", paidPct, paidPct, emphasis));
        tile.put("expectedAmount", expected);
        tile.put("actualAmount", actual);
        tile.put("varianceAmount", expected - actual);
        tile.put("metrics", metrics);
        tile.put("drillDown", Map.of("route", "WORKSPACE_LIST", "filters", Map.of("stage", stageKey)));
        return tile;
    }

    /**
     * Share of units (invoices, schedules, payment txns) whose expected amount matches captured payment within {@code 0.01}.
     */
    private static double reconciliationPaidPct(int unitCount, int mismatchCount) {
        if (unitCount <= 0) {
            return 0.0;
        }
        return Math.max(0.0, (unitCount - mismatchCount) * 100.0 / unitCount);
    }

    private static List<Map<String, Object>> settlementBankMatchesFromIntents(List<Map<String, Object>> intentPayloads) {
        List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> intentPayload : intentPayloads) {
            Object txns = intentPayload.get("paymentTransactions");
            if (!(txns instanceof List<?> txnList)) {
                continue;
            }
            for (Object txnObj : txnList) {
                if (!(txnObj instanceof Map<?, ?> txnMap)) {
                    continue;
                }
                Object bankMatches = txnMap.get("settlementBankMatches");
                if (!(bankMatches instanceof List<?> bankList)) {
                    continue;
                }
                for (Object row : bankList) {
                    if (row instanceof Map<?, ?> m) {
                        matches.add((Map<String, Object>) m);
                    }
                }
            }
        }
        return matches;
    }

    private Map<String, Object> buildInvoiceAmountReconciliation(
            Map<String, Object> invoice,
            List<Map<String, Object>> intentPayloads
    ) {
        double expected = invoice.get("totalAmount") == null
                ? 0.0
                : ((Number) invoice.get("totalAmount")).doubleValue();
        double captured = 0.0;
        for (Map<String, Object> intentPayload : intentPayloads) {
            Object txns = intentPayload.get("paymentTransactions");
            if (!(txns instanceof List<?> txnList)) {
                continue;
            }
            for (Object txnObj : txnList) {
                if (!(txnObj instanceof Map<?, ?> txnMap)) {
                    continue;
                }
                Object amt = txnMap.get("amount");
                if (amt instanceof Number n) {
                    captured += n.doubleValue() / 100.0;
                }
            }
        }
        double variance = expected - captured;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("expectedAmount", expected);
        out.put("actualAmount", captured);
        out.put("varianceAmount", variance);
        out.put("reconciled", Math.abs(variance) <= 0.01);
        return out;
    }

    private static int toInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private Map<String, Object> metricRow(String label, double displayValue, double valueRaw, String emphasis) {
        Map<String, Object> m = new HashMap<>();
        m.put("label", label);
        m.put("value", formatMetricValue(displayValue));
        m.put("valueRaw", valueRaw);
        m.put("emphasis", emphasis);
        return m;
    }

    private static String formatMetricValue(double v) {
        if (Math.abs(v - Math.rint(v)) < 0.0001) {
            return String.format("%,.0f", v);
        }
        return String.format("%,.2f", v);
    }

    private Map<String, Object> buildVarianceByReconciliationType(
            Map<String, Object> subAgg,
            Map<String, Object> cashAgg,
            int driverLimit,
            boolean includeChart
    ) {
        double expS = ((Number) subAgg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actS = ((Number) subAgg.getOrDefault("total_actual_amount", 0)).doubleValue();
        double expC = ((Number) cashAgg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actC = ((Number) cashAgg.getOrDefault("total_actual_amount", 0)).doubleValue();
        double vS = expS - actS;
        double vC = expC - actC;
        double denom = Math.abs(vS) + Math.abs(vC);
        double shareS = denom <= 0 ? 0.0 : Math.abs(vS) * 100.0 / denom;
        double shareC = denom <= 0 ? 0.0 : Math.abs(vC) * 100.0 / denom;
        double pctS = expS == 0 ? 0.0 : Math.abs(vS) * 100.0 / expS;
        double pctC = expC == 0 ? 0.0 : Math.abs(vC) * 100.0 / expC;

        List<Map<String, Object>> driversSub = topVarianceDrivers("SUBSCRIPTION", vS, driverLimit);
        List<Map<String, Object>> driversCash = topVarianceDrivers("CASH", vC, driverLimit);

        Map<String, Object> rowS = new HashMap<>();
        rowS.put("reconciliationType", "SUBSCRIPTION");
        rowS.put("label", "Subscription");
        rowS.put("varianceAmount", vS);
        rowS.put("variancePct", pctS);
        rowS.put("shareOfTotalPct", shareS);
        rowS.put("drivers", driversSub);

        Map<String, Object> rowC = new HashMap<>();
        rowC.put("reconciliationType", "CASH");
        rowC.put("label", "Cash / POS");
        rowC.put("varianceAmount", vC);
        rowC.put("variancePct", pctC);
        rowC.put("shareOfTotalPct", shareC);
        rowC.put("drivers", driversCash);

        Map<String, Object> out = new HashMap<>();
        out.put("title", "Variance by Reconciliation Type");
        out.put("rows", List.of(rowS, rowC));
        if (includeChart) {
            out.put("chart", Map.of(
                    "kind", "BAR_HORIZONTAL",
                    "series", List.of(
                            Map.of("key", "SUBSCRIPTION", "label", "Subscription", "value", Math.abs(vS)),
                            Map.of("key", "CASH", "label", "Cash", "value", Math.abs(vC))
                    )
            ));
        }
        return out;
    }

    @SuppressWarnings("unused")
    private List<Map<String, Object>> topVarianceDrivers(String reconType, double varianceAmount, int limit) {
        if (Math.abs(varianceAmount) < 0.0001) {
            return List.of();
        }
        return List.of(Map.of(
                "driver", "Amount mismatch",
                "amount", Math.abs(varianceAmount),
                "reconciliationType", reconType
        ));
    }

    private static String humanizeStatus(String status) {
        if (status == null || status.isEmpty()) {
            return "";
        }
        return switch (status) {
            case "MATCHED" -> "Matched";
            case "PARTIAL" -> "Partial";
            case "MISMATCH" -> "Mismatch";
            default -> status.charAt(0) + status.substring(1).toLowerCase();
        };
    }

    private static int parsePositiveInt(Object o, int def) {
        if (o == null) {
            return def;
        }
        try {
            int v = Integer.parseInt(String.valueOf(o).trim());
            return v <= 0 ? def : v;
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static boolean parseBooleanDefault(Object o, boolean def) {
        if (o == null) {
            return def;
        }
        if (o instanceof Boolean b) {
            return b;
        }
        return Boolean.parseBoolean(String.valueOf(o).trim());
    }

    private Map<String, Object> loadDashboardSummaryFromRun(String recoRunId, Map<String, Object> filters) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT summary_json::text AS j FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                """, recoRunId);
        if (rows.isEmpty()) {
            return emptyFinancialHealthSummary("RUN_NOT_FOUND");
        }
        String json = (String) rows.get(0).get("j");
        if (json == null || json.isBlank() || "{}".equals(json.trim())) {
            return emptyFinancialHealthSummary("RUN_SUMMARY_EMPTY");
        }
        try {
            Map<String, Object> parsed = objectMapper.readValue(json, new TypeReference<>() {
            });
            parsed.putIfAbsent("hint", "OK");
            enrichDashboardLayoutV2FromRunIfMissing(recoRunId, parsed);
            patchSubscriptionPipelineStagesFromRun(recoRunId, parsed);
            patchUnreconciledItemsOnDashboard(recoRunId, parsed);
            patchGlPostingChainStagesInSummary(parsed, computeGlPostingMetricsPackForRun(recoRunId));
            return parsed;
        } catch (JsonProcessingException e) {
            return emptyFinancialHealthSummary("RUN_SUMMARY_INVALID");
        }
    }

    /**
     * Refreshes subscription pipeline tiles (billing schedule + payment stages) from run filters so
     * persisted snapshots are not stuck with legacy placeholders (e.g. billing schedule expected = 0).
     */
    @SuppressWarnings("unchecked")
    private void patchSubscriptionPipelineStagesFromRun(String recoRunId, Map<String, Object> parsed) {
        Object scope = parsed.get("reconciliationScope");
        if (!(scope instanceof Map<?, ?> scopeMap)) {
            return;
        }
        Object sub = scopeMap.get("subscription");
        if (!(sub instanceof Map<?, ?> subMap)) {
            return;
        }
        try {
            String fj = jdbc.queryForObject("""
                    SELECT filters_json::text FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                    """, String.class, recoRunId);
            Map<String, Object> filterMap = parseRunFiltersJson(fj);
            Map<String, Object> subAgg = computeScopeInvoicePaymentAgg(filterMap, true);
            Map<String, Object> scheduleAgg = computeBillingScheduleAgg(filterMap);
            List<Map<String, Object>> rebuilt = buildSubscriptionChainStages(subAgg, scheduleAgg);
            Map<String, Object> glPack = computeGlPostingMetricsPackForRun(recoRunId);
            Map<String, Object> subScope = (Map<String, Object>) subMap;
            subScope.put("chainStages", withGlPostingStage(
                    rebuilt,
                    8,
                    enrichScopeGlWithInvoiceMismatch(scopeGlMetrics(glPack, "subscription"), subScope),
                    scopeInvoiceMismatch(kpisFromScope(subScope))));
        } catch (Exception ignored) {
            // keep snapshot stages
        }
    }

    private void enrichDashboardLayoutV2FromRunIfMissing(String recoRunId, Map<String, Object> parsed) {
        if (parsed.containsKey("reconciliationScope")) {
            return;
        }
        try {
            String fj = jdbc.queryForObject("""
                    SELECT filters_json::text FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                    """, String.class, recoRunId);
            if (fj == null || fj.isBlank() || "{}".equals(fj.trim())) {
                return;
            }
            Map<String, Object> filterMap = objectMapper.readValue(fj, new TypeReference<>() {
            });
            Map<String, Object> live = computeDashboardSummaryLive(filterMap);
            parsed.put("reconciliationScope", live.get("reconciliationScope"));
            parsed.put("varianceByReconciliationType", live.get("varianceByReconciliationType"));
            parsed.put("meta", live.get("meta"));
            patchGlPostingChainStagesInSummary(parsed, computeGlPostingMetricsPackForFilters(filterMap));
        } catch (Exception ignored) {
            // leave legacy payload if recompute fails
        }
    }

    /**
     * After a run completes, attach payment-posted {@code gl_entry} rows that share the run's financial period
     * and level but were inserted with {@code reconciliation_run_id} null by an external posting process.
     */
    public int linkGlEntriesToRun(String runReferenceCode) {
        if (runReferenceCode == null || runReferenceCode.isBlank()) {
            return 0;
        }
        return jdbc.update("""
                UPDATE reconciliation.gl_entry ge
                SET reconciliation_run_id = rr.reconciliation_run_id,
                    modified_on = now()
                FROM reconciliation.reconciliation_run rr
                WHERE rr.run_reference_code = ?
                  AND ge.reconciliation_run_id IS NULL
                  AND ge.financial_period_id IS NOT DISTINCT FROM rr.financial_period_id
                  AND (
                        ge.level_id IS NULL
                        OR rr.level_id IS NULL
                        OR ge.level_id IS NOT DISTINCT FROM rr.level_id
                  )
                """, runReferenceCode.trim());
    }

    private UUID resolveLevelIdFromFilters(Map<String, Object> filterMap) {
        if (filterMap == null) {
            return null;
        }
        UUID id = parseOptionalUuid(asString(filterMap.get("locationLevelId")));
        if (id == null) {
            id = parseOptionalUuid(asString(filterMap.get("location_level_id")));
        }
        return id;
    }

    public Map<String, Object> computeGlPostingMetricsForRun(String runReferenceCode) {
        if (runReferenceCode == null || runReferenceCode.isBlank()) {
            return emptyGlPostingMetrics();
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rr.reconciliation_run_id,
                    rr.financial_period_id,
                    rr.level_id,
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS period_code
                FROM reconciliation.reconciliation_run rr
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                WHERE rr.run_reference_code = ?
                """, runReferenceCode.trim());
        if (rows.isEmpty()) {
            return emptyGlPostingMetrics();
        }
        Map<String, Object> r = rows.get(0);
        return computeGlPostingMetrics(
                (UUID) r.get("reconciliation_run_id"),
                (UUID) r.get("financial_period_id"),
                (UUID) r.get("level_id"),
                r.get("period_code") == null ? null : String.valueOf(r.get("period_code")),
                null);
    }

    private Map<String, Object> computeGlPostingMetricsForFilters(Map<String, Object> filters) {
        String periodCode = asString(filters.get("financialPeriod"));
        UUID financialPeriodId = resolveOrEnsureFinancialPeriodId(periodCode);
        return computeGlPostingMetrics(
                null,
                financialPeriodId,
                resolveLevelIdFromFilters(filters),
                periodCode,
                null);
    }

    private Map<String, Object> emptyGlPostingMetrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("glLineCount", 0L);
        m.put("glPairCount", 0L);
        m.put("missingMappingLines", 0);
        m.put("unbalancedPairCount", 0);
        m.put("linkedToRunCount", 0L);
        m.put("paymentTxnCount", 0L);
        m.put("missingGlPairs", 0);
        m.put("glCoveragePct", 0.0);
        return m;
    }

    public Map<String, Object> computeGlPostingMetricsPackForRun(String runReferenceCode) {
        if (runReferenceCode == null || runReferenceCode.isBlank()) {
            return emptyGlPostingPack();
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rr.reconciliation_run_id,
                    rr.financial_period_id,
                    rr.level_id,
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS period_code,
                    rr.filters_json::text AS filters_json_text
                FROM reconciliation.reconciliation_run rr
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                WHERE rr.run_reference_code = ?
                """, runReferenceCode.trim());
        if (rows.isEmpty()) {
            return emptyGlPostingPack();
        }
        Map<String, Object> row = rows.get(0);
        Map<String, Object> filterMap = parseRunFiltersJson(row.get("filters_json_text"));
        return buildGlPostingMetricsPack(
                (UUID) row.get("reconciliation_run_id"),
                (UUID) row.get("financial_period_id"),
                (UUID) row.get("level_id"),
                row.get("period_code") == null ? null : String.valueOf(row.get("period_code")),
                filterMap);
    }

    private Map<String, Object> computeGlPostingMetricsPackForFilters(Map<String, Object> filters) {
        String periodCode = asString(filters.get("financialPeriod"));
        UUID financialPeriodId = resolveOrEnsureFinancialPeriodId(periodCode);
        return buildGlPostingMetricsPack(
                null,
                financialPeriodId,
                resolveLevelIdFromFilters(filters),
                periodCode,
                filters);
    }

    private Map<String, Object> emptyGlPostingPack() {
        Map<String, Object> empty = emptyGlPostingMetrics();
        Map<String, Object> pack = new LinkedHashMap<>();
        pack.put("run", new LinkedHashMap<>(empty));
        pack.put("subscription", new LinkedHashMap<>(empty));
        pack.put("cash", new LinkedHashMap<>(empty));
        return pack;
    }

    private Map<String, Object> buildGlPostingMetricsPack(
            UUID reconciliationRunId,
            UUID financialPeriodId,
            UUID levelId,
            String periodCode,
            Map<String, Object> filters
    ) {
        Map<String, Object> runMetrics = enrichGlPostingMetrics(
                computeGlPostingMetrics(reconciliationRunId, financialPeriodId, levelId, periodCode, null),
                countPaymentTransactionsInScope(filters, null),
                0);
        Map<String, Object> subMetrics = enrichGlPostingMetrics(
                computeGlPostingMetrics(reconciliationRunId, financialPeriodId, levelId, periodCode, true),
                countPaymentTransactionsInScope(filters, true),
                0);
        Map<String, Object> cashMetrics = enrichGlPostingMetrics(
                computeGlPostingMetrics(reconciliationRunId, financialPeriodId, levelId, periodCode, false),
                countPaymentTransactionsInScope(filters, false),
                0);
        Map<String, Object> pack = new LinkedHashMap<>();
        pack.put("run", runMetrics);
        pack.put("subscription", subMetrics);
        pack.put("cash", cashMetrics);
        return pack;
    }

    private Map<String, Object> enrichGlPostingMetrics(
            Map<String, Object> base,
            long paymentTxnCount,
            int invoiceAmountMismatchCount
    ) {
        Map<String, Object> out = new LinkedHashMap<>(base);
        long pairs = ((Number) out.getOrDefault("glPairCount", 0L)).longValue();
        long missingGlPairs = Math.max(0L, paymentTxnCount - pairs);
        out.put("paymentTxnCount", paymentTxnCount);
        out.put("missingGlPairs", missingGlPairs);
        out.put("glCoveragePct", paymentTxnCount == 0
                ? (pairs > 0 ? 100.0 : 0.0)
                : Math.min(100.0, pairs * 100.0 / paymentTxnCount));
        out.put("invoiceAmountMismatchCount", invoiceAmountMismatchCount);
        return out;
    }

    private long countPaymentTransactionsInScope(Map<String, Object> filters, Boolean subscriptionScope) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String scope = "";
        if (subscriptionScope != null) {
            scope = subscriptionScope
                    ? (" AND " + SQL_INVOICE_IS_SUBSCRIPTION)
                    : (" AND NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ")");
        }
        Long n = jdbc.queryForObject("""
                SELECT COUNT(DISTINCT cpt.client_payment_transaction_id)
                FROM transactions.invoice i
                JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                JOIN client_payments.client_payment_transaction cpt
                    ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + scope, Long.class, args.toArray());
        return n == null ? 0L : n;
    }

    private Map<String, Object> computeGlPostingMetrics(
            UUID reconciliationRunId,
            UUID financialPeriodId,
            UUID levelId,
            String periodCode,
            Boolean subscriptionScope
    ) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");
        appendGlEntryScopeWhere(where, args, reconciliationRunId, financialPeriodId, levelId, periodCode);
        if (subscriptionScope != null) {
            appendGlEntryInvoiceClassificationWhere(where, subscriptionScope);
        }

        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT
                    COUNT(*)::bigint AS gl_line_count,
                    COUNT(*) FILTER (WHERE ge.gl_mapping_rule_id IS NULL)::int AS missing_mapping_lines,
                    COUNT(DISTINCT regexp_replace(ge.gl_entry_reference_code, '-(DR|CR)$', ''))::bigint AS gl_pair_count,
                    COUNT(*) FILTER (WHERE ge.reconciliation_run_id IS NOT NULL)::bigint AS linked_to_run_count
                FROM reconciliation.gl_entry ge
                """ + where, args.toArray());

        List<Object> ubArgs = new ArrayList<>(args);
        Integer unbalanced = jdbc.queryForObject("""
                SELECT COUNT(*)::int FROM (
                    SELECT regexp_replace(ge.gl_entry_reference_code, '-(DR|CR)$', '') AS pair_key
                    FROM reconciliation.gl_entry ge
                """ + where + """
                    GROUP BY pair_key
                    HAVING ABS(COALESCE(SUM(ge.debit_amount), 0) - COALESCE(SUM(ge.credit_amount), 0)) > 0.01
                ) unbalanced_pairs
                """, Integer.class, ubArgs.toArray());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("glLineCount", counts.getOrDefault("gl_line_count", 0L));
        out.put("glPairCount", counts.getOrDefault("gl_pair_count", 0L));
        out.put("missingMappingLines", counts.getOrDefault("missing_mapping_lines", 0));
        out.put("unbalancedPairCount", unbalanced == null ? 0 : unbalanced);
        out.put("linkedToRunCount", counts.getOrDefault("linked_to_run_count", 0L));
        return out;
    }

    /**
     * Restrict {@code gl_entry} rows to those tied to invoices in the subscription or cash scope.
     */
    private void appendGlEntryInvoiceClassificationWhere(StringBuilder sql, boolean subscription) {
        String subExists = """
                 AND EXISTS (
                    SELECT 1
                    FROM client_payments.client_payment_transaction cpt
                    JOIN client_payments.client_payment_intent cpi
                        ON cpi.client_payment_intent_id = cpt.client_payment_intent_id
                    JOIN transactions.invoice i ON i.invoice_id = cpi.invoice_id
                    WHERE (
                        ge.entity_id = i.invoice_id
                        OR ge.entity_reference_code = i.invoice_number
                        OR ge.entity_reference_code = cpt.client_payment_transaction_id::text
                        OR ge.entity_reference_code LIKE 'PAY-CPT-' || cpt.client_payment_transaction_id::text || '-%%'
                    )
                    AND %s
                 )
                """.formatted(subscription ? SQL_INVOICE_IS_SUBSCRIPTION : ("NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ")"));
        sql.append(subExists);
    }

    private void appendGlEntryScopeWhere(
            StringBuilder sql,
            List<Object> args,
            UUID reconciliationRunId,
            UUID financialPeriodId,
            UUID levelId,
            String periodCode
    ) {
        String period = periodCode == null ? "" : periodCode.trim();
        if (reconciliationRunId != null) {
            sql.append("""
                     AND (
                        ge.reconciliation_run_id = ?
                        OR (
                            ge.reconciliation_run_id IS NULL
                            AND (
                                ge.financial_period_id IS NOT DISTINCT FROM ?
                                OR (
                                    ? <> ''
                                    AND EXISTS (
                                        SELECT 1
                                        FROM reconciliation.financial_period fp_scope
                                        WHERE fp_scope.financial_period_id = ge.financial_period_id
                                          AND fp_scope.period_code = ?
                                    )
                                )
                            )
                            AND (
                                ?::uuid IS NULL
                                OR ge.level_id IS NULL
                                OR ge.level_id IS NOT DISTINCT FROM ?::uuid
                            )
                        )
                     )
                    """);
            args.add(reconciliationRunId);
            args.add(financialPeriodId);
            args.add(period);
            args.add(period);
            args.add(levelId);
            args.add(levelId);
            return;
        }
        sql.append("""
                 AND (
                    ge.financial_period_id IS NOT DISTINCT FROM ?
                    OR (
                        ? <> ''
                        AND EXISTS (
                            SELECT 1
                            FROM reconciliation.financial_period fp_scope
                            WHERE fp_scope.financial_period_id = ge.financial_period_id
                              AND fp_scope.period_code = ?
                        )
                    )
                 )
                 AND (
                    ?::uuid IS NULL
                    OR ge.level_id IS NULL
                    OR ge.level_id IS NOT DISTINCT FROM ?::uuid
                 )
                """);
        args.add(financialPeriodId);
        args.add(period);
        args.add(period);
        args.add(levelId);
        args.add(levelId);
    }

    @SuppressWarnings("unchecked")
    private void patchGlPostingChainStagesInSummary(Map<String, Object> parsed, Map<String, Object> glPack) {
        Object scope = parsed.get("reconciliationScope");
        if (!(scope instanceof Map<?, ?> scopeMap)) {
            return;
        }
        Object sub = scopeMap.get("subscription");
        if (sub instanceof Map<?, ?> subMap) {
            Map<String, Object> subScope = (Map<String, Object>) subMap;
            Map<String, Object> subGl = enrichScopeGlWithInvoiceMismatch(
                    scopeGlMetrics(glPack, "subscription"), subScope);
            subScope.put("chainStages", withGlPostingStage(
                    chainStagesFromScope(subScope), 8, subGl, scopeInvoiceMismatch(kpisFromScope(subScope))));
        }
        Object cash = scopeMap.get("cash");
        if (cash instanceof Map<?, ?> cashMap) {
            Map<String, Object> cashScope = (Map<String, Object>) cashMap;
            Map<String, Object> cashGl = enrichScopeGlWithInvoiceMismatch(
                    scopeGlMetrics(glPack, "cash"), cashScope);
            cashScope.put("chainStages", withGlPostingStage(
                    chainStagesFromScope(cashScope), 5, cashGl, scopeInvoiceMismatch(kpisFromScope(cashScope))));
        }
        Object meta = parsed.get("meta");
        if (meta instanceof Map<?, ?> metaMap) {
            ((Map<String, Object>) metaMap).put("glPosting", glPack);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> kpisFromScope(Map<String, Object> scopePart) {
        Object kpis = scopePart.get("kpis");
        if (kpis instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        return Map.of();
    }

    private Map<String, Object> enrichScopeGlWithInvoiceMismatch(
            Map<String, Object> glMetrics,
            Map<String, Object> scopePart
    ) {
        int invoiceMismatch = scopeInvoiceMismatch(kpisFromScope(scopePart));
        Map<String, Object> out = new LinkedHashMap<>(glMetrics);
        out.put("invoiceAmountMismatchCount", invoiceMismatch);
        long paymentTxnCount = ((Number) out.getOrDefault("paymentTxnCount", 0L)).longValue();
        long pairs = ((Number) out.getOrDefault("glPairCount", 0L)).longValue();
        out.put("missingGlPairs", Math.max(0L, paymentTxnCount - pairs));
        out.put("glCoveragePct", paymentTxnCount == 0
                ? (pairs > 0 ? 100.0 : 0.0)
                : Math.min(100.0, pairs * 100.0 / paymentTxnCount));
        return out;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> chainStagesFromScope(Map<String, Object> scopePart) {
        Object raw = scopePart.get("chainStages");
        if (raw instanceof List<?> list) {
            List<Map<String, Object>> stages = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) {
                    stages.add((Map<String, Object>) m);
                }
            }
            return stages;
        }
        return List.of();
    }

    private List<Map<String, Object>> withGlPostingStage(
            List<Map<String, Object>> stages,
            int order,
            Map<String, Object> glMetrics,
            int invoiceAmountMismatchCount
    ) {
        List<Map<String, Object>> out = new ArrayList<>();
        boolean replaced = false;
        for (Map<String, Object> stage : stages) {
            if ("GL_POSTING".equals(stage.get("stageKey"))) {
                out.add(buildGlPostingChainStage(order, "GL Posting", glMetrics, invoiceAmountMismatchCount));
                replaced = true;
            } else {
                out.add(stage);
            }
        }
        if (!replaced) {
            out.add(buildGlPostingChainStage(order, "GL Posting", glMetrics, invoiceAmountMismatchCount));
        }
        return out;
    }

    private Map<String, Object> buildGlPostingChainStage(
            int order,
            String title,
            Map<String, Object> glMetrics,
            int invoiceAmountMismatchCount
    ) {
        long pairs = ((Number) glMetrics.getOrDefault("glPairCount", 0L)).longValue();
        long paymentTxnCount = ((Number) glMetrics.getOrDefault("paymentTxnCount", 0L)).longValue();
        int missingMapping = ((Number) glMetrics.getOrDefault("missingMappingLines", 0)).intValue();
        int unbalanced = ((Number) glMetrics.getOrDefault("unbalancedPairCount", 0)).intValue();
        int missingGlPairs = ((Number) glMetrics.getOrDefault("missingGlPairs", 0)).intValue();
        double coveragePct = ((Number) glMetrics.getOrDefault("glCoveragePct", 0.0)).doubleValue();

        int mismatch = missingMapping + unbalanced + missingGlPairs
                + (invoiceAmountMismatchCount > 0 ? invoiceAmountMismatchCount : 0);
        long volume = paymentTxnCount > 0 ? paymentTxnCount : (pairs > 0 ? pairs : 0L);

        String status;
        if (mismatch == 0 && volume > 0 && coveragePct >= 99.99) {
            status = "MATCHED";
        } else if (volume == 0 && pairs == 0) {
            status = "MATCHED";
        } else if (mismatch >= volume && volume > 0) {
            status = "MISMATCH";
        } else {
            status = "PARTIAL";
        }

        Map<String, Object> stage = new LinkedHashMap<>();
        stage.put("stageKey", "GL_POSTING");
        stage.put("order", order);
        stage.put("title", title);
        stage.put("status", status);
        stage.put("statusDisplay", humanizeStatus(status));
        stage.put("mismatch", mismatch);
        stage.put("mismatchPct", volume == 0 ? 0.0 : mismatch * 100.0 / volume);
        String emphasis = mismatch > 0 ? "DANGER" : "NEUTRAL";
        List<Map<String, Object>> metrics = new ArrayList<>();
        metrics.add(metricRow("GL pairs", pairs, pairs, emphasis));
        metrics.add(metricRow("Payments", paymentTxnCount, paymentTxnCount, emphasis));
        metrics.add(metricRow("Coverage %", coveragePct, coveragePct, emphasis));
        stage.put("metrics", metrics);
        Map<String, Object> glSnapshot = new LinkedHashMap<>(glMetrics);
        glSnapshot.put("invoiceAmountMismatchCount", invoiceAmountMismatchCount);
        stage.put("glPosting", glSnapshot);
        stage.put("drillDown", Map.of(
                "route", "ACCOUNTING_JOURNAL",
                "filters", Map.of("stage", "GL_POSTING")
        ));
        return stage;
    }

    private List<Map<String, Object>> getDashboardMismatchesFromRun(String recoRunId, Map<String, Object> filters, int page, int pageSize) {
        int offset = Math.max(0, (Math.max(1, page) - 1) * Math.max(1, pageSize));
        List<Object> args = new ArrayList<>();
        args.add(recoRunId);
        StringBuilder extra = new StringBuilder();
        List<String> sev = upperList(filters.get("severity"));
        if (!sev.isEmpty()) {
            extra.append(" AND sev.code IN (");
            appendPlaceholders(extra, sev.size());
            extra.append(") ");
            args.addAll(sev);
        }
        List<String> st = upperList(filters.get("status"));
        if (!st.isEmpty()) {
            extra.append(" AND rcs.code IN (");
            appendPlaceholders(extra, st.size());
            extra.append(") ");
            args.addAll(st);
        }
        String search = asString(filters.get("search"));
        if (search != null && !search.isBlank()) {
            extra.append(" AND (COALESCE(s.entity_reference_code, '') ILIKE ? OR CAST(s.entity_id AS text) ILIKE ?) ");
            args.add("%" + search.trim() + "%");
            args.add("%" + search.trim() + "%");
        }
        String reconType = asString(filters.get("reconciliationType"));
        if (reconType != null && !reconType.isBlank()) {
            String u = reconType.trim().toUpperCase();
            if ("SUBSCRIPTION".equals(u)) {
                extra.append("""
                         AND (
                            s.attributes_json->>'reconciliationType' IN ('SUBSCRIPTION')
                            OR (s.attributes_json->>'reconciliationType') IS NULL
                        )
                        """);
            } else if ("CASH".equals(u)) {
                extra.append(" AND s.attributes_json->>'reconciliationType' = 'CASH' ");
            }
        }
        args.add(pageSize);
        args.add(offset);
        return jdbc.queryForList("""
                SELECT
                    s.snapshot_reference_code AS "mismatchId",
                    'INVOICE_TO_PAYMENT' AS "type",
                    COALESCE(s.attributes_json->>'reconciliationType', 'SUBSCRIPTION') AS "reconciliationType",
                    COALESCE(s.entity_reference_code, CAST(s.entity_id AS text)) AS "entityId",
                    '' AS "customerName",
                    '' AS "agreementCode",
                    COALESCE(s.expected_amount, 0) AS "expectedAmount",
                    COALESCE(s.actual_amount, 0) AS "actualAmount",
                    (COALESCE(s.expected_amount, 0) - COALESCE(s.actual_amount, 0)) AS "varianceAmount",
                    CASE WHEN COALESCE(s.expected_amount, 0) = 0 THEN 0
                         ELSE ABS(COALESCE(s.expected_amount, 0) - COALESCE(s.actual_amount, 0)) * 100.0 / COALESCE(s.expected_amount, 1) END AS "variancePct",
                    rcs.code AS "status",
                    sev.code AS "severity",
                    lr.code AS "reasonCode",
                    COALESCE(s.reason_text, '') AS "reasonText",
                    s.event_at AS "createdAt",
                    NULL::text AS "locationId"
                FROM reconciliation.reconciliation_run_snapshot s
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                LEFT JOIN reconciliation.lu_severity sev ON sev.severity_id = s.severity_id
                LEFT JOIN reconciliation.lu_reconciliation_reason lr ON lr.reconciliation_reason_id = s.reconciliation_reason_id
                WHERE rr.run_reference_code = ?
                  AND rcs.code <> 'RECONCILED'
                """ + extra + """
                ORDER BY s.event_at DESC
                LIMIT ? OFFSET ?
                """, args.toArray());
    }

    private List<Map<String, Object>> getDashboardTrendsFromRun(String recoRunId, Map<String, Object> filters, String reconciliationType) {
        String metric = asString(filters.get("metric"));
        String valueExpr;
        if ("mismatch_count".equals(metric)) {
            valueExpr = "COUNT(1) FILTER (WHERE rcs.code <> 'RECONCILED')";
        } else if ("reconciled_count".equals(metric)) {
            valueExpr = "COUNT(1) FILTER (WHERE rcs.code = 'RECONCILED')";
        } else {
            valueExpr = "COALESCE(SUM(ABS(COALESCE(s.expected_amount,0) - COALESCE(s.actual_amount,0))),0)";
        }
        List<Object> targs = new ArrayList<>();
        targs.add(recoRunId);
        StringBuilder typeFilter = new StringBuilder();
        if (reconciliationType != null && !reconciliationType.isBlank()) {
            if ("SUBSCRIPTION".equalsIgnoreCase(reconciliationType)) {
                typeFilter.append("""
                         AND (
                            s.attributes_json->>'reconciliationType' IN ('SUBSCRIPTION')
                            OR (s.attributes_json->>'reconciliationType') IS NULL
                        )
                        """);
            } else if ("CASH".equalsIgnoreCase(reconciliationType)) {
                typeFilter.append(" AND s.attributes_json->>'reconciliationType' = 'CASH' ");
            }
        }
        // Apply .formatted only to the fragment that contains %s; otherwise %s stays literal in SQL (BadSqlGrammar).
        String sql = """
                SELECT to_char(s.event_at, 'YYYY-MM-DD') AS bucket, %s AS value
                FROM reconciliation.reconciliation_run_snapshot s
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                WHERE rr.run_reference_code = ?
                """.formatted(valueExpr)
                + typeFilter
                + """
                GROUP BY to_char(s.event_at, 'YYYY-MM-DD')
                ORDER BY 1
                LIMIT 365
                """;
        return jdbc.queryForList(sql, targs.toArray());
    }

    private int insertMismatchSnapshotsForRun(UUID runId, Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        args.add(runId);
        String where = buildInvoiceWhere(filters, args);
        String has = INVOICE_HAS_ANY_CAPTURED_TXN_EXPR.strip();
        String paid = INVOICE_CAPTURED_PAID_MAJOR_EXPR.strip();
        String scopeTail = where + " AND (\n                    NOT (" + has + ")\n                    OR ABS(COALESCE(i.total_amount, 0)::numeric - (" + paid + ")) > 0.01\n                )";
        String sql = INSERT_MISMATCH_SNAPSHOT_SQL
                .replace("__RECON_HAS_TXN__", has)
                .replace("__RECON_PAID_MAJOR__", paid)
                .replace("__RECON_SCOPE_FILTER__", scopeTail);
        return jdbc.update(sql, args.toArray());
    }

    private static final String RECON_RUN_CATALOG_SCOPE_JOIN = """
            LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = rr.level_id
            """;

    public Map<String, Object> getReconciliationRun(String runId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rr.run_reference_code AS "reconciliationRunId",
                    rr.run_reference_code AS "runId",
                    rr.run_type AS "runType",
                    tm.code AS "triggerMode",
                    st.code AS "status",
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS "financialPeriod",
                    rr.filters_json->>'currency' AS "currency",
                    CAST(rr.requested_by AS text) AS "requestedBy",
                    CAST(rr.application_id AS text) AS "tenantId",
                    rr.filters_json AS "filters",
                    CAST(rr.level_id AS text) AS "scopeLevelId",
                    lv_scope."name" AS "scopeLevelName",
                    rr.started_on AS "startedAt",
                    rr.ended_on AS "endedAt",
                    rr.created_on AS "createdAt",
                    rr.modified_on AS "updatedAt",
                    COALESCE((rr.filters_json->>'runVersion')::int, 1) AS "version",
                    rr.filters_json->>'failureStage' AS "failureStage",
                    rr.filters_json->>'failureMessage' AS "failureMessage",
                    CAST(rr.reco_config_publish_id AS text) AS "recoConfigPublishId",
                    cp.version_label AS "recoConfigVersionLabel",
                    cp.published_at AS "recoConfigPublishedAt"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                JOIN reconciliation.lu_reconciliation_trigger_mode tm ON tm.reconciliation_trigger_mode_id = rr.reconciliation_trigger_mode_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                LEFT JOIN reconciliation.reco_config_publish cp ON cp.reco_config_publish_id = rr.reco_config_publish_id
                """
                + RECON_RUN_CATALOG_SCOPE_JOIN + """
                WHERE rr.run_reference_code = ?
                """, runId);
        if (rows.isEmpty()) {
            return Map.of("reconciliationRunId", runId, "status", "NOT_FOUND");
        }
        List<Map<String, Object>> stageSummary = jdbc.queryForList("""
                SELECT rs.code AS stage, COUNT(1) AS total,
                       COUNT(1) FILTER (WHERE rcs.code <> 'RECONCILED') AS mismatch
                FROM reconciliation.reconciliation_run_snapshot s
                JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id
                JOIN reconciliation.lu_reconciliation_stage rs ON rs.reconciliation_stage_id = s.reconciliation_stage_id
                JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id
                WHERE rr.run_reference_code = ?
                GROUP BY rs.code, rs.stage_order
                ORDER BY rs.stage_order
                """, runId);
        return Map.of("run", rows.get(0), "stageSummary", stageSummary);
    }

    public Map<String, Object> listReconciliationRuns(
            String financialPeriod,
            String fromDate,
            String toDate,
            List<String> locationIds,
            String status,
            String runType,
            String currency,
            int page,
            int pageSize
    ) {
        int p = Math.max(1, page);
        int ps = Math.max(1, Math.min(pageSize, 200));
        int offset = (p - 1) * ps;
        List<Object> filterArgs = new ArrayList<>();
        StringBuilder where = new StringBuilder();
        appendRunCatalogFilters(where, filterArgs, financialPeriod, fromDate, toDate, locationIds, status, runType, currency);
        String fromSql = """
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                JOIN reconciliation.lu_reconciliation_trigger_mode tm ON tm.reconciliation_trigger_mode_id = rr.reconciliation_trigger_mode_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                """
                + RECON_RUN_CATALOG_SCOPE_JOIN + """
                WHERE 1=1
                """ + where;
        Long total = jdbc.queryForObject("SELECT COUNT(1) " + fromSql, Long.class, filterArgs.toArray());
        List<Object> pageArgs = new ArrayList<>(filterArgs);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT
                    rr.run_reference_code AS "reconciliationRunId",
                    rr.run_reference_code AS "runId",
                    rr.run_type AS "runType",
                    tm.code AS "triggerMode",
                    st.code AS "status",
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS "financialPeriod",
                    rr.filters_json->>'currency' AS "currency",
                    CAST(rr.application_id AS text) AS "tenantId",
                    CAST(rr.requested_by AS text) AS "requestedBy",
                    rr.filters_json AS "filters",
                    rr.started_on AS "startedAt",
                    rr.ended_on AS "endedAt",
                    rr.created_on AS "createdAt",
                    rr.modified_on AS "updatedAt",
                    COALESCE((rr.filters_json->>'runVersion')::int, 1) AS "version",
                    rr.filters_json->>'failureStage' AS "failureStage",
                    rr.filters_json->>'failureMessage' AS "failureMessage",
                    CAST(rr.level_id AS text) AS "scopeLevelId",
                    lv_scope."name" AS "scopeLevelName"
                """ + fromSql + """
                ORDER BY COALESCE(rr.ended_on, rr.modified_on, rr.created_on) DESC
                LIMIT ? OFFSET ?
                """, pageArgs.toArray());
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("totalCount", total == null ? 0L : total);
        return out;
    }

    private void appendRunCatalogFilters(
            StringBuilder where,
            List<Object> args,
            String financialPeriod,
            String fromDate,
            String toDate,
            List<String> locationIds,
            String status,
            String runType,
            String currency
    ) {
        if (financialPeriod != null && !financialPeriod.isBlank()) {
            where.append(" AND (fp.period_code = ? OR rr.filters_json->>'financialPeriod' = ?) ");
            args.add(financialPeriod.trim());
            args.add(financialPeriod.trim());
        }
        /*
         * When financialPeriod is set, do not also constrain rr.created_on by fromDate/toDate: the UI passes the
         * accounting month's calendar bounds while runs may be created earlier/later but still belong to that period
         * via financial_period_id / filters_json. Without financialPeriod, fromDate/toDate mean "runs created in range".
         */
        boolean scopeByCreatedOnOnly = financialPeriod == null || financialPeriod.isBlank();
        if (scopeByCreatedOnOnly && fromDate != null && !fromDate.isBlank()) {
            where.append(" AND rr.created_on::date >= ?::date ");
            args.add(fromDate.trim());
        }
        if (scopeByCreatedOnOnly && toDate != null && !toDate.isBlank()) {
            where.append(" AND rr.created_on::date <= ?::date ");
            args.add(toDate.trim());
        }
        if (status != null && !status.isBlank()) {
            where.append(" AND st.code = ? ");
            args.add(status.trim().toUpperCase());
        }
        if (runType != null && !runType.isBlank()) {
            where.append(" AND rr.run_type = ? ");
            args.add(runType.trim().toUpperCase());
        }
        if (currency != null && !currency.isBlank()) {
            where.append(" AND rr.filters_json->>'currency' = ? ");
            args.add(currency.trim().toUpperCase());
        }
        if (locationIds != null && !locationIds.isEmpty()) {
            List<UUID> locUuids = new ArrayList<>();
            for (String loc : locationIds) {
                if (loc == null || loc.isBlank()) {
                    continue;
                }
                for (String piece : loc.split(",")) {
                    String s = piece.trim();
                    if (s.isEmpty()) {
                        continue;
                    }
                    try {
                        locUuids.add(UUID.fromString(s));
                    } catch (IllegalArgumentException ignored) {
                        // skip invalid token
                    }
                }
            }
            if (!locUuids.isEmpty()) {
                where.append("""
                         AND EXISTS (
                            SELECT 1 FROM jsonb_array_elements_text(COALESCE(rr.filters_json->'locationIds', '[]'::jsonb)) elem
                            WHERE NULLIF(TRIM(elem), '')::uuid IN (
                        """);
                appendPlaceholders(where, locUuids.size());
                where.append(")) ");
                args.addAll(locUuids);
            }
        }
    }

    private void enrichRunCatalogFields(Map<String, Object> filterMap) {
        Object cur = filterMap.get("currency");
        if (cur instanceof String s && !s.isBlank()) {
            filterMap.put("currency", s.trim().toUpperCase());
        }
        inferFinancialPeriodFromDateFilters(filterMap);
    }

    /**
     * Next {@code filters_json.runVersion} for a new run: strictly greater than any existing run for the same
     * {@code tenant_id} + {@code financial_period_id} and strictly greater than {@code priorRunVersionForSequence}
     * when provided (resync parent).
     */
    private int computeNextRunVersion(UUID tenantUuid, UUID financialPeriodId, Integer priorRunVersionForSequence) {
        // Do not use PostgreSQL jsonb "?" key-exists operator here: JDBC treats "?" as a bind placeholder.
        Integer maxExisting = jdbc.queryForObject("""
                SELECT COALESCE(MAX(
                    CASE
                        WHEN (rr.filters_json->>'runVersion') ~ '^[0-9]+$'
                        THEN (rr.filters_json->>'runVersion')::int
                    END
                ), 0)
                FROM reconciliation.reconciliation_run rr
                WHERE rr.application_id IS NOT DISTINCT FROM ?
                  AND rr.financial_period_id IS NOT DISTINCT FROM ?
                """,
                Integer.class,
                tenantUuid,
                financialPeriodId);
        int max = maxExisting == null ? 0 : maxExisting;
        int prior = priorRunVersionForSequence == null ? 0 : Math.max(0, priorRunVersionForSequence);
        return Math.max(max, prior) + 1;
    }

    private static int parseStoredRunVersion(Map<String, Object> filterMap) {
        Object v = filterMap.get("runVersion");
        if (v instanceof Number n) {
            return Math.max(1, n.intValue());
        }
        if (v == null) {
            return 1;
        }
        try {
            return Math.max(1, Integer.parseInt(String.valueOf(v).trim()));
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /**
     * When {@code financialPeriod} is absent but {@code fromDate} is {@code yyyy-MM-dd}, set {@code financialPeriod} to {@code yyyy-MM}
     * so {@link #resolveOrEnsureFinancialPeriodId} can link the run row.
     */
    private void inferFinancialPeriodFromDateFilters(Map<String, Object> filterMap) {
        String existing = asString(filterMap.get("financialPeriod"));
        if (existing != null && !existing.isBlank()) {
            return;
        }
        String from = asString(filterMap.get("fromDate"));
        if (from == null || from.length() < 7) {
            return;
        }
        try {
            String dayPart = from.length() >= 10 ? from.substring(0, 10) : from;
            LocalDate d = LocalDate.parse(dayPart);
            filterMap.put("financialPeriod", String.format("%d-%02d", d.getYear(), d.getMonthValue()));
        } catch (Exception ignored) {
            // leave unset
        }
    }

    private static UUID parseOptionalUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private UUID firstLocationIdFromFilters(Map<String, Object> filterMap) {
        for (String id : listOf(filterMap.get("locationIds"))) {
            if (id == null || id.isBlank()) {
                continue;
            }
            try {
                return UUID.fromString(id.trim());
            } catch (IllegalArgumentException ignored) {
                // skip
            }
        }
        return null;
    }

    private UUID resolveFinancialPeriodId(String periodCode) {
        if (periodCode == null || periodCode.isBlank()) {
            return null;
        }
        List<UUID> ids = jdbc.query("""
                        SELECT financial_period_id FROM reconciliation.financial_period WHERE period_code = ? LIMIT 1
                        """,
                (rs, rn) -> rs.getObject("financial_period_id", UUID.class),
                periodCode.trim());
        return ids.isEmpty() ? null : ids.get(0);
    }

    private UUID resolveFinancialPeriodIdByYearMonth(int fiscalYear, int fiscalPeriodNo) {
        List<UUID> ids = jdbc.query("""
                        SELECT financial_period_id FROM reconciliation.financial_period
                        WHERE fiscal_year = ? AND fiscal_period_no = ? LIMIT 1
                        """,
                (rs, rn) -> rs.getObject("financial_period_id", UUID.class),
                fiscalYear, fiscalPeriodNo);
        return ids.isEmpty() ? null : ids.get(0);
    }

    /**
     * Resolves {@code reconciliation.financial_period} by {@code period_code} ({@code yyyy-MM}); if missing, inserts a row
     * (status OPEN) so {@code reconciliation_run.financial_period_id} is populated.
     */
    private UUID resolveOrEnsureFinancialPeriodId(String periodCode) {
        if (periodCode == null || periodCode.isBlank()) {
            return null;
        }
        String code = periodCode.trim();
        UUID existing = resolveFinancialPeriodId(code);
        if (existing != null) {
            return existing;
        }
        int[] ym = parseYearMonth(code);
        if (ym == null) {
            return null;
        }
        int y = ym[0];
        int m = ym[1];
        LocalDate start = LocalDate.of(y, m, 1);
        LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
        jdbc.update("""
                        INSERT INTO reconciliation.financial_period
                        (period_code, fiscal_year, fiscal_period_no, period_start_date, period_end_date, financial_period_status_id, is_active)
                        VALUES (?, ?, ?, ?::date, ?::date,
                            (SELECT financial_period_status_id FROM reconciliation.lu_financial_period_status WHERE code = 'OPEN' LIMIT 1),
                            true)
                        ON CONFLICT (fiscal_year, fiscal_period_no) DO NOTHING
                        """,
                code, y, m, start.toString(), end.toString());
        UUID byCode = resolveFinancialPeriodId(code);
        if (byCode != null) {
            return byCode;
        }
        return resolveFinancialPeriodIdByYearMonth(y, m);
    }

    private static int[] parseYearMonth(String periodCode) {
        String[] parts = periodCode.trim().split("-");
        if (parts.length < 2) {
            return null;
        }
        try {
            int year = Integer.parseInt(parts[0].trim());
            int month = Integer.parseInt(parts[1].trim());
            if (month < 1 || month > 12) {
                return null;
            }
            return new int[]{year, month};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Map<String, Object> getReconciliationLookups() {
        Map<String, Object> out = new HashMap<>();
        out.put("reconciliationStatuses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_reconciliation_status WHERE is_active = true ORDER BY code
                """));
        out.put("severities", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", rank_order AS "rankOrder", is_active AS "active"
                FROM reconciliation.lu_severity WHERE is_active = true ORDER BY rank_order, code
                """));
        out.put("exceptionStatuses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_terminal AS "terminal", is_active AS "active"
                FROM reconciliation.lu_exception_status WHERE is_active = true ORDER BY code
                """));
        out.put("reconciliationStages", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", stage_order AS "stageOrder", is_active AS "active"
                FROM reconciliation.lu_reconciliation_stage WHERE is_active = true ORDER BY stage_order
                """));
        out.put("refundFlags", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_refund_flag WHERE is_active = true ORDER BY code
                """));
        out.put("exportFormats", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_export_format WHERE is_active = true ORDER BY code
                """));
        out.put("reconciliationRunStatuses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_terminal AS "terminal", is_active AS "active"
                FROM reconciliation.lu_reconciliation_run_status WHERE is_active = true ORDER BY code
                """));
        out.put("reconciliationTriggerModes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_reconciliation_trigger_mode WHERE is_active = true ORDER BY code
                """));
        out.put("bankMatchTypes", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_bank_match_type WHERE is_active = true ORDER BY code
                """));
        out.put("glPostingStatuses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_gl_posting_status WHERE is_active = true ORDER BY code
                """));
        out.put("financialPeriodStatuses", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_closed AS "closed", is_active AS "active"
                FROM reconciliation.lu_financial_period_status WHERE is_active = true ORDER BY code
                """));
        out.put("sourceSystems", jdbc.queryForList("""
                SELECT code AS "code", display_name AS "displayName", is_active AS "active"
                FROM reconciliation.lu_source_system WHERE is_active = true ORDER BY code
                """));
        out.put("financialPeriods", jdbc.queryForList("""
                SELECT
                    CAST(financial_period_id AS text) AS "id",
                    period_code AS "periodCode",
                    fiscal_year AS "fiscalYear",
                    fiscal_period_no AS "fiscalPeriodNo",
                    period_start_date::text AS "periodStartDate",
                    period_end_date::text AS "periodEndDate",
                    is_active AS "active"
                FROM reconciliation.financial_period
                WHERE is_active = true
                ORDER BY period_start_date DESC
                LIMIT 240
                """));
        return out;
    }

    private static String accessUserDisplayNameSql(String userAlias) {
        return "COALESCE(NULLIF(TRIM(CONCAT_WS(' ', "
                + userAlias + ".first_name, " + userAlias + ".last_name)), ''), CAST("
                + userAlias + ".user_id AS text))";
    }

    private static List<String> normalizeCodeFilterList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                if (item == null) {
                    continue;
                }
                for (String part : String.valueOf(item).split(",")) {
                    String code = part.trim();
                    if (!code.isEmpty()) {
                        out.add(code.toUpperCase());
                    }
                }
            }
        } else {
            for (String part : String.valueOf(raw).split(",")) {
                String code = part.trim();
                if (!code.isEmpty()) {
                    out.add(code.toUpperCase());
                }
            }
        }
        return out.stream().distinct().toList();
    }

    private static List<UUID> normalizeUuidFilterList(Object raw) {
        List<String> tokens = normalizeCodeFilterList(raw);
        List<UUID> out = new ArrayList<>();
        for (String token : tokens) {
            UUID id = parseOptionalUuid(token);
            if (id != null) {
                out.add(id);
            }
        }
        return out;
    }

    private void appendExceptionListFilters(StringBuilder sql, List<Object> args, Map<String, Object> filters) {
        UUID tenantUuid = parseOptionalUuid(asString(filters.get("tenantId")));
        if (tenantUuid != null) {
            sql.append(" AND (re.application_id IS NOT DISTINCT FROM ? OR re.tenant_id IS NOT DISTINCT FROM ?) ");
            args.add(tenantUuid);
            args.add(tenantUuid);
        }
        String recoRunId = asString(filters.get("recoRunId"));
        if (recoRunId != null && !recoRunId.isBlank()) {
            sql.append("""
                     AND EXISTS (
                        SELECT 1 FROM reconciliation.reconciliation_run rr2
                        WHERE rr2.run_reference_code = ?
                          AND (
                              re.reconciliation_run_id = rr2.reconciliation_run_id
                              OR re.origin_reconciliation_run_id = rr2.reconciliation_run_id
                              OR re.latest_reconciliation_run_id = rr2.reconciliation_run_id
                          )
                    )
                    """);
            args.add(recoRunId.trim());
        }
        List<String> severities = normalizeCodeFilterList(filters.get("severity"));
        if (!severities.isEmpty()) {
            sql.append(" AND UPPER(sev.code) IN (");
            appendPlaceholders(sql, severities.size());
            sql.append(") ");
            args.addAll(severities);
        }
        List<String> statuses = normalizeCodeFilterList(filters.get("status"));
        if (!statuses.isEmpty()) {
            sql.append(" AND UPPER(es.code) IN (");
            appendPlaceholders(sql, statuses.size());
            sql.append(") ");
            args.addAll(statuses);
        }
        List<String> stages = normalizeCodeFilterList(filters.get("stage"));
        if (!stages.isEmpty()) {
            sql.append(" AND UPPER(stg.code) IN (");
            appendPlaceholders(sql, stages.size());
            sql.append(") ");
            args.addAll(stages);
        }
        List<UUID> assigneeIds = normalizeUuidFilterList(filters.get("assigneeIds"));
        if (!assigneeIds.isEmpty()) {
            sql.append(" AND re.assignee_user_id IN (");
            appendPlaceholders(sql, assigneeIds.size());
            sql.append(") ");
            args.addAll(assigneeIds);
        }
        Object slaBreached = filters.get("slaBreached");
        if (slaBreached instanceof Boolean b && b) {
            sql.append("""
                     AND re.sla_due_at IS NOT NULL
                     AND re.sla_due_at < now()
                     AND es.is_terminal = false
                    """);
        }
        String search = asString(filters.get("search"));
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            sql.append("""
                     AND (
                        re.exception_reference_code ILIKE ? ESCAPE '\\'
                        OR re.mismatch_reference_code ILIKE ? ESCAPE '\\'
                        OR re.entity_reference_code ILIKE ? ESCAPE '\\'
                        OR re.title ILIKE ? ESCAPE '\\'
                        OR re.description ILIKE ? ESCAPE '\\'
                        OR CAST(re.entity_id AS text) ILIKE ? ESCAPE '\\'
                     )
                    """);
            for (int i = 0; i < 6; i++) {
                args.add(q);
            }
        }
    }

    private static final String EXCEPTION_LIST_FROM_SQL = """
            FROM reconciliation.reconciliation_exception re
            JOIN reconciliation.lu_severity sev ON sev.severity_id = re.severity_id
            JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = re.reconciliation_stage_id
            JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
            LEFT JOIN access.access_user assignee_u ON assignee_u.user_id = re.assignee_user_id
            LEFT JOIN access.access_user creator_u ON creator_u.user_id = re.created_by
            LEFT JOIN access.access_user resolver_u ON resolver_u.user_id = re.modified_by
            """;

    private static final String EXCEPTION_LIST_SELECT_SQL = buildExceptionListSelectSql();

    private static String buildExceptionListSelectSql() {
        return """
                SELECT
                    re.exception_reference_code AS "exceptionId",
                    re.exception_reference_code AS "exceptionRef",
                    re.mismatch_reference_code AS "mismatchId",
                    COALESCE(re.entity_reference_code, CAST(re.entity_id AS text), '') AS "entityId",
                    re.title,
                    re.description,
                    sev.code AS "severity",
                    stg.code AS "stage",
                    es.code AS "status",
                    CAST(re.assignee_user_id AS text) AS "assigneeId",
                """
                + accessUserDisplayNameSql("assignee_u") + " AS \"assigneeName\", "
                + """
                    (SELECT LEFT(en.note_text, 500)
                     FROM reconciliation.exception_note en
                     WHERE en.reconciliation_exception_id = re.reconciliation_exception_id
                     ORDER BY en.created_on DESC
                     LIMIT 1) AS "latestNote",
                    CAST(CASE
                        WHEN es.code IN ('RESOLVED', 'CLOSED', 'AUTO_RESOLVED', 'SUPERSEDED') THEN re.modified_by
                        ELSE NULL END AS text) AS "resolvedBy",
                    CAST(CASE
                        WHEN es.code IN ('RESOLVED', 'CLOSED', 'AUTO_RESOLVED', 'SUPERSEDED') THEN re.modified_by
                        ELSE NULL END AS text) AS "resolvedById",
                    CASE
                        WHEN es.code = 'AUTO_RESOLVED' THEN 'System'
                        WHEN es.code IN ('RESOLVED', 'CLOSED', 'SUPERSEDED') """
                + " THEN " + accessUserDisplayNameSql("resolver_u") + " "
                + """
                        ELSE NULL END AS "resolvedByName",
                    re.resolved_at AS "resolvedAt",
                    re.created_on AS "createdAt",
                    CAST(re.created_by AS text) AS "createdBy",
                    CAST(re.created_by AS text) AS "createdById",
                    CASE
                        WHEN re.created_by IS NULL THEN 'System' """
                + " ELSE " + accessUserDisplayNameSql("creator_u") + " "
                + """
                    END AS "createdByName",
                    COALESCE(
                        (SELECT rr_l.run_reference_code FROM reconciliation.reconciliation_run rr_l
                         WHERE rr_l.reconciliation_run_id = re.latest_reconciliation_run_id),
                        (SELECT rr_o.run_reference_code FROM reconciliation.reconciliation_run rr_o
                         WHERE rr_o.reconciliation_run_id = re.origin_reconciliation_run_id),
                        (SELECT rr_c.run_reference_code FROM reconciliation.reconciliation_run rr_c
                         WHERE rr_c.reconciliation_run_id = re.reconciliation_run_id)
                    ) AS "recoRunId",
                    (SELECT rr_o.run_reference_code FROM reconciliation.reconciliation_run rr_o
                     WHERE rr_o.reconciliation_run_id = re.origin_reconciliation_run_id) AS "originRunId",
                    (SELECT rr_l.run_reference_code FROM reconciliation.reconciliation_run rr_l
                     WHERE rr_l.reconciliation_run_id = re.latest_reconciliation_run_id) AS "latestRunId",
                    re.occurrence_count AS "occurrenceCount",
                    re.is_auto_resolved AS "isAutoResolved",
                    COALESCE(
                        (SELECT NULLIF(TRIM(s2.attributes_json->>'reconciliationType'), '')
                         FROM reconciliation.reconciliation_run_snapshot s2
                         WHERE s2.reconciliation_run_snapshot_id = re.reconciliation_run_snapshot_id
                         LIMIT 1),
                        'SUBSCRIPTION'
                    ) AS "reconciliationType",
                    re.first_detected_at AS "firstDetectedAt",
                    re.last_seen_at AS "lastSeenAt",
                    re.sla_due_at AS "slaDueAt",
                    (re.sla_due_at IS NOT NULL AND re.sla_due_at < now() AND es.is_terminal = false) AS "slaBreached"
                """;
    }

    public Map<String, Object> listExceptions(Map<String, Object> filters) {
        int p = filters.get("page") instanceof Number n && n.intValue() > 0 ? n.intValue() : 1;
        int ps = filters.get("pageSize") instanceof Number n && n.intValue() > 0
                ? Math.min(n.intValue(), 500) : 20;
        int offset = (p - 1) * ps;

        List<Object> args = new ArrayList<>();
        StringBuilder fromWhere = new StringBuilder(EXCEPTION_LIST_FROM_SQL + " WHERE 1=1 ");
        appendExceptionListFilters(fromWhere, args, filters);

        String countSql = "SELECT COUNT(1) " + fromWhere;
        Long totalN = jdbc.queryForObject(countSql, Long.class, args.toArray());
        long total = totalN == null ? 0L : totalN;

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        String listSql = EXCEPTION_LIST_SELECT_SQL + fromWhere + " ORDER BY re.created_on DESC LIMIT ? OFFSET ?";
        List<Map<String, Object>> items = jdbc.queryForList(listSql, pageArgs.toArray());

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("page", p);
        meta.put("pageSize", ps);
        meta.put("total", total);
        meta.put("totalRecords", total);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) ps)));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        return out;
    }

    public Map<String, Object> listAssignableExceptionUsers(
            String tenantId,
            String search,
            Integer page,
            Integer pageSize,
            Boolean activeOnly
    ) {
        int p = page == null || page < 1 ? 1 : page;
        int ps = pageSize == null || pageSize < 1 ? 25 : Math.min(pageSize, 100);
        int offset = (p - 1) * ps;
        boolean active = activeOnly == null || activeOnly;

        UUID tenantUuid = parseOptionalUuid(tenantId);
        if (tenantUuid == null) {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("page", p);
            meta.put("pageSize", ps);
            meta.put("total", 0L);
            meta.put("totalRecords", 0L);
            meta.put("totalPages", 1);
            return Map.of("items", List.of(), "meta", meta);
        }
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder("""
                WHERE aau.application_id IS NOT DISTINCT FROM ?
                """);
        args.add(tenantUuid);

        if (active) {
            where.append(" AND aau.is_active = true AND COALESCE(r.is_active, true) = true ");
        }
        if (search != null && !search.isBlank()) {
            String q = "%" + search.trim().replace("%", "\\%").replace("_", "\\_") + "%";
            where.append("""
                     AND (
                        CAST(aau.user_id AS text) ILIKE ? ESCAPE '\\'
                        OR COALESCE(u.email, '') ILIKE ? ESCAPE '\\' """
                    + " OR " + accessUserDisplayNameSql("u") + " ILIKE ? ESCAPE '\\' "
                    + """
                     )
                    """);
            args.add(q);
            args.add(q);
            args.add(q);
        }

        String fromSql = """
                FROM access.access_application_user aau
                JOIN access.access_user u ON u.user_id = aau.user_id
                JOIN access.access_user_role_location aurl ON aurl.application_user_id = aau.application_user_id
                JOIN access.access_role r ON r.role_id = aurl.role_id
                  AND r.application_id IS NOT DISTINCT FROM aau.application_id
                """ + where;

        String distinctUsers = """
                SELECT DISTINCT
                    CAST(aau.user_id AS text) AS "userId",
                    """
                + accessUserDisplayNameSql("u") + " AS \"displayName\", "
                + """
                    u.email AS "email",
                    (SELECT r2.name
                     FROM access.access_user_role_location aurl2
                     JOIN access.access_role r2 ON r2.role_id = aurl2.role_id
                     JOIN access.access_application_user aau2 ON aau2.application_user_id = aurl2.application_user_id
                     WHERE aau2.user_id = aau.user_id
                       AND aau2.application_id IS NOT DISTINCT FROM aau.application_id
                     ORDER BY r2.name
                     LIMIT 1) AS "roleName"
                """
                + fromSql;

        Long totalN = jdbc.queryForObject(
                "SELECT COUNT(1) FROM (" + distinctUsers + ") assignable_users_sub",
                Long.class,
                args.toArray());
        long total = totalN == null ? 0L : totalN;

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList(
                distinctUsers + " ORDER BY \"displayName\" LIMIT ? OFFSET ?",
                pageArgs.toArray());

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("page", p);
        meta.put("pageSize", ps);
        meta.put("total", total);
        meta.put("totalRecords", total);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) ps)));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        return out;
    }

    public Map<String, Object> getExceptionDetail(String exceptionId, String tenantId) {
        if (exceptionId == null || exceptionId.isBlank()) {
            return Map.of("status", "NOT_FOUND");
        }
        List<Object> args = new ArrayList<>();
        StringBuilder fromWhere = new StringBuilder(EXCEPTION_LIST_FROM_SQL + " WHERE re.exception_reference_code = ? ");
        args.add(exceptionId.trim());
        Map<String, Object> tenantFilter = new LinkedHashMap<>();
        tenantFilter.put("tenantId", tenantId);
        appendExceptionListFilters(fromWhere, args, tenantFilter);

        List<Map<String, Object>> rows = jdbc.queryForList(
                EXCEPTION_LIST_SELECT_SQL + fromWhere + " LIMIT 1",
                args.toArray());
        if (rows.isEmpty()) {
            return Map.of("status", "NOT_FOUND", "exceptionId", exceptionId);
        }
        Map<String, Object> row = rows.get(0);

        List<Map<String, Object>> notes = jdbc.queryForList("""
                SELECT
                    CAST(en.exception_note_id AS text) AS "noteId",
                    en.note_text AS "text",
                    en.note_text AS "note",
                    en.note_text AS "body",
                    en.created_on AS "createdAt",
                    CAST(en.created_by AS text) AS "createdBy",
                    COALESCE(
                        NULLIF(TRIM(CONCAT_WS(' ', nu.first_name, nu.last_name)), ''),
                        CAST(en.created_by AS text),
                        'System'
                    ) AS "createdByName"
                FROM reconciliation.exception_note en
                JOIN reconciliation.reconciliation_exception re
                    ON re.reconciliation_exception_id = en.reconciliation_exception_id
                LEFT JOIN access.access_user nu ON nu.user_id = en.created_by
                WHERE re.exception_reference_code = ?
                ORDER BY en.created_on ASC
                """, exceptionId.trim());

        Map<String, Object> detail = new LinkedHashMap<>(row);
        detail.put("notes", notes);
        if (notes.isEmpty()) {
            Object latest = row.get("latestNote");
            if (latest != null && !String.valueOf(latest).isBlank()) {
                detail.put("noteSummary", String.valueOf(latest));
            }
        } else {
            Map<String, Object> last = notes.get(notes.size() - 1);
            detail.put("latestNote", last.get("text"));
            detail.put("noteSummary", last.get("text"));
        }
        return detail;
    }

    public String createException(ReconciliationRequests.CreateExceptionRequest request, String tenantId) {
        String exceptionCode = "EX-" + Math.abs(UUID.randomUUID().hashCode());
        UUID entityPk = parseOptionalUuid(request.entityId());
        jdbc.update("""
                INSERT INTO reconciliation.reconciliation_exception
                (exception_reference_code, mismatch_reference_code, entity_id, entity_reference_code, title, description, reconciliation_stage_id, severity_id, exception_status_id, tags_json, application_id, reconciliation_entity_type_id, created_on, modified_on)
                VALUES (
                    ?, ?, ?, ?, ?, ?,
                    (SELECT reconciliation_stage_id FROM reconciliation.lu_reconciliation_stage WHERE code = ?),
                    (SELECT severity_id FROM reconciliation.lu_severity WHERE code = ?),
                    (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code = 'OPEN'),
                    ?::jsonb,
                    NULLIF(?, '')::uuid,
                    (SELECT reconciliation_entity_type_id FROM reconciliation.lu_reconciliation_entity_type WHERE code = 'INVOICE' LIMIT 1),
                    now(), now()
                )
                """, exceptionCode, request.mismatchId(), entityPk, request.entityId(), request.title(), request.description(), request.stage().toUpperCase(), request.severity().toUpperCase(), toJson(request.tags() == null ? List.of() : request.tags()), tenantId);
        return exceptionCode;
    }

    public void assignException(String exceptionId, String assigneeId) {
        jdbc.update("""
                UPDATE reconciliation.reconciliation_exception
                SET assignee_user_id = NULLIF(?, '')::uuid,
                    exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code='IN_PROGRESS'),
                    modified_on = now()
                WHERE exception_reference_code = ?
                """, assigneeId, exceptionId);
    }

    public void updateExceptionStatus(String exceptionId, String status, String reason) {
        String st = status.toUpperCase();
        jdbc.update("""
                UPDATE reconciliation.reconciliation_exception
                SET exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code=?),
                    resolution_note = ?,
                    resolved_at = CASE WHEN ? IN ('RESOLVED','CLOSED','AUTO_RESOLVED','SUPERSEDED') THEN now() ELSE resolved_at END,
                    closed_at = CASE WHEN ? = 'CLOSED' THEN now() ELSE closed_at END,
                    is_auto_resolved = CASE WHEN ? = 'AUTO_RESOLVED' THEN true ELSE is_auto_resolved END,
                    modified_on = now()
                WHERE exception_reference_code = ?
                """, st, reason, st, st, st, exceptionId);
    }

    public int bulkExceptionAction(ReconciliationRequests.BulkActionRequest request) {
        if (request.exceptionIds() == null || request.exceptionIds().isEmpty()) {
            return 0;
        }
        String placeholders = request.exceptionIds().stream().map(i -> "?").reduce((a, b) -> a + "," + b).orElse("?");
        if (request.status() != null && !request.status().isBlank()) {
            String st = request.status().toUpperCase();
            List<Object> args = new ArrayList<>();
            args.add(st);
            args.add(request.note());
            args.add(st);
            args.add(st);
            args.add(st);
            args.addAll(request.exceptionIds());
            return jdbc.update("""
                    UPDATE reconciliation.reconciliation_exception
                    SET exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code=?),
                        resolution_note = COALESCE(?, resolution_note),
                        resolved_at = CASE WHEN ? IN ('RESOLVED','CLOSED','AUTO_RESOLVED','SUPERSEDED') THEN now() ELSE resolved_at END,
                        closed_at = CASE WHEN ? = 'CLOSED' THEN now() ELSE closed_at END,
                        is_auto_resolved = CASE WHEN ? = 'AUTO_RESOLVED' THEN true ELSE is_auto_resolved END,
                        modified_on = now()
                    WHERE exception_reference_code IN (%s)
                    """.formatted(placeholders), args.toArray());
        }
        List<Object> args = new ArrayList<>();
        args.addAll(request.exceptionIds());
        return jdbc.update("UPDATE reconciliation.reconciliation_exception SET modified_on = now() WHERE exception_reference_code IN (" + placeholders + ")", args.toArray());
    }

    public void addExceptionNote(String exceptionId, String note) {
        addExceptionNote(exceptionId, note, null);
    }

    public void addExceptionNote(String exceptionId, String note, String exceptionNoteTypeCode) {
        UUID typeId = null;
        if (exceptionNoteTypeCode != null && !exceptionNoteTypeCode.isBlank()) {
            List<UUID> ids = jdbc.query(
                    "SELECT exception_note_type_id FROM reconciliation.lu_exception_note_type WHERE UPPER(code) = UPPER(?) AND is_active = true LIMIT 1",
                    (rs, rn) -> rs.getObject(1, UUID.class),
                    exceptionNoteTypeCode.trim());
            typeId = ids.isEmpty() ? null : ids.get(0);
        }
        jdbc.update("""
                INSERT INTO reconciliation.exception_note (reconciliation_exception_id, note_text, created_on, exception_note_type_id)
                VALUES ((SELECT reconciliation_exception_id FROM reconciliation.reconciliation_exception WHERE exception_reference_code = ?), ?, now(), ?)
                """, exceptionId, note, typeId);
    }

    /**
     * Journal listing from {@code reconciliation.gl_entry} joined to {@code finance.gl_code},
     * mapping rule dimensions, and reconciliation lookups (matches production DDL).
     */
    public Map<String, Object> getJournal(
            String tenantId,
            String period,
            List<String> glAccountCodes,
            List<String> postingStatus,
            String entityId,
            String recoRunId,
            Integer page,
            Integer pageSize
    ) {
        int p = page == null || page < 1 ? 1 : page;
        int ps = pageSize == null || pageSize < 1 ? 50 : Math.min(pageSize, 500);
        int offset = (p - 1) * ps;

        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    ge.gl_entry_reference_code AS "journalId",
                    CAST(ge.gl_entry_id AS text) AS "glEntryId",
                    COALESCE(ge.entity_reference_code, CAST(ge.entity_id AS text)) AS "entityId",
                    CAST(ge.entity_id AS text) AS "entityUuid",
                    et.code AS "entityType",
                    et.display_name AS "entityTypeName",
                    rr.run_reference_code AS "recoRunId",
                    CAST(rr.application_id AS text) AS "applicationId",
                    fp.period_code AS "periodCode",
                    CAST(ge.gl_code_id AS text) AS "glCodeId",
                    gc.gl_code AS "glAccountCode",
                    gc.description AS "glAccountName",
                    gat.name AS "glAccountType",
                    CAST(ge.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    gmr.mapping_code AS "glMappingRuleCode",
                    gmr.mapping_name AS "glMappingRuleName",
                    jt.code AS "journalType",
                    jt.display_name AS "journalTypeName",
                    lob.code AS "lineOfBusiness",
                    lob.display_name AS "lineOfBusinessName",
                    gst.code AS "sourceType",
                    gst.display_name AS "sourceTypeName",
                    gtx.code AS "transactionType",
                    gtx.display_name AS "transactionTypeName",
                    pcur.currency_code AS "currencyCode",
                    ge.debit_amount AS "debitAmount",
                    ge.credit_amount AS "creditAmount",
                    COALESCE(gps.code, 'UNKNOWN') AS "postingStatus",
                    gps.display_name AS "postingStatusName",
                    ge.posting_reference AS "postingReference",
                    ge.posted_at AS "postedAt",
                    ge.created_on AS "createdAt",
                    ge.modified_on AS "updatedAt"
                FROM reconciliation.gl_entry ge
                INNER JOIN finance.gl_code gc ON gc.gl_code_id = ge.gl_code_id
                LEFT JOIN finance.lu_gl_account_type gat ON gat.gl_account_type_id = gc.gl_account_type_id
                LEFT JOIN reconciliation.lu_gl_posting_status gps ON gps.gl_posting_status_id = ge.gl_posting_status_id
                LEFT JOIN reconciliation.lu_reconciliation_entity_type et ON et.reconciliation_entity_type_id = ge.entity_type_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = ge.financial_period_id
                LEFT JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = ge.reconciliation_run_id
                LEFT JOIN reconciliation.gl_mapping_rule gmr ON gmr.gl_mapping_rule_id = ge.gl_mapping_rule_id
                LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = COALESCE(ge.journal_type_id, gmr.journal_type_id)
                LEFT JOIN reconciliation.lu_line_of_business lob ON lob.line_of_business_id = COALESCE(ge.line_of_business_id, gmr.line_of_business_id)
                LEFT JOIN reconciliation.lu_gl_source_type gst ON gst.gl_source_type_id = gmr.gl_source_type_id
                LEFT JOIN reconciliation.lu_gl_transaction_type gtx ON gtx.gl_transaction_type_id = gmr.gl_transaction_type_id
                LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur
                    ON pcur.payment_gateway_currency_type_id = COALESCE(ge.payment_currency_type_id, gmr.payment_currency_type_id)
                WHERE 1=1
                """);
        UUID tenantUuid = parseOptionalUuid(tenantId);
        String runRef = recoRunId == null || recoRunId.isBlank() ? null : recoRunId.trim();
        boolean runScoped = runRef != null;
        if (tenantUuid != null) {
            if (runScoped) {
                sql.append("""
                         AND EXISTS (
                            SELECT 1 FROM reconciliation.reconciliation_run rr_tenant
                            WHERE rr_tenant.run_reference_code = ?
                              AND rr_tenant.application_id IS NOT DISTINCT FROM ?
                         )
                        """);
                args.add(runRef);
                args.add(tenantUuid);
            } else {
                sql.append("""
                         AND (
                            gc.application_id IS NOT DISTINCT FROM ?
                            OR gmr.application_id IS NOT DISTINCT FROM ?
                            OR rr.application_id IS NOT DISTINCT FROM ?
                         )
                        """);
                args.add(tenantUuid);
                args.add(tenantUuid);
                args.add(tenantUuid);
            }
        }
        if (runScoped) {
            appendJournalRunScopeFilter(sql, args, runRef);
        }
        appendJournalPeriodFilter(sql, args, period, runScoped);
        List<String> accounts = glAccountCodes == null ? List.of() : glAccountCodes.stream().filter(s -> s != null && !s.isBlank()).toList();
        if (!accounts.isEmpty()) {
            sql.append(" AND gc.gl_code IN (");
            appendPlaceholders(sql, accounts.size());
            sql.append(") ");
            args.addAll(accounts);
        }
        List<String> pstat = postingStatus == null ? List.of() : postingStatus.stream().filter(s -> s != null && !s.isBlank()).map(String::toUpperCase).toList();
        if (!pstat.isEmpty()) {
            sql.append(" AND UPPER(COALESCE(gps.code, '')) IN (");
            appendPlaceholders(sql, pstat.size());
            sql.append(") ");
            args.addAll(pstat);
        }
        if (entityId != null && !entityId.isBlank()) {
            sql.append(" AND (ge.entity_reference_code = ? OR CAST(ge.entity_id AS text) = ?) ");
            args.add(entityId);
            args.add(entityId);
        }

        String fromWhere = sql.toString();
        Long n = jdbc.queryForObject("SELECT COUNT(1) FROM (" + fromWhere + ") AS journal_count_sub", Long.class, args.toArray());
        long total = n == null ? 0L : n;

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(ps);
        pageArgs.add(offset);
        sql.append(" ORDER BY ge.created_on DESC LIMIT ? OFFSET ? ");
        List<Map<String, Object>> items = jdbc.queryForList(sql.toString(), pageArgs.toArray());
        if (runScoped) {
            for (Map<String, Object> item : items) {
                if (item.get("recoRunId") == null) {
                    item.put("recoRunId", runRef);
                }
            }
        }

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("total", total);
        meta.put("page", p);
        meta.put("pageSize", ps);
        meta.put("totalPages", (int) Math.max(1, Math.ceil(total / (double) ps)));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("meta", meta);
        return out;
    }

    /**
     * Match entries posted directly on the run, or (common for payment-posted GL) entries with
     * {@code reconciliation_run_id} null but same financial period / period code / level as the run.
     */
    private void appendJournalRunScopeFilter(StringBuilder sql, List<Object> args, String runRef) {
        sql.append("""
                 AND EXISTS (
                    SELECT 1
                    FROM reconciliation.reconciliation_run rr_scope
                    LEFT JOIN reconciliation.financial_period fp_scope
                        ON fp_scope.financial_period_id = rr_scope.financial_period_id
                    WHERE rr_scope.run_reference_code = ?
                      AND (
                        ge.reconciliation_run_id = rr_scope.reconciliation_run_id
                        OR (
                            ge.reconciliation_run_id IS NULL
                            AND (
                                ge.financial_period_id IS NOT DISTINCT FROM rr_scope.financial_period_id
                                OR (
                                    fp.period_code IS NOT NULL
                                    AND (
                                        fp.period_code = fp_scope.period_code
                                        OR fp.period_code = COALESCE(rr_scope.filters_json->>'financialPeriod', '')
                                    )
                                )
                            )
                            AND (
                                ge.level_id IS NULL
                                OR rr_scope.level_id IS NULL
                                OR ge.level_id IS NOT DISTINCT FROM rr_scope.level_id
                            )
                        )
                      )
                 )
                """);
        args.add(runRef);
    }

    /**
     * Period may live on {@code gl_entry.financial_period_id} or only on the linked reconciliation run
     * ({@code financial_period.period_code} / {@code filters_json.financialPeriod}) when entry period is null.
     */
    private void appendJournalPeriodFilter(StringBuilder sql, List<Object> args, String period, boolean runScoped) {
        if (period == null || period.isBlank()) {
            return;
        }
        String periodCode = period.trim();
        if (runScoped) {
            // Run scope (appendJournalRunScopeFilter) already aligns period via financial_period_id / period_code.
            sql.append(" AND (fp.period_code = ? OR fp.period_code IS NULL) ");
            args.add(periodCode);
            return;
        }
        sql.append("""
                 AND (
                    fp.period_code = ?
                    OR (
                        fp.period_code IS NULL
                        AND EXISTS (
                            SELECT 1
                            FROM reconciliation.reconciliation_run rr_per
                            LEFT JOIN reconciliation.financial_period fp_run
                                ON fp_run.financial_period_id = rr_per.financial_period_id
                            WHERE rr_per.reconciliation_run_id = ge.reconciliation_run_id
                              AND (
                                fp_run.period_code = ?
                                OR COALESCE(rr_per.filters_json->>'financialPeriod', '') = ?
                              )
                        )
                    )
                 )
                """);
        args.add(periodCode);
        args.add(periodCode);
        args.add(periodCode);
    }

    /**
     * Validates DR/CR journal pairs: sums debits and credits for all lines sharing the same
     * pair key ({@code regexp_replace(gl_entry_reference_code, '-(DR|CR)$', '')}).
     * When {@code applyPostingStatus} is true, balanced pairs → {@code POSTED}, else → {@code UNBALANCED}
     * on all lines in the pair ({@code lu_gl_posting_status}).
     */
    public List<Map<String, Object>> validateJournal(List<String> journalIds, boolean applyPostingStatus) {
        if (journalIds == null || journalIds.isEmpty()) {
            return List.of();
        }
        List<String> requested = journalIds.stream()
                .filter(s -> s != null && !s.isBlank())
                .map(String::trim)
                .toList();
        if (requested.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> pairKeys = new LinkedHashSet<>();
        for (String id : requested) {
            pairKeys.add(journalPairKey(id));
        }
        String placeholders = pairKeys.stream().map(i -> "?").collect(Collectors.joining(","));
        List<Map<String, Object>> lines = jdbc.queryForList("""
                SELECT
                    regexp_replace(ge.gl_entry_reference_code, '-(DR|CR)$', '') AS pair_key,
                    COALESCE(ge.debit_amount, 0) AS debit_amount,
                    COALESCE(ge.credit_amount, 0) AS credit_amount
                FROM reconciliation.gl_entry ge
                WHERE regexp_replace(ge.gl_entry_reference_code, '-(DR|CR)$', '') IN (%s)
                   OR ge.gl_entry_reference_code IN (%s)
                """.formatted(placeholders, placeholders),
                Stream.concat(pairKeys.stream(), pairKeys.stream()).toArray());
        Map<String, double[]> totalsByPair = new LinkedHashMap<>();
        for (Map<String, Object> line : lines) {
            String pairKey = String.valueOf(line.get("pair_key"));
            double[] totals = totalsByPair.computeIfAbsent(pairKey, k -> new double[2]);
            totals[0] += ((Number) line.get("debit_amount")).doubleValue();
            totals[1] += ((Number) line.get("credit_amount")).doubleValue();
        }
        Set<String> appliedPairs = new LinkedHashSet<>();
        List<Map<String, Object>> out = new ArrayList<>();
        for (String id : requested) {
            String pairKey = journalPairKey(id);
            double[] totals = totalsByPair.get(pairKey);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("journalId", id);
            if (totals == null) {
                row.put("validationStatus", "NOT_FOUND");
            } else {
                boolean balanced = Math.abs(totals[0] - totals[1]) <= 0.01;
                String validationStatus = balanced ? "BALANCED" : "UNBALANCED";
                row.put("validationStatus", validationStatus);
                if (applyPostingStatus) {
                    String postingStatus = balanced ? "POSTED" : "UNBALANCED";
                    row.put("postingStatus", postingStatus);
                    if (appliedPairs.add(pairKey)) {
                        row.put("linesUpdated", applyGlPostingStatusForPair(pairKey, postingStatus));
                    }
                }
            }
            out.add(row);
        }
        return out;
    }

    /**
     * Sets {@code gl_posting_status_id} for every line in a DR/CR pair. {@code POSTED} sets {@code posted_at = now()};
     * {@code UNBALANCED} clears {@code posted_at}.
     */
    private int applyGlPostingStatusForPair(String pairKey, String postingStatusCode) {
        boolean markPosted = "POSTED".equalsIgnoreCase(postingStatusCode);
        return jdbc.update("""
                UPDATE reconciliation.gl_entry ge
                SET gl_posting_status_id = (
                        SELECT gl_posting_status_id
                        FROM reconciliation.lu_gl_posting_status
                        WHERE UPPER(code) = UPPER(?)
                          AND is_active = true
                        LIMIT 1
                    ),
                    posted_at = CASE WHEN ? THEN now() ELSE NULL END,
                    modified_on = now()
                WHERE regexp_replace(ge.gl_entry_reference_code, '-(DR|CR)$', '') = ?
                   OR ge.gl_entry_reference_code = ?
                """, postingStatusCode, markPosted, pairKey, pairKey);
    }

    private static String journalPairKey(String journalId) {
        return journalId.replaceAll("-(DR|CR)$", "");
    }

    public String saveBankStatement(String fileName, String bankAccountId, String fromDate, String toDate, String format) {
        String statementCode = "BST-" + Math.abs(UUID.randomUUID().hashCode());
        jdbc.update("""
                INSERT INTO reconciliation.bank_statement
                (statement_reference_code, bank_account_id, file_name, statement_date_from, statement_date_to, format_code, uploaded_on)
                VALUES (?, NULLIF(?, '')::uuid, ?, ?::date, ?::date, ?, now())
                """, statementCode, bankAccountId, fileName, fromDate, toDate, format);
        return statementCode;
    }

    public String createBankReconcileRun(ReconciliationRequests.TriggerBankReconcileRequest request) {
        String runCode = "BRR-" + Math.abs(UUID.randomUUID().hashCode());
        jdbc.update("""
                INSERT INTO reconciliation.bank_reconciliation
                (bank_reconciliation_ref_code, bank_statement_id, reconciliation_run_status_id, tolerance_amount, created_on, modified_on)
                VALUES (
                    ?,
                    (SELECT bank_statement_id FROM reconciliation.bank_statement WHERE statement_reference_code = ?),
                    (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code='QUEUED'),
                    ?,
                    now(), now()
                )
                """, runCode, request.statementId(), request.toleranceAmount());
        return runCode;
    }

    public Map<String, Object> getBankReconcileResult(String reconcileRunId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    br.bank_reconciliation_ref_code AS "reconcileRunId",
                    bs.statement_reference_code AS "statementId",
                    st.code AS "status",
                    br.matched_count AS "matchedCount",
                    br.unmatched_count AS "unmatchedCount",
                    br.modified_on AS "updatedAt"
                FROM reconciliation.bank_reconciliation br
                LEFT JOIN reconciliation.bank_statement bs ON bs.bank_statement_id = br.bank_statement_id
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = br.reconciliation_run_status_id
                WHERE br.bank_reconciliation_ref_code = ?
                """, reconcileRunId);
        return rows.isEmpty() ? Map.of("reconcileRunId", reconcileRunId, "status", "NOT_FOUND") : rows.get(0);
    }

    public void manualMatch(String reconcileRunId, ReconciliationRequests.ManualMatchRequest request) {
        jdbc.update("""
                INSERT INTO reconciliation.bank_reconciliation_match
                (bank_reconciliation_id, bank_match_type_id, bank_entry_reference, system_transaction_reference, notes, created_on)
                VALUES (
                    (SELECT bank_reconciliation_id FROM reconciliation.bank_reconciliation WHERE bank_reconciliation_ref_code = ?),
                    (SELECT bank_match_type_id FROM reconciliation.lu_bank_match_type WHERE code='MANUAL'),
                    ?, ?, ?, now()
                )
                """, reconcileRunId, request.bankEntryRef(), request.systemTxnRef(), request.notes());
    }

    public void flagBankException(String reconcileRunId, ReconciliationRequests.FlagExceptionRequest request) {
        jdbc.update("""
                INSERT INTO reconciliation.bank_reconciliation_match
                (bank_reconciliation_id, bank_match_type_id, bank_entry_reference, notes, created_on)
                VALUES (
                    (SELECT bank_reconciliation_id FROM reconciliation.bank_reconciliation WHERE bank_reconciliation_ref_code = ?),
                    (SELECT bank_match_type_id FROM reconciliation.lu_bank_match_type WHERE code='EXCEPTION'),
                    ?, ?, now()
                )
                """, reconcileRunId, request.bankEntryRef(), request.reasonCode() + ":" + request.reasonText());
    }

    public List<Map<String, Object>> listRefunds(String recoRunId) {
        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    refund_reference_code AS "refundId",
                    COALESCE(invoice_reference_code, CAST(invoice_id AS text)) AS "invoiceId",
                    expected_amount AS "expectedAmount",
                    actual_amount AS "actualAmount",
                    rs.code AS "status",
                    rt.created_on AS "createdAt"
                FROM reconciliation.refund_transaction rt
                JOIN reconciliation.lu_reconciliation_status rs ON rs.reconciliation_status_id = rt.reconciliation_status_id
                WHERE 1=1
                """);
        if (recoRunId != null && !recoRunId.isBlank()) {
            sql.append("""
                     AND rt.reconciliation_run_id = (
                        SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                    )
                    """);
            args.add(recoRunId);
        }
        sql.append(" ORDER BY rt.created_on DESC LIMIT 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> refundDetail(String refundId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    rt.refund_reference_code AS "refundId",
                    rt.invoice_reference_code AS "invoiceId",
                    rt.expected_amount AS "expectedAmount",
                    rt.actual_amount AS "actualAmount",
                    rs.code AS "status",
                    rt.reason_code AS "reasonCode",
                    rt.created_on AS "createdAt",
                    rt.modified_on AS "updatedAt"
                FROM reconciliation.refund_transaction rt
                JOIN reconciliation.lu_reconciliation_status rs ON rs.reconciliation_status_id = rt.reconciliation_status_id
                WHERE rt.refund_reference_code = ?
                """, refundId);
        return rows.isEmpty() ? Map.of("refundId", refundId, "status", "NOT_FOUND") : rows.get(0);
    }

    public void retryRefund(String refundId) {
        jdbc.update("""
                UPDATE reconciliation.refund_transaction
                SET refund_flag_id = (SELECT refund_flag_id FROM reconciliation.lu_refund_flag WHERE code='RETRY'),
                    modified_on = now()
                WHERE refund_reference_code = ?
                """, refundId);
    }

    public void linkRefund(String refundId, String invoiceId) {
        jdbc.update("""
                UPDATE reconciliation.refund_transaction
                SET invoice_reference_code = ?, modified_on = now()
                WHERE refund_reference_code = ?
                """, invoiceId, refundId);
    }

    public List<Map<String, Object>> reportCatalog() {
        return List.of(
                Map.of("slug", "revenue-vs-collection", "name", "Revenue vs Collection"),
                Map.of("slug", "exception-trends", "name", "Exception Trends"),
                Map.of("slug", "gateway-performance", "name", "Gateway Performance")
        );
    }

    public Map<String, Object> normalizeReportFilters(Object filtersRaw, String tenantId) {
        Map<String, Object> filters = normalizeFiltersObject(filtersRaw);
        if (tenantId != null && !tenantId.isBlank()) {
            filters.putIfAbsent("tenantId", tenantId.trim());
            filters.putIfAbsent("applicationId", tenantId.trim());
        }
        return filters;
    }

    public Map<String, Object> reportQuery(String slug, Object filtersRaw) {
        Map<String, Object> filters = normalizeFiltersObject(filtersRaw);
        applyDefaultReportDates(filters);
        String normalized = slug == null ? "" : slug.trim().toLowerCase();
        return switch (normalized) {
            case "revenue-vs-collection" -> queryRevenueVsCollectionReport(filters);
            case "exception-trends" -> queryExceptionTrendsReport(filters);
            case "gateway-performance" -> queryGatewayPerformanceReport(filters);
            default -> unknownReportPayload(slug, filters);
        };
    }

    private void applyDefaultReportDates(Map<String, Object> filters) {
        String to = asString(filters.get("toDate"));
        String from = asString(filters.get("fromDate"));
        LocalDate end = to == null || to.isBlank() ? LocalDate.now() : LocalDate.parse(to.trim());
        if (to == null || to.isBlank()) {
            filters.put("toDate", end.toString());
        }
        if (from == null || from.isBlank()) {
            filters.put("fromDate", end.minusDays(29).toString());
        }
    }

    private Map<String, Object> unknownReportPayload(String slug, Map<String, Object> filters) {
        return reportEnvelope(
                slug,
                filters,
                Map.of("message", "Unknown report slug"),
                List.of(),
                List.of()
        );
    }

    private Map<String, Object> reportEnvelope(
            String slug,
            Map<String, Object> filters,
            Map<String, Object> summary,
            List<Map<String, Object>> rows,
            List<Map<String, Object>> breakdown
    ) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("slug", slug);
        out.put("filters", filters);
        out.put("generatedAt", OffsetDateTime.now().toString());
        out.put("summary", summary);
        out.put("rows", rows);
        if (breakdown != null && !breakdown.isEmpty()) {
            out.put("breakdown", breakdown);
        }
        out.put("meta", Map.of(
                "rowCount", rows.size(),
                "hasData", !rows.isEmpty(),
                "breakdownCount", breakdown == null ? 0 : breakdown.size()
        ));
        return out;
    }

    private static double reportPct(double numerator, double denominator) {
        return denominator == 0d ? 0d : Math.round(numerator * 10000.0 / denominator) / 100.0;
    }

    private UUID applicationIdFromFilters(Map<String, Object> filters) {
        UUID id = parseOptionalUuid(asString(filters.get("applicationId")));
        if (id != null) {
            return id;
        }
        return parseOptionalUuid(asString(filters.get("tenantId")));
    }

    private Map<String, Object> queryRevenueVsCollectionReport(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String paidExpr = INVOICE_CAPTURED_PAID_MAJOR_EXPR.strip();
        String invoiceFrom = """
                FROM transactions.invoice i
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """;
        String invoiceScoped = """
                SELECT
                    i.invoice_id,
                    MAX(i.created_on::date) AS period_date,
                    COALESCE(MAX(i.total_amount), 0)::numeric AS revenue_amount,
                    MAX("""
                + "(" + paidExpr + ")"
                + """
                    )::numeric AS collected_amount
                """
                + invoiceFrom + where + """
                GROUP BY i.invoice_id
                """;

        String totalsSql = """
                SELECT
                    COUNT(*) AS "invoiceCount",
                    COALESCE(SUM(scoped.revenue_amount), 0) AS "revenueAmount",
                    COALESCE(SUM(scoped.collected_amount), 0) AS "collectedAmount",
                    COUNT(*) FILTER (
                        WHERE ABS(scoped.revenue_amount - scoped.collected_amount) <= 0.01
                    ) AS "reconciledCount",
                    COUNT(*) FILTER (
                        WHERE ABS(scoped.revenue_amount - scoped.collected_amount) > 0.01
                    ) AS "varianceCount"
                FROM ("""
                + invoiceScoped + ") scoped";
        Map<String, Object> totals = jdbc.queryForMap(totalsSql, args.toArray());

        String rowsSql = """
                SELECT
                    scoped.period_date AS "periodDate",
                    COUNT(*) AS "invoiceCount",
                    COALESCE(SUM(scoped.revenue_amount), 0) AS "revenueAmount",
                    COALESCE(SUM(scoped.collected_amount), 0) AS "collectedAmount",
                    COALESCE(SUM(scoped.revenue_amount - scoped.collected_amount), 0) AS "varianceAmount"
                FROM ("""
                + invoiceScoped + """
                ) scoped
                GROUP BY scoped.period_date
                ORDER BY scoped.period_date
                """;
        List<Map<String, Object>> rows = jdbc.queryForList(rowsSql, args.toArray());
        enrichRevenueCollectionRows(rows);

        double revenue = num(totals.get("revenueAmount"));
        double collected = num(totals.get("collectedAmount"));
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("invoiceCount", ((Number) totals.getOrDefault("invoiceCount", 0)).longValue());
        summary.put("revenueAmount", revenue);
        summary.put("collectedAmount", collected);
        summary.put("varianceAmount", revenue - collected);
        summary.put("collectionRatePct", reportPct(collected, revenue));
        summary.put("reconciledCount", ((Number) totals.getOrDefault("reconciledCount", 0)).longValue());
        summary.put("varianceCount", ((Number) totals.getOrDefault("varianceCount", 0)).longValue());
        summary.put("reconciledRatePct", reportPct(
                ((Number) totals.getOrDefault("reconciledCount", 0)).doubleValue(),
                ((Number) totals.getOrDefault("invoiceCount", 0)).doubleValue()));

        return reportEnvelope("revenue-vs-collection", filters, summary, rows, List.of());
    }

    private void enrichRevenueCollectionRows(List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            double revenue = num(row.get("revenueAmount"));
            double collected = num(row.get("collectedAmount"));
            row.put("collectionRatePct", reportPct(collected, revenue));
            row.put("varianceAmount", num(row.get("varianceAmount")));
        }
    }

    private Map<String, Object> queryExceptionTrendsReport(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");
        String fromDate = asString(filters.get("fromDate"));
        String toDate = asString(filters.get("toDate"));
        if (fromDate != null && !fromDate.isBlank()) {
            where.append(" AND re.created_on::date >= ?::date ");
            args.add(fromDate);
        }
        if (toDate != null && !toDate.isBlank()) {
            where.append(" AND re.created_on::date <= ?::date ");
            args.add(toDate);
        }
        UUID applicationId = applicationIdFromFilters(filters);
        if (applicationId != null) {
            where.append(" AND re.application_id IS NOT DISTINCT FROM ? ");
            args.add(applicationId);
        }

        String fromSql = """
                FROM reconciliation.reconciliation_exception re
                JOIN reconciliation.lu_severity sev ON sev.severity_id = re.severity_id
                JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = re.reconciliation_stage_id
                """ + where;

        Map<String, Object> totals = jdbc.queryForMap("""
                SELECT
                    COUNT(*) AS "totalExceptions",
                    COUNT(*) FILTER (WHERE es.code IN ('OPEN', 'IN_PROGRESS', 'ESCALATED')) AS "openCount",
                    COUNT(*) FILTER (WHERE es.is_terminal = true OR es.code IN ('RESOLVED', 'CLOSED')) AS "closedCount",
                    COUNT(*) FILTER (WHERE sev.code = 'CRITICAL') AS "criticalCount",
                    COUNT(*) FILTER (WHERE sev.code = 'HIGH') AS "highCount"
                """ + fromSql, args.toArray());

        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    re.created_on::date AS "periodDate",
                    COUNT(*) AS "openedCount",
                    COUNT(*) FILTER (WHERE re.resolved_at IS NOT NULL OR re.closed_at IS NOT NULL) AS "resolvedCount",
                    COUNT(*) FILTER (WHERE es.code IN ('OPEN', 'IN_PROGRESS', 'ESCALATED')) AS "openCount",
                    COUNT(*) FILTER (WHERE sev.code = 'CRITICAL') AS "criticalCount",
                    COUNT(*) FILTER (WHERE sev.code = 'HIGH') AS "highCount"
                """ + fromSql + """
                GROUP BY re.created_on::date
                ORDER BY re.created_on::date
                """, args.toArray());

        List<Map<String, Object>> severityBreakdown = jdbc.queryForList("""
                SELECT sev.code AS "severity", COUNT(*) AS "count"
                """ + fromSql + """
                GROUP BY sev.code
                ORDER BY COUNT(*) DESC
                """, args.toArray());

        List<Map<String, Object>> statusBreakdown = jdbc.queryForList("""
                SELECT es.code AS "status", COUNT(*) AS "count"
                """ + fromSql + """
                GROUP BY es.code
                ORDER BY COUNT(*) DESC
                """, args.toArray());

        List<Map<String, Object>> stageBreakdown = jdbc.queryForList("""
                SELECT stg.code AS "stage", COUNT(*) AS "count"
                """ + fromSql + """
                GROUP BY stg.code
                ORDER BY COUNT(*) DESC
                """, args.toArray());

        Map<String, Object> summary = new LinkedHashMap<>(totals);
        summary.put("periodDays", rows.size());

        List<Map<String, Object>> breakdown = new ArrayList<>();
        breakdown.add(Map.of("dimension", "severity", "items", severityBreakdown));
        breakdown.add(Map.of("dimension", "status", "items", statusBreakdown));
        breakdown.add(Map.of("dimension", "stage", "items", stageBreakdown));

        return reportEnvelope("exception-trends", filters, summary, rows, breakdown);
    }

    private Map<String, Object> queryGatewayPerformanceReport(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE cpt.client_payment_transaction_id IS NOT NULL ");
        String fromDate = asString(filters.get("fromDate"));
        String toDate = asString(filters.get("toDate"));
        if (fromDate != null && !fromDate.isBlank()) {
            where.append(" AND cpt.created_on::date >= ?::date ");
            args.add(fromDate);
        }
        if (toDate != null && !toDate.isBlank()) {
            where.append(" AND cpt.created_on::date <= ?::date ");
            args.add(toDate);
        }
        where.append(" AND ").append(SQL_INVOICE_STATUS_IN_RECONCILIATION_SCOPE);

        String fromSql = """
                FROM client_payments.client_payment_transaction cpt
                JOIN client_payments.client_payment_intent cpi ON cpi.client_payment_intent_id = cpt.client_payment_intent_id
                JOIN transactions.invoice i ON i.invoice_id = cpi.invoice_id
                LEFT JOIN payment_gateway.lu_payment_gateway_transaction_status pgts
                    ON pgts.payment_gateway_transaction_status_id = cpt.payment_gateway_transaction_status_id
                LEFT JOIN payment_gateway.payment_gateway pgw
                    ON pgw.payment_gateway_id = pgts.payment_gateway_id
                """ + where;

        String successCondition =
                "UPPER(COALESCE(pgts.status_code, '')) IN ('SUCCESS', 'CAPTURED', 'COMPLETED', 'SUCCEEDED', 'PAID')";

        String totalsSql = "SELECT "
                + "COUNT(*) AS \"transactionCount\", "
                + "COUNT(*) FILTER (WHERE " + successCondition + ") AS \"successCount\", "
                + "COUNT(*) FILTER (WHERE NOT (" + successCondition + ")) AS \"failedCount\", "
                + "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + "), 0) AS \"totalAmount\", "
                + "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + ") FILTER (WHERE " + successCondition + "), 0) AS \"successAmount\" "
                + fromSql;
        Map<String, Object> totals = jdbc.queryForMap(totalsSql, args.toArray());

        String rowsSql = "SELECT "
                + "COALESCE(NULLIF(TRIM(pgw.name), ''), 'Unknown') AS \"gatewayName\", "
                + "CAST(COALESCE(pgw.payment_gateway_id, '00000000-0000-0000-0000-000000000000') AS text) AS \"gatewayId\", "
                + "COUNT(*) AS \"transactionCount\", "
                + "COUNT(*) FILTER (WHERE " + successCondition + ") AS \"successCount\", "
                + "COUNT(*) FILTER (WHERE NOT (" + successCondition + ")) AS \"failedCount\", "
                + "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + "), 0) AS \"totalAmount\", "
                + "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + ") FILTER (WHERE " + successCondition + "), 0) AS \"successAmount\", "
                + "COALESCE(AVG(" + CPT_AMT_MAJOR_EXPR + "), 0) AS \"avgTransactionAmount\" "
                + fromSql
                + " GROUP BY pgw.payment_gateway_id, pgw.name "
                + " ORDER BY COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + "), 0) DESC";
        List<Map<String, Object>> rows = jdbc.queryForList(rowsSql, args.toArray());

        for (Map<String, Object> row : rows) {
            long txn = ((Number) row.getOrDefault("transactionCount", 0)).longValue();
            long ok = ((Number) row.getOrDefault("successCount", 0)).longValue();
            row.put("successRatePct", reportPct(ok, txn));
            double total = num(row.get("totalAmount"));
            double successAmt = num(row.get("successAmount"));
            row.put("failedAmount", Math.max(0d, total - successAmt));
        }

        long txnTotal = ((Number) totals.getOrDefault("transactionCount", 0)).longValue();
        long successTotal = ((Number) totals.getOrDefault("successCount", 0)).longValue();
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("transactionCount", txnTotal);
        summary.put("successCount", successTotal);
        summary.put("failedCount", ((Number) totals.getOrDefault("failedCount", 0)).longValue());
        summary.put("totalAmount", num(totals.get("totalAmount")));
        summary.put("successAmount", num(totals.get("successAmount")));
        summary.put("successRatePct", reportPct(successTotal, txnTotal));
        summary.put("gatewayCount", rows.size());

        return reportEnvelope("gateway-performance", filters, summary, rows, List.of());
    }

    public String insertReportExport(String slug, String format, Map<String, Object> filters) {
        String exportCode = "REP-" + Math.abs(UUID.randomUUID().hashCode());
        jdbc.update("""
                INSERT INTO reconciliation.report_export
                (report_export_reference_code, report_slug, export_format_id, reconciliation_run_status_id, filters_json, created_on, modified_on)
                VALUES (
                    ?, ?, (SELECT export_format_id FROM reconciliation.lu_export_format WHERE code = ?),
                    (SELECT reconciliation_run_status_id FROM reconciliation.lu_reconciliation_run_status WHERE code='QUEUED'),
                    ?::jsonb, now(), now()
                )
                """, exportCode, slug, format.toUpperCase(), toJson(filters));
        return exportCode;
    }

    public void updateReportExportStatus(
            String exportId,
            String statusCode,
            String fileUrl,
            String errorMessage,
            Integer exportRowCount
    ) {
        Map<String, Object> patch = new LinkedHashMap<>();
        if (errorMessage != null && !errorMessage.isBlank()) {
            patch.put("error", errorMessage);
        }
        if (exportRowCount != null) {
            patch.put("exportRowCount", exportRowCount);
        }
        jdbc.update("""
                UPDATE reconciliation.report_export
                SET reconciliation_run_status_id = (
                        SELECT reconciliation_run_status_id
                        FROM reconciliation.lu_reconciliation_run_status
                        WHERE code = ?
                    ),
                    file_url = ?,
                    filters_json = filters_json || ?::jsonb,
                    modified_on = now()
                WHERE report_export_reference_code = ?
                """,
                statusCode,
                fileUrl,
                toJson(patch),
                exportId);
    }

    public Map<String, Object> reportExportStatus(String exportId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    re.report_export_reference_code AS "exportId",
                    re.report_slug AS "slug",
                    ef.code AS "format",
                    rs.code AS "status",
                    re.file_url AS "fileUrl",
                    re.filters_json->>'error' AS "error",
                    re.filters_json->>'exportRowCount' AS "exportRowCount",
                    re.created_on AS "createdAt",
                    re.modified_on AS "updatedAt"
                FROM reconciliation.report_export re
                JOIN reconciliation.lu_export_format ef ON ef.export_format_id = re.export_format_id
                JOIN reconciliation.lu_reconciliation_run_status rs ON rs.reconciliation_run_status_id = re.reconciliation_run_status_id
                WHERE re.report_export_reference_code = ?
                """, exportId);
        return rows.isEmpty() ? Map.of("exportId", exportId, "status", "NOT_FOUND") : rows.get(0);
    }

    public List<Map<String, Object>> listAudit(String entityType, String entityId, Integer limit, Integer page, Integer pageSize) {
        int effectiveLimit;
        int offset = 0;
        if (page != null && page > 0 && pageSize != null && pageSize > 0) {
            int cappedPageSize = Math.min(pageSize, 200);
            offset = (page - 1) * cappedPageSize;
            effectiveLimit = cappedPageSize;
        } else {
            effectiveLimit = limit == null || limit < 1 ? 500 : Math.min(limit, 2000);
        }
        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    CAST(audit_log_id AS text) AS "auditId",
                    entity_type AS "entityType",
                    CAST(entity_id AS text) AS "entityId",
                    COALESCE(entity_reference_code, CAST(entity_id AS text)) AS "entityRef",
                    action_code AS "action",
                    CAST(actor_user_id AS text) AS "actor",
                    details_json AS "details",
                    event_at AS "eventAt",
                    created_on AS "createdAt"
                FROM reconciliation.audit_log
                WHERE 1=1
                """);
        if (entityType != null && !entityType.isBlank()) {
            sql.append(" AND entity_type = ? ");
            args.add(entityType.trim());
        }
        if (entityId != null && !entityId.isBlank()) {
            sql.append(" AND (entity_reference_code = ? OR CAST(entity_id AS text) = ?) ");
            args.add(entityId.trim());
            args.add(entityId.trim());
        }
        sql.append(" ORDER BY event_at DESC LIMIT ? OFFSET ? ");
        args.add(effectiveLimit);
        args.add(offset);
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Object getConfig(String type) {
        return switch (type) {
            case "matching-rules" -> fetchConfigJson("matching_rules", "rules_json");
            case "gl-mapping" -> fetchConfigJson("gl_mapping", "mapping_json");
            case "thresholds" -> fetchConfigJson("config_threshold", "config_json");
            case "schedules" -> fetchConfigJson("config_schedule", "config_json");
            case "access" -> fetchConfigJson("config_access", "config_json");
            default -> Map.of();
        };
    }

    public Map<String, Object> findIdempotency(String tenantId, String idempotencyKey) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    CAST(idempotency_request_id AS text) AS "idempotencyRequestId",
                    request_hash AS "requestHash",
                    response_status_code AS "responseStatusCode",
                    response_body_json AS "responseBodyJson",
                    response_reference_code AS "responseReferenceCode"
                FROM reconciliation.idempotency_request
                WHERE application_id IS NOT DISTINCT FROM NULLIF(?, '')::uuid
                  AND idempotency_key = ?
                LIMIT 1
                """, tenantId, idempotencyKey);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public boolean reserveIdempotency(String tenantId, String idempotencyKey, String method, String path, String requestHash, Object requestBody) {
        int count = jdbc.update("""
                INSERT INTO reconciliation.idempotency_request
                (application_id, idempotency_key, request_method, request_path, request_hash, request_body_json, first_seen_on, last_seen_on)
                VALUES (NULLIF(?, '')::uuid, ?, ?, ?, ?, ?::jsonb, now(), now())
                ON CONFLICT (application_id, idempotency_key) DO NOTHING
                """, tenantId, idempotencyKey, method, path, requestHash, toJson(requestBody));
        return count > 0;
    }

    public void completeIdempotency(String tenantId, String idempotencyKey, int statusCode, Object responseBody, String responseReferenceCode) {
        jdbc.update("""
                UPDATE reconciliation.idempotency_request
                SET response_status_code = ?,
                    response_body_json = ?::jsonb,
                    response_reference_code = ?,
                    last_seen_on = now()
                WHERE application_id IS NOT DISTINCT FROM NULLIF(?, '')::uuid
                  AND idempotency_key = ?
                """, statusCode, toJson(responseBody), responseReferenceCode, tenantId, idempotencyKey);
    }

    public void updateConfig(String type, Object config) {
        String json = toJson(config);
        switch (type) {
            case "matching-rules" -> jdbc.update("UPDATE reconciliation.matching_rules SET rules_json=?::jsonb, modified_on=now() WHERE is_active=true", json);
            case "gl-mapping" -> jdbc.update("UPDATE reconciliation.gl_mapping SET mapping_json=?::jsonb, modified_on=now() WHERE is_active=true", json);
            case "thresholds" -> jdbc.update("UPDATE reconciliation.config_threshold SET config_json=?::jsonb, modified_on=now() WHERE is_active=true", json);
            case "schedules" -> jdbc.update("UPDATE reconciliation.config_schedule SET config_json=?::jsonb, modified_on=now() WHERE is_active=true", json);
            case "access" -> jdbc.update("UPDATE reconciliation.config_access SET config_json=?::jsonb, modified_on=now() WHERE is_active=true", json);
            default -> throw new IllegalArgumentException("Unsupported config type: " + type);
        }
    }

    private Object fetchConfigJson(String table, String column) {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT " + column + " AS cfg FROM reconciliation." + table + " WHERE is_active = true ORDER BY created_on DESC LIMIT 1");
        return rows.isEmpty() ? Map.of() : rows.get(0).get("cfg");
    }

    /**
     * Full reconciliation chain detail for a run (subscription + cash), paginated independently.
     * Uses the same invoice scope as run {@code filters_json} (dates, gateway filters on payment joins).
     */
    public Map<String, Object> getRunReconciliationDetails(
            String recoRunId,
            int subscriptionPage,
            int subscriptionPageSize,
            int cashPage,
            int cashPageSize
    ) {
        int sp = Math.max(1, subscriptionPage);
        int ss = Math.max(1, Math.min(100, subscriptionPageSize));
        int cp = Math.max(1, cashPage);
        int cs = Math.max(1, Math.min(100, cashPageSize));
        Map<String, Object> out = new HashMap<>();
        out.put("meta", Map.of(
                "schemaVersion", 1,
                "subscriptionCashClassification", "SUBSCRIPTION = invoice linked to subscription_billing_schedule; else CASH"
        ));
        if (recoRunId == null || recoRunId.isBlank()) {
            out.put("run", null);
            out.put("filters", Map.of());
            out.put("subscription", emptyDetailPage(sp, ss));
            out.put("cash", emptyDetailPage(cp, cs));
            out.put("hint", "recoRunId required");
            return out;
        }
        List<Map<String, Object>> runRows = jdbc.queryForList("""
                SELECT
                    rr.run_reference_code AS "reconciliationRunId",
                    rr.run_type AS "runType",
                    st.code AS "runStatus",
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS "financialPeriod",
                    rr.filters_json::text AS "filtersJsonText",
                    rr.summary_json::text AS "summaryJsonText",
                    rr.created_on AS "runCreatedAt",
                    rr.ended_on AS "runEndedAt"
                FROM reconciliation.reconciliation_run rr
                JOIN reconciliation.lu_reconciliation_run_status st ON st.reconciliation_run_status_id = rr.reconciliation_run_status_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                WHERE rr.run_reference_code = ?
                """, recoRunId);
        if (runRows.isEmpty()) {
            out.put("run", null);
            out.put("filters", Map.of());
            out.put("subscription", emptyDetailPage(sp, ss));
            out.put("cash", emptyDetailPage(cp, cs));
            out.put("hint", "RUN_NOT_FOUND");
            return out;
        }
        Map<String, Object> runHeader = new HashMap<>(runRows.get(0));
        Map<String, Object> filters = parseRunFiltersJson(runHeader.remove("filtersJsonText"));
        runHeader.remove("summaryJsonText");
        out.put("run", runHeader);
        out.put("filters", filters);

        List<Map<String, Object>> runScopeRows = jdbc.queryForList("""
                SELECT
                    rr.reconciliation_run_id,
                    rr.financial_period_id,
                    rr.level_id,
                    COALESCE(fp.period_code, rr.filters_json->>'financialPeriod') AS period_code
                FROM reconciliation.reconciliation_run rr
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = rr.financial_period_id
                WHERE rr.run_reference_code = ?
                """, recoRunId);
        UUID runUuid = runScopeRows.isEmpty() ? null : (UUID) runScopeRows.get(0).get("reconciliation_run_id");
        UUID runFinancialPeriodId = runScopeRows.isEmpty() ? null : (UUID) runScopeRows.get(0).get("financial_period_id");
        UUID runLevelId = runScopeRows.isEmpty() ? null : (UUID) runScopeRows.get(0).get("level_id");
        String runPeriodCode = runScopeRows.isEmpty() || runScopeRows.get(0).get("period_code") == null
                ? null : String.valueOf(runScopeRows.get(0).get("period_code"));

        List<Object> scopeArgs = new ArrayList<>();
        String invoiceScopeWhere = buildInvoiceWhere(filters, scopeArgs);
        String invoiceFrom = """
                FROM transactions.invoice i
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """;

        long subTotal = countDistinctSchedulesInScope(invoiceFrom, invoiceScopeWhere, scopeArgs);
        List<Map<String, Object>> subRoots = pageSubscriptionSchedules(
                invoiceFrom, invoiceScopeWhere, new ArrayList<>(scopeArgs), sp, ss);
        out.put("subscription", Map.of(
                "page", sp,
                "pageSize", ss,
                "totalCount", subTotal,
                "items", enrichSubscriptionChains(recoRunId, runUuid, runFinancialPeriodId, runLevelId, runPeriodCode, subRoots)
        ));

        long cashTotal = countCashInvoicesInScope(invoiceFrom, invoiceScopeWhere, scopeArgs);
        List<Map<String, Object>> cashInvoices = pageCashInvoices(
                invoiceFrom, invoiceScopeWhere, new ArrayList<>(scopeArgs), cp, cs);
        out.put("cash", Map.of(
                "page", cp,
                "pageSize", cs,
                "totalCount", cashTotal,
                "items", enrichCashChains(recoRunId, runUuid, runFinancialPeriodId, runLevelId, runPeriodCode, cashInvoices)
        ));
        out.put("glPosting", computeGlPostingMetricsPackForRun(recoRunId));
        return out;
    }

    private static Map<String, Object> emptyDetailPage(int page, int pageSize) {
        return Map.of("page", page, "pageSize", pageSize, "totalCount", 0L, "items", List.of());
    }

    private Map<String, Object> parseRunFiltersJson(Object filtersJsonText) {
        if (filtersJsonText == null) {
            return new HashMap<>();
        }
        String t = String.valueOf(filtersJsonText).trim();
        if (t.isEmpty() || "{}".equals(t)) {
            return new HashMap<>();
        }
        try {
            return objectMapper.readValue(t, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            return new HashMap<>();
        }
    }

    private long countDistinctSchedulesInScope(String invoiceFrom, String invoiceScopeWhere, List<Object> scopeArgs) {
        String sql = "SELECT COUNT(DISTINCT sbs.billing_schedule_id) " + invoiceFrom + """
                 JOIN client_subscription_billing.subscription_billing_schedule sbs ON sbs.invoice_id = i.invoice_id
                """ + invoiceScopeWhere;
        Long n = jdbc.queryForObject(sql, Long.class, scopeArgs.toArray());
        return n == null ? 0L : n;
    }

    private long countCashInvoicesInScope(String invoiceFrom, String invoiceScopeWhere, List<Object> scopeArgs) {
        String sql = "SELECT COUNT(DISTINCT i.invoice_id) " + invoiceFrom + invoiceScopeWhere
                + " AND NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ") ";
        Long n = jdbc.queryForObject(sql, Long.class, scopeArgs.toArray());
        return n == null ? 0L : n;
    }

    private List<Map<String, Object>> pageSubscriptionSchedules(
            String invoiceFrom,
            String invoiceScopeWhere,
            List<Object> args,
            int page,
            int pageSize
    ) {
        int offset = (page - 1) * pageSize;
        List<Object> qargs = new ArrayList<>(args);
        qargs.add(pageSize);
        qargs.add(offset);
        String sql = """
                SELECT DISTINCT ON (sbs.billing_schedule_id)
                    sbs.billing_schedule_id AS "billingScheduleId",
                    sbs.subscription_instance_id AS "subscriptionInstanceId",
                    sbs.subscription_plan_id AS "subscriptionPlanId",
                    sbs.invoice_id AS "invoiceId",
                    sbs.billing_date AS "billingDate",
                    sbs.billed_on AS "billedOn",
                    sbs.billing_period_start AS "billingPeriodStart",
                    sbs.billing_period_end AS "billingPeriodEnd",
                    sbs.final_amount AS "scheduleFinalAmount",
                    sbs.cycle_number AS "cycleNumber",
                    sbs.label AS "scheduleLabel",
                    sbs.period_label AS "periodLabel",
                    sbs.billing_run_id AS "scheduleBillingRunId",
                    sbs.isactive AS "scheduleIsActive",
                    bss.status_code AS "billingScheduleStatusCode",
                    bss.display_name AS "billingScheduleStatusName",
                    i.invoice_number AS "invoiceNumber",
                    i.total_amount AS "invoiceTotalAmount",
                    i.created_on AS "invoiceCreatedOn"
                """ + invoiceFrom + """
                JOIN client_subscription_billing.subscription_billing_schedule sbs ON sbs.invoice_id = i.invoice_id
                JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                """ + invoiceScopeWhere + """
                ORDER BY sbs.billing_schedule_id, i.invoice_id
                LIMIT ? OFFSET ?
                """;
        return jdbc.queryForList(sql, qargs.toArray());
    }

    private List<Map<String, Object>> pageCashInvoices(
            String invoiceFrom,
            String invoiceScopeWhere,
            List<Object> args,
            int page,
            int pageSize
    ) {
        int offset = (page - 1) * pageSize;
        List<Object> qargs = new ArrayList<>(args);
        qargs.add(pageSize);
        qargs.add(offset);
        String sql = """
                SELECT DISTINCT ON (i.invoice_id)
                    i.invoice_id AS "invoiceId",
                    i.invoice_number AS "invoiceNumber",
                    i.total_amount AS "invoiceTotalAmount",
                    i.created_on AS "invoiceCreatedOn",
                    lis.status_name AS "invoiceStatusName"
                """ + invoiceFrom + """
                LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                """ + invoiceScopeWhere + """
                  AND NOT (
                """ + SQL_INVOICE_IS_SUBSCRIPTION + """
                )
                ORDER BY i.invoice_id
                LIMIT ? OFFSET ?
                """;
        return jdbc.queryForList(sql, qargs.toArray());
    }

    private List<Map<String, Object>> enrichSubscriptionChains(
            String recoRunId,
            UUID runUuid,
            UUID runFinancialPeriodId,
            UUID runLevelId,
            String runPeriodCode,
            List<Map<String, Object>> roots
    ) {
        if (roots.isEmpty()) {
            return List.of();
        }
        List<UUID> invoiceIds = roots.stream()
                .map(r -> parseUuidLenient(r.get("invoiceId")))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        return assembleChains(recoRunId, runUuid, runFinancialPeriodId, runLevelId, runPeriodCode, roots, invoiceIds, true);
    }

    private List<Map<String, Object>> enrichCashChains(
            String recoRunId,
            UUID runUuid,
            UUID runFinancialPeriodId,
            UUID runLevelId,
            String runPeriodCode,
            List<Map<String, Object>> invoiceRows
    ) {
        if (invoiceRows.isEmpty()) {
            return List.of();
        }
        List<UUID> invoiceIds = invoiceRows.stream()
                .map(r -> parseUuidLenient(r.get("invoiceId")))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        return assembleChains(recoRunId, runUuid, runFinancialPeriodId, runLevelId, runPeriodCode, invoiceRows, invoiceIds, false);
    }

    private List<Map<String, Object>> assembleChains(
            String recoRunId,
            UUID runUuid,
            UUID runFinancialPeriodId,
            UUID runLevelId,
            String runPeriodCode,
            List<Map<String, Object>> rootRows,
            List<UUID> invoiceIds,
            boolean subscription
    ) {
        Map<String, String> invoiceNumberToId = invoiceNumberToIdKey(invoiceIds);
        Map<String, List<Map<String, Object>>> intentsByInvoice = loadIntentsGrouped(invoiceIds);
        Set<UUID> intentIds = intentsByInvoice.values().stream()
                .flatMap(List::stream)
                .map(m -> parseUuidLenient(m.get("client_payment_intent_id")))
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<Map<String, Object>>> txnsByIntent = loadTxnsGrouped(intentIds);
        Map<String, String> paymentTxnIdToInvoiceKey = paymentTxnIdToInvoiceKey(intentsByInvoice, txnsByIntent);
        Set<String> gatewayRefs = txnsByIntent.values().stream()
                .flatMap(List::stream)
                .map(m -> m.get("payment_gateway_order_id"))
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<Map<String, Object>>> historyByInvoice = loadBillingHistoryGrouped(invoiceIds);
        Map<String, List<Map<String, Object>>> refundsByInvoice = loadRefundsGrouped(recoRunId, invoiceIds, invoiceNumberToId);
        Map<String, List<Map<String, Object>>> glByInvoice = loadGlGrouped(
                runUuid, runFinancialPeriodId, runLevelId, runPeriodCode, invoiceIds, invoiceNumberToId, paymentTxnIdToInvoiceKey);
        Map<String, List<Map<String, Object>>> snapshotsByInvoice = loadSnapshotsGrouped(recoRunId, invoiceIds, invoiceNumberToId);
        Map<String, List<Map<String, Object>>> bankByGatewayRef = loadBankMatchesGrouped(gatewayRefs);

        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> root : rootRows) {
            UUID invId = parseUuidLenient(root.get("invoiceId"));
            String invKey = invId == null ? null : invId.toString();
            List<Map<String, Object>> intents = invKey == null ? List.of() : intentsByInvoice.getOrDefault(invKey, List.of());
            List<Map<String, Object>> intentPayloads = new ArrayList<>();
            for (Map<String, Object> intent : intents) {
                String ik = intent.get("client_payment_intent_id") == null ? null : String.valueOf(intent.get("client_payment_intent_id"));
                List<Map<String, Object>> txns = ik == null ? List.of() : txnsByIntent.getOrDefault(ik, List.of());
                List<Map<String, Object>> txWithGw = new ArrayList<>();
                for (Map<String, Object> t : txns) {
                    Map<String, Object> txm = new LinkedHashMap<>(t);
                    Object gw = t.get("payment_gateway_order_id");
                    String gws = gw == null ? null : String.valueOf(gw);
                    Map<String, Object> gatewayView = new LinkedHashMap<>();
                    gatewayView.put("gatewayTransactionStatusCode", t.get("gateway_transaction_status_code"));
                    gatewayView.put("gatewayTransactionStatusName", t.get("gateway_transaction_status_name"));
                    gatewayView.put("paymentGatewayName", t.get("status_payment_gateway_name"));
                    gatewayView.put("gatewayMethodTypeCode", t.get("status_gateway_method_type_code"));
                    gatewayView.put("gatewayMethodTypeName", t.get("status_gateway_method_type_name"));
                    gatewayView.put("paymentTypeCode", t.get("transaction_payment_type_code"));
                    gatewayView.put("paymentTypeName", t.get("transaction_payment_type_name"));
                    gatewayView.put("mandateGatewayReference", t.get("mandate_gateway_reference"));
                    gatewayView.put("mandateCurrency", t.get("mandate_currency"));
                    gatewayView.put("gatewayMandateStatusCode", t.get("gateway_mandate_status_code"));
                    gatewayView.put("gatewayMandateStatusName", t.get("gateway_mandate_status_name"));
                    txm.put("gateway", gatewayView);
                    txm.put("settlementBankMatches", gws == null ? List.of() : bankByGatewayRef.getOrDefault(gws, List.of()));
                    txWithGw.add(txm);
                }
                Map<String, Object> ip = new LinkedHashMap<>();
                ip.put("intent", intent);
                ip.put("paymentTransactions", txWithGw);
                intentPayloads.add(ip);
            }
            Map<String, Object> invoiceDetail = new LinkedHashMap<>();
            if (invId != null) {
                List<Map<String, Object>> invFull = jdbc.queryForList("""
                        SELECT
                            i.invoice_id AS "invoiceId",
                            i.invoice_number AS "invoiceNumber",
                            i.invoice_date AS "invoiceDate",
                            i.total_amount AS "totalAmount",
                            i.sub_total AS "subTotal",
                            i.tax_amount AS "taxAmount",
                            i.discount_amount AS "discountAmount",
                            i.is_paid AS "isPaid",
                            i.created_on AS "createdOn",
                            i.modified_on AS "modifiedOn",
                            lis.status_name AS "statusName"
                        FROM transactions.invoice i
                        LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                        WHERE i.invoice_id = ?
                        """, invId);
                if (!invFull.isEmpty()) {
                    invoiceDetail.putAll(invFull.get(0));
                }
            }
            Map<String, Object> chain = new LinkedHashMap<>();
            chain.put("reconciliationType", subscription ? "SUBSCRIPTION" : "CASH");
            chain.put("recoRunId", recoRunId);
            chain.put("billingSchedule", subscription ? root : null);
            chain.put("invoice", invoiceDetail.isEmpty() ? root : invoiceDetail);
            chain.put("paymentIntents", intentPayloads);
            chain.put("subscriptionBillingHistory", invKey == null ? List.of() : historyByInvoice.getOrDefault(invKey, List.of()));
            chain.put("refunds", invKey == null ? List.of() : refundsByInvoice.getOrDefault(invKey, List.of()));
            List<Map<String, Object>> glEntries = invKey == null ? List.of() : glByInvoice.getOrDefault(invKey, List.of());
            chain.put("glPosting", Map.of("entries", glEntries));
            chain.put("amountReconciliation", buildInvoiceAmountReconciliation(invoiceDetail, intentPayloads));
            chain.put("runSnapshots", invKey == null ? List.of() : snapshotsByInvoice.getOrDefault(invKey, List.of()));
            chain.put("stages", buildStageSummaries(subscription, root, invoiceDetail, intentPayloads,
                    invKey == null ? List.of() : refundsByInvoice.getOrDefault(invKey, List.of()),
                    invKey == null ? List.of() : glByInvoice.getOrDefault(invKey, List.of())));
            out.add(chain);
        }
        return out;
    }

    private List<Map<String, Object>> buildStageSummaries(
            boolean subscription,
            Map<String, Object> scheduleOrInvoiceRoot,
            Map<String, Object> invoice,
            List<Map<String, Object>> intentPayloads,
            List<Map<String, Object>> refunds,
            List<Map<String, Object>> glEntries
    ) {
        List<Map<String, Object>> stages = new ArrayList<>();
        if (subscription) {
            stages.add(Map.of("stageKey", "BILLING_SCHEDULE", "title", "Billing Schedule", "detail", scheduleOrInvoiceRoot));
        }
        stages.add(Map.of("stageKey", "INVOICE", "title", "Invoice", "detail", invoice));
        stages.add(Map.of("stageKey", "PAYMENT_INTENT", "title", "Payment Intent", "detail", intentPayloads.stream().map(m -> m.get("intent")).toList()));
        stages.add(Map.of("stageKey", "PAYMENT_TRANSACTION", "title", "Payment Transaction",
                "detail", intentPayloads.stream()
                        .flatMap(m -> ((List<?>) m.getOrDefault("paymentTransactions", List.of())).stream())
                        .toList()));
        stages.add(Map.of("stageKey", "GATEWAY", "title", "Gateway",
                "detail", intentPayloads.stream()
                        .flatMap(m -> ((List<?>) m.getOrDefault("paymentTransactions", List.of())).stream())
                        .map(o -> ((Map<?, ?>) o).get("gateway"))
                        .toList()));
        stages.add(Map.of("stageKey", "SETTLEMENT_BANK", "title", "Settlement (Bank)",
                "detail", settlementBankMatchesFromIntents(intentPayloads)));
        stages.add(Map.of("stageKey", "REFUND", "title", "Refunds", "detail", refunds));
        stages.add(Map.of("stageKey", "GL_POSTING", "title", "GL Posting", "detail", glEntries));
        return stages;
    }

    private Map<String, String> invoiceNumberToIdKey(List<UUID> invoiceIds) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT invoice_id::text AS kid, invoice_number::text AS num FROM transactions.invoice WHERE invoice_id IN (" + inList + ")");
        Map<String, String> numberToId = new HashMap<>();
        for (Map<String, Object> r : rows) {
            Object n = r.get("num");
            if (n != null && r.get("kid") != null) {
                numberToId.put(String.valueOf(n), String.valueOf(r.get("kid")));
            }
        }
        return numberToId;
    }

    private Map<String, List<Map<String, Object>>> loadIntentsGrouped(List<UUID> invoiceIds) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT cpi.*, "
                        + "pis.payment_intent_status AS intent_status_text, "
                        + "pg.\"name\" AS payment_gateway_name, "
                        + "pgct.currency_code AS payment_currency_code, "
                        + "pgct.currency_name AS payment_currency_name, "
                        + "lmt.method_type_code AS payment_method_type_code, "
                        + "lmt.method_type_name AS payment_method_type_name, "
                        + "iptt.payment_type_code AS intent_payment_type_code, "
                        + "iptt.payment_type_name AS intent_payment_type_name "
                        + "FROM client_payments.client_payment_intent cpi "
                        + "LEFT JOIN client_payments.lu_payment_intent_status pis "
                        + "ON pis.payment_intent_status_id = cpi.intent_status_id "
                        + "LEFT JOIN client_payments.client_payment_method cpm "
                        + "ON cpm.client_payment_method_id = cpi.client_payment_method_id "
                        + "LEFT JOIN payment_gateway.payment_gateway pg ON pg.payment_gateway_id = cpm.payment_gateway_id "
                        + "LEFT JOIN payment_gateway.payment_gateway_supported_currency pgsc "
                        + "ON pgsc.payment_gateway_supported_currency_id = cpm.payment_gateway_currency_id "
                        + "LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pgct "
                        + "ON pgct.payment_gateway_currency_type_id = pgsc.payment_gateway_currency_type_id "
                        + "LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm "
                        + "ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id "
                        + "LEFT JOIN payment_gateway.lu_payment_gateway_method_type lmt "
                        + "ON lmt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id "
                        + "LEFT JOIN payment_gateway.payment_gateway_supported_payment_type ipt "
                        + "ON ipt.payment_gateway_supported_payment_type_id = cpi.payment_type_id "
                        + "LEFT JOIN payment_gateway.lu_payment_gateway_payment_type iptt "
                        + "ON iptt.payment_gateway_payment_type_id = ipt.payment_gateway_payment_type_id "
                        + "WHERE cpi.invoice_id IN (" + inList + ") "
                        + "ORDER BY cpi.created_on NULLS LAST, cpi.client_payment_intent_id");
        return rows.stream().collect(Collectors.groupingBy(m -> String.valueOf(m.get("invoice_id")), LinkedHashMap::new, Collectors.toList()));
    }

    private Map<String, List<Map<String, Object>>> loadTxnsGrouped(Set<UUID> intentIds) {
        if (intentIds.isEmpty()) {
            return Map.of();
        }
        String inList = intentIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        String sql = "SELECT cpt.*, "
                + "pgts.status_code AS gateway_transaction_status_code, "
                + "pgts.description AS gateway_transaction_status_name, "
                + "pgw.\"name\" AS status_payment_gateway_name, "
                + "pgmt.method_type_code AS status_gateway_method_type_code, "
                + "pgmt.method_type_name AS status_gateway_method_type_name, "
                + "gpt.payment_type_code AS transaction_payment_type_code, "
                + "gpt.payment_type_name AS transaction_payment_type_name, "
                + "cgm.gateway_mandate_id AS mandate_gateway_reference, "
                + "cgm.mandate_currency AS mandate_currency, "
                + "gms.code AS gateway_mandate_status_code, "
                + "gms.\"name\" AS gateway_mandate_status_name "
                + "FROM client_payments.client_payment_transaction cpt "
                + "LEFT JOIN payment_gateway.lu_payment_gateway_transaction_status pgts "
                + "ON pgts.payment_gateway_transaction_status_id = cpt.payment_gateway_transaction_status_id "
                + "LEFT JOIN payment_gateway.payment_gateway pgw "
                + "ON pgw.payment_gateway_id = pgts.payment_gateway_id "
                + "LEFT JOIN payment_gateway.lu_payment_gateway_method_type pgmt "
                + "ON pgmt.payment_gateway_method_type_id = pgts.payment_gateway_payment_method_type_id "
                + "LEFT JOIN payment_gateway.payment_gateway_supported_payment_type pgpt "
                + "ON pgpt.payment_gateway_supported_payment_type_id = cpt.payment_type_id "
                + "LEFT JOIN payment_gateway.lu_payment_gateway_payment_type gpt "
                + "ON gpt.payment_gateway_payment_type_id = pgpt.payment_gateway_payment_type_id "
                + "LEFT JOIN client_payments.client_gateway_mandate cgm "
                + "ON cgm.client_gateway_mandate_id = cpt.client_gateway_mandate_id "
                + "LEFT JOIN client_payments.lu_gateway_mandate_status gms "
                + "ON gms.gateway_mandate_status_id = cgm.mandate_status_id "
                + "WHERE cpt.client_payment_intent_id IN (" + inList + ") "
                + "ORDER BY cpt.created_on NULLS LAST, cpt.client_payment_transaction_id";
        List<Map<String, Object>> rows = jdbc.queryForList(sql);
        return rows.stream().collect(Collectors.groupingBy(m -> String.valueOf(m.get("client_payment_intent_id")), LinkedHashMap::new, Collectors.toList()));
    }

    private Map<String, List<Map<String, Object>>> loadBillingHistoryGrouped(List<UUID> invoiceIds) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        String sql = "SELECT sbh.*, bs.status_code AS billing_status_code, bs.display_name AS billing_status_display "
                + "FROM client_subscription_billing.subscription_billing_history sbh "
                + "LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                + "WHERE sbh.invoice_id IN (" + inList + ") "
                + "ORDER BY sbh.billing_attempt_on DESC NULLS LAST, sbh.created_on DESC";
        List<Map<String, Object>> rows = jdbc.queryForList(sql);
        for (Map<String, Object> r : rows) {
            normalizeJsonColumn(r, "mock_charge_details");
        }
        return rows.stream().collect(Collectors.groupingBy(m -> String.valueOf(m.get("invoice_id")), LinkedHashMap::new, Collectors.toList()));
    }

    private Map<String, List<Map<String, Object>>> loadRefundsGrouped(
            String recoRunId,
            List<UUID> invoiceIds,
            Map<String, String> invoiceNumberToId
    ) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        String sql = "SELECT "
                + "rt.refund_reference_code AS \"refundId\", "
                + "rt.invoice_id AS invoice_id, "
                + "rt.invoice_reference_code AS \"invoiceReferenceCode\", "
                + "rt.expected_amount AS \"expectedAmount\", "
                + "rt.actual_amount AS \"actualAmount\", "
                + "rs.code AS \"status\", "
                + "rt.gateway_reference AS \"gatewayReference\", "
                + "rt.reason_code AS \"reasonCode\", "
                + "rt.reason_text AS \"reasonText\", "
                + "rt.refund_requested_at AS \"refundRequestedAt\", "
                + "rt.refund_processed_at AS \"refundProcessedAt\", "
                + "rt.created_on AS \"createdAt\" "
                + "FROM reconciliation.refund_transaction rt "
                + "JOIN reconciliation.lu_reconciliation_status rs ON rs.reconciliation_status_id = rt.reconciliation_status_id "
                + "WHERE rt.reconciliation_run_id = (SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?) "
                + "AND (rt.invoice_id IN (" + inList + ") "
                + "OR rt.invoice_reference_code IN (SELECT invoice_number FROM transactions.invoice WHERE invoice_id IN (" + inList + ")))";
        List<Map<String, Object>> rows = jdbc.queryForList(sql, recoRunId);
        Map<String, List<Map<String, Object>>> byInv = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            Object iid = r.get("invoice_id");
            String key = iid == null ? null : String.valueOf(iid);
            if (key == null || "null".equals(key)) {
                String ref = r.get("invoiceReferenceCode") == null ? null : String.valueOf(r.get("invoiceReferenceCode"));
                if (ref != null) {
                    key = invoiceNumberToId.get(ref);
                }
            }
            if (key != null) {
                byInv.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }
        return byInv;
    }

    private Map<String, String> paymentTxnIdToInvoiceKey(
            Map<String, List<Map<String, Object>>> intentsByInvoice,
            Map<String, List<Map<String, Object>>> txnsByIntent
    ) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> invEntry : intentsByInvoice.entrySet()) {
            String invKey = invEntry.getKey();
            for (Map<String, Object> intent : invEntry.getValue()) {
                Object intentId = intent.get("client_payment_intent_id");
                if (intentId == null) {
                    continue;
                }
                String ik = String.valueOf(intentId);
                for (Map<String, Object> txn : txnsByIntent.getOrDefault(ik, List.of())) {
                    Object txnId = txn.get("client_payment_transaction_id");
                    if (txnId != null) {
                        map.put(String.valueOf(txnId), invKey);
                    }
                }
            }
        }
        return map;
    }

    private String resolveGlEntryInvoiceKey(
            Map<String, Object> row,
            Map<String, String> invoiceNumberToId,
            Map<String, String> paymentTxnIdToInvoiceKey
    ) {
        Object eid = row.get("entityUuid");
        if (eid != null) {
            return String.valueOf(eid);
        }
        String erc = row.get("entityReferenceCode") == null ? null : String.valueOf(row.get("entityReferenceCode"));
        if (erc == null || erc.isBlank()) {
            return null;
        }
        String byNumber = invoiceNumberToId.get(erc);
        if (byNumber != null) {
            return byNumber;
        }
        String byTxn = paymentTxnIdToInvoiceKey.get(erc);
        if (byTxn != null) {
            return byTxn;
        }
        if (erc.startsWith("PAY-CPT-")) {
            String core = erc.replaceFirst("^PAY-CPT-", "").replaceFirst("-(DR|CR)$", "");
            return paymentTxnIdToInvoiceKey.get(core);
        }
        return null;
    }

    private Map<String, List<Map<String, Object>>> loadGlGrouped(
            UUID runUuid,
            UUID runFinancialPeriodId,
            UUID runLevelId,
            String runPeriodCode,
            List<UUID> invoiceIds,
            Map<String, String> invoiceNumberToId,
            Map<String, String> paymentTxnIdToInvoiceKey
    ) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        List<Object> args = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");
        appendGlEntryScopeWhere(where, args, runUuid, runFinancialPeriodId, runLevelId, runPeriodCode);
        where.append("""
                 AND (
                    ge.entity_id IN (%s)
                    OR ge.entity_reference_code IN (
                        SELECT invoice_number FROM transactions.invoice WHERE invoice_id IN (%s)
                    )
                    OR ge.entity_reference_code IN (
                        SELECT cpt.client_payment_transaction_id::text
                        FROM client_payments.client_payment_transaction cpt
                        JOIN client_payments.client_payment_intent cpi
                          ON cpi.client_payment_intent_id = cpt.client_payment_intent_id
                        WHERE cpi.invoice_id IN (%s)
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM client_payments.client_payment_transaction cpt
                        JOIN client_payments.client_payment_intent cpi
                          ON cpi.client_payment_intent_id = cpt.client_payment_intent_id
                        WHERE cpi.invoice_id IN (%s)
                          AND (
                            ge.entity_reference_code = cpt.client_payment_transaction_id::text
                            OR ge.entity_reference_code LIKE 'PAY-CPT-' || cpt.client_payment_transaction_id::text || '-%%'
                          )
                    )
                 )
                """.formatted(inList, inList, inList, inList));

        String sql = """
                SELECT
                    ge.gl_entry_reference_code AS "journalId",
                    CAST(ge.gl_entry_id AS text) AS "glEntryId",
                    ge.entity_reference_code AS "entityReferenceCode",
                    CAST(ge.entity_id AS text) AS "entityUuid",
                    et.code AS "entityType",
                    gc.gl_code AS "glAccountCode",
                    gc.description AS "glAccountName",
                    CAST(ge.gl_mapping_rule_id AS text) AS "glMappingRuleId",
                    gmr.mapping_code AS "glMappingRuleCode",
                    gmr.mapping_name AS "glMappingRuleName",
                    jt.code AS "journalType",
                    pcur.currency_code AS "currencyCode",
                    ge.debit_amount AS "debitAmount",
                    ge.credit_amount AS "creditAmount",
                    COALESCE(gps.code, 'UNKNOWN') AS "postingStatus",
                    ge.posting_reference AS "postingReference",
                    ge.posted_at AS "postedAt",
                    ge.created_on AS "createdOn"
                FROM reconciliation.gl_entry ge
                INNER JOIN finance.gl_code gc ON gc.gl_code_id = ge.gl_code_id
                LEFT JOIN reconciliation.lu_gl_posting_status gps ON gps.gl_posting_status_id = ge.gl_posting_status_id
                LEFT JOIN reconciliation.lu_reconciliation_entity_type et ON et.reconciliation_entity_type_id = ge.entity_type_id
                LEFT JOIN reconciliation.gl_mapping_rule gmr ON gmr.gl_mapping_rule_id = ge.gl_mapping_rule_id
                LEFT JOIN reconciliation.lu_journal_type jt ON jt.journal_type_id = ge.journal_type_id
                LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur
                  ON pcur.payment_gateway_currency_type_id = ge.payment_currency_type_id
                """ + where;
        List<Map<String, Object>> rows = jdbc.queryForList(sql, args.toArray());
        Map<String, List<Map<String, Object>>> byInv = new LinkedHashMap<>();
        for (UUID id : invoiceIds) {
            byInv.put(id.toString(), new ArrayList<>());
        }
        for (Map<String, Object> r : rows) {
            String key = resolveGlEntryInvoiceKey(r, invoiceNumberToId, paymentTxnIdToInvoiceKey);
            r.remove("entityUuid");
            if (key != null) {
                byInv.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }
        return byInv;
    }

    private Map<String, List<Map<String, Object>>> loadSnapshotsGrouped(
            String recoRunId,
            List<UUID> invoiceIds,
            Map<String, String> invoiceNumberToId
    ) {
        if (invoiceIds.isEmpty()) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        String sql = "SELECT "
                + "s.snapshot_reference_code AS \"snapshotId\", "
                + "s.entity_id::text AS entity_id, "
                + "s.entity_reference_code AS \"entityReferenceCode\", "
                + "COALESCE(s.expected_amount, 0) AS \"expectedAmount\", "
                + "COALESCE(s.actual_amount, 0) AS \"actualAmount\", "
                + "rcs.code AS \"reconciliationStatus\", "
                + "sev.code AS \"severity\", "
                + "lr.code AS \"reasonCode\", "
                + "s.reason_text AS \"reasonText\", "
                + "s.attributes_json AS \"attributesJson\", "
                + "s.event_at AS \"eventAt\" "
                + "FROM reconciliation.reconciliation_run_snapshot s "
                + "JOIN reconciliation.reconciliation_run rr ON rr.reconciliation_run_id = s.reconciliation_run_id "
                + "JOIN reconciliation.lu_reconciliation_status rcs ON rcs.reconciliation_status_id = s.reconciliation_status_id "
                + "LEFT JOIN reconciliation.lu_severity sev ON sev.severity_id = s.severity_id "
                + "LEFT JOIN reconciliation.lu_reconciliation_entity_type et ON et.reconciliation_entity_type_id = s.reconciliation_entity_type_id "
                + "LEFT JOIN reconciliation.lu_reconciliation_reason lr ON lr.reconciliation_reason_id = s.reconciliation_reason_id "
                + "WHERE rr.run_reference_code = ? "
                + "AND et.code = 'INVOICE' "
                + "AND (s.entity_id IN (" + inList + ") "
                + "OR s.entity_reference_code IN (SELECT invoice_number FROM transactions.invoice WHERE invoice_id IN (" + inList + ")))";
        List<Map<String, Object>> rows = jdbc.queryForList(sql, recoRunId);
        Map<String, List<Map<String, Object>>> byInv = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            normalizeJsonColumn(r, "attributesJson");
            Object eid = r.get("entity_id");
            String key = eid == null ? null : String.valueOf(eid);
            if (key == null || "null".equals(key)) {
                String erc = r.get("entityReferenceCode") == null ? null : String.valueOf(r.get("entityReferenceCode"));
                if (erc != null) {
                    key = invoiceNumberToId.get(erc);
                }
            }
            if (key != null) {
                byInv.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }
        for (List<Map<String, Object>> lst : byInv.values()) {
            lst.sort(Comparator.comparing(m -> String.valueOf(m.get("eventAt")), Comparator.nullsLast(String::compareTo)));
        }
        return byInv;
    }

    /**
     * JDBC returns {@code jsonb} as driver-specific objects; Jackson would otherwise emit {@code {type,value,null}}.
     */
    private void normalizeJsonColumn(Map<String, Object> row, String key) {
        if (row == null || key == null || !row.containsKey(key)) {
            return;
        }
        row.put(key, jsonValueToJava(row.get(key)));
    }

    private static boolean isPostgresJsonWrapper(Object raw) {
        return raw != null && "org.postgresql.util.PGobject".equals(raw.getClass().getName());
    }

    private Object jsonValueToJava(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Map<?, ?> || raw instanceof List<?>) {
            return raw;
        }
        if (isPostgresJsonWrapper(raw)) {
            try {
                String t = (String) raw.getClass().getMethod("getType").invoke(raw);
                String v = (String) raw.getClass().getMethod("getValue").invoke(raw);
                if (v == null || v.isBlank()) {
                    return null;
                }
                if ("json".equals(t) || "jsonb".equals(t)) {
                    try {
                        return objectMapper.readValue(v, new TypeReference<>() {
                        });
                    } catch (JsonProcessingException e) {
                        return v;
                    }
                }
                return v;
            } catch (ReflectiveOperationException e) {
                return String.valueOf(raw);
            }
        }
        if (raw instanceof String s) {
            if (s.isBlank()) {
                return null;
            }
            try {
                return objectMapper.readValue(s, new TypeReference<>() {
                });
            } catch (JsonProcessingException e) {
                return s;
            }
        }
        return raw;
    }

    private Map<String, List<Map<String, Object>>> loadBankMatchesGrouped(Set<String> gatewayRefs) {
        if (gatewayRefs.isEmpty()) {
            return Map.of();
        }
        Map<String, List<Map<String, Object>>> out = new LinkedHashMap<>();
        for (String ref : gatewayRefs) {
            List<Map<String, Object>> rows = jdbc.queryForList("""
                    SELECT
                        brm.bank_reconciliation_match_id::text AS "matchId",
                        br.bank_reconciliation_ref_code AS "bankReconcileRunId",
                        brm.bank_entry_reference AS "bankEntryReference",
                        brm.system_transaction_reference AS "systemTransactionReference",
                        brm.variance_amount AS "varianceAmount",
                        bmt.code AS "matchType",
                        brm.notes AS "notes",
                        brm.created_on AS "createdAt"
                    FROM reconciliation.bank_reconciliation_match brm
                    JOIN reconciliation.bank_reconciliation br ON br.bank_reconciliation_id = brm.bank_reconciliation_id
                    JOIN reconciliation.lu_bank_match_type bmt ON bmt.bank_match_type_id = brm.bank_match_type_id
                    WHERE brm.system_transaction_reference = ?
                    ORDER BY brm.created_on DESC
                    LIMIT 50
                    """, ref);
            out.put(ref, rows);
        }
        return out;
    }

    private static UUID parseUuidLenient(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof UUID u) {
            return u;
        }
        try {
            return UUID.fromString(String.valueOf(o));
        } catch (Exception e) {
            return null;
        }
    }

    private String buildInvoiceWhere(Map<String, Object> filters, List<Object> args) {
        StringBuilder sb = new StringBuilder(" WHERE 1=1 ");
        String fromDate = asString(filters.get("fromDate"));
        String toDate = asString(filters.get("toDate"));
        if (fromDate != null && !fromDate.isBlank()) {
            sb.append(" AND i.created_on::date >= ?::date ");
            args.add(fromDate);
        }
        if (toDate != null && !toDate.isBlank()) {
            sb.append(" AND i.created_on::date <= ?::date ");
            args.add(toDate);
        }
        List<String> gatewayIds = listOf(filters.get("gatewayIds"));
        if (!gatewayIds.isEmpty()) {
            sb.append(" AND COALESCE(cpt.payment_gateway_order_id, '') IN (");
            appendPlaceholders(sb, gatewayIds.size());
            sb.append(") ");
            args.addAll(gatewayIds);
        }
        sb.append(" AND ").append(SQL_INVOICE_STATUS_IN_RECONCILIATION_SCOPE);
        return sb.toString();
    }

    private void appendPlaceholders(StringBuilder sb, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
    }

    private List<String> listOf(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).filter(s -> !s.isBlank()).toList();
        }
        return List.of(String.valueOf(value));
    }

    private List<String> upperList(Object value) {
        return listOf(value).stream().map(v -> v.toUpperCase().trim()).toList();
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to convert payload to JSON", e);
        }
    }
}
