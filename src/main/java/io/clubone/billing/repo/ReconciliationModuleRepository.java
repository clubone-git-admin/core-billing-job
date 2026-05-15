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
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    i.invoice_id, i.invoice_number, i.total_amount AS invoice_amount, lis.status_name AS invoice_status, i.created_on AS invoice_created_on,
                    (SELECT cpi_one.client_payment_intent_id FROM client_payments.client_payment_intent cpi_one
                     WHERE cpi_one.invoice_id = i.invoice_id ORDER BY cpi_one.client_payment_intent_id DESC LIMIT 1) AS client_payment_intent_id,
                    (SELECT COALESCE(cpi_one.amount, 0)::numeric / 100.0
                     FROM client_payments.client_payment_intent cpi_one
                     WHERE cpi_one.invoice_id = i.invoice_id ORDER BY cpi_one.client_payment_intent_id DESC LIMIT 1) AS intent_amount,
                    (SELECT cpt_one.client_payment_transaction_id
                     FROM client_payments.client_payment_intent cpi_one
                     JOIN client_payments.client_payment_transaction cpt_one ON cpt_one.client_payment_intent_id = cpi_one.client_payment_intent_id
                     WHERE cpi_one.invoice_id = i.invoice_id
                     ORDER BY cpt_one.client_payment_transaction_id DESC LIMIT 1) AS client_payment_transaction_id,
                """ + "(" + INVOICE_CAPTURED_PAID_MAJOR_EXPR.strip() + ") AS txn_amount,\n" + """
                    (SELECT cpt_one.payment_gateway_order_id
                     FROM client_payments.client_payment_intent cpi_one
                     JOIN client_payments.client_payment_transaction cpt_one ON cpt_one.client_payment_intent_id = cpi_one.client_payment_intent_id
                     WHERE cpi_one.invoice_id = i.invoice_id
                     ORDER BY cpt_one.client_payment_transaction_id DESC LIMIT 1) AS payment_gateway_order_id
                FROM transactions.invoice i
                LEFT JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                WHERE i.invoice_number = ? OR CAST(i.invoice_id AS text) = ?
                LIMIT 1
                """, entityId, entityId);
        if (rows.isEmpty()) {
            Map<String, Object> notFound = new HashMap<>();
            Map<String, Object> lookup = new HashMap<>();
            lookup.put("entityId", entityId);
            lookup.put("entityType", entityType == null ? "INVOICE" : entityType);
            notFound.put("lookup", lookup);
            notFound.put("reconciliationStatus", "NOT_FOUND");
            notFound.put("mismatches", List.of());
            notFound.put("snapshotSync", Map.of("performed", false));
            return notFound;
        }
        Map<String, Object> row = rows.get(0);
        Map<String, Object> out = buildWorkspacePayload(row, entityId, entityType);
        if (recoRunId != null && !recoRunId.isBlank()) {
            Map<String, Object> snap = loadRunSnapshotForEntity(recoRunId, entityId);
            out.put("runSnapshot", snap);
            if (recompute) {
                out.put("snapshotSync", syncRunSnapshotWithLiveIfChanged(recoRunId, tenantId, row, snap));
            } else {
                out.put("snapshotSync", Map.of("performed", false, "reason", "recompute_disabled"));
            }
        } else {
            out.put("runSnapshot", Map.of());
            out.put("snapshotSync", Map.of("performed", false, "reason", "no_reco_run_id"));
        }
        return out;
    }

    private Map<String, Object> buildWorkspacePayload(Map<String, Object> row, String entityId, String entityType) {
        Map<String, Object> lookup = new HashMap<>();
        lookup.put("entityId", entityId);
        lookup.put("entityType", entityType == null ? "INVOICE" : entityType);

        Map<String, Object> invoice = new HashMap<>();
        invoice.put("invoiceId", row.get("invoice_id") != null ? String.valueOf(row.get("invoice_id")) : null);
        invoice.put("invoiceNumber", row.get("invoice_number") != null ? String.valueOf(row.get("invoice_number")) : null);
        invoice.put("invoiceAmount", row.get("invoice_amount"));
        invoice.put("status", row.get("invoice_status"));
        invoice.put("createdOn", row.get("invoice_created_on"));

        Map<String, Object> paymentIntent = new HashMap<>();
        paymentIntent.put("paymentIntentId", row.get("client_payment_intent_id") != null ? String.valueOf(row.get("client_payment_intent_id")) : null);
        paymentIntent.put("amount", row.get("intent_amount"));

        Map<String, Object> paymentTransaction = new HashMap<>();
        paymentTransaction.put("paymentTxnId", row.get("client_payment_transaction_id") != null ? String.valueOf(row.get("client_payment_transaction_id")) : null);
        paymentTransaction.put("gatewayTransactionId", row.get("payment_gateway_order_id"));
        paymentTransaction.put("amount", row.get("txn_amount"));

        String recoStatus = deriveInvoiceToPaymentStatus(row);

        Map<String, Object> out = new HashMap<>();
        out.put("lookup", lookup);
        out.put("reconciliationStatus", recoStatus);
        out.put("invoice", invoice);
        out.put("paymentIntent", paymentIntent);
        out.put("paymentTransaction", paymentTransaction);
        return out;
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
                    s.snapshot_reference_code,
                    s.expected_amount,
                    s.actual_amount,
                    rcs.code AS reco_status,
                    sev.code AS severity,
                    s.mismatch_reference_code,
                    lr.code AS reason_code,
                    s.reason_text,
                    s.event_at
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

        double snapExpected = num(snapshot.get("expected_amount"));
        double snapActual = num(snapshot.get("actual_amount"));
        String snapStatus = String.valueOf(snapshot.getOrDefault("reco_status", ""));

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
                "subscription", Map.of("kpis", buildKpiBlock(zeroAgg, "SUBSCRIPTION"), "chainStages", buildSubscriptionChainStages(zeroAgg, 0)),
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

    public List<Map<String, Object>> getWorkspaceTimeline(String entityId) {
        return jdbc.queryForList("""
                SELECT created_on AS eventAt, 'BILLING' AS source, 'INVOICE_CREATED' AS eventType, 'SUCCESS' AS status, invoice_number AS referenceId, 'Invoice generated' AS details
                FROM transactions.invoice
                WHERE invoice_number = ? OR CAST(invoice_id AS text) = ?
                ORDER BY created_on DESC LIMIT 20
                """, entityId, entityId);
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
                    NULL,
                    ?::jsonb,
                    now(), now(), now()
                )
                """, refCode, runType, triggerMode == null ? "MANUAL" : triggerMode.toUpperCase(), tenantUuid, requestedByUuid, financialPeriodId, toJson(filterMap));
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
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        Map<String, Object> totals = jdbc.queryForMap("""
                SELECT COUNT(1) AS total_invoices, COALESCE(SUM(i.total_amount), 0) AS total_expected_amount
                FROM transactions.invoice i
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where, args.toArray());
        String sumCaptured = "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + "), 0)";
        String mismatchCountExpr = "COUNT(1) FILTER (WHERE ABS(COALESCE(i.total_amount, 0)::numeric - (" + CPT_AMT_MAJOR_EXPR + ")) > 0.01)";
        String actualSql = "SELECT " + sumCaptured + " AS total_actual_amount, " + mismatchCountExpr + " AS mismatch_count "
                + "FROM transactions.invoice i "
                + "LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id "
                + "LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id "
                + where;
        Map<String, Object> actual = jdbc.queryForMap(actualSql, args.toArray());
        double expected = ((Number) totals.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actualAmount = ((Number) actual.getOrDefault("total_actual_amount", 0)).doubleValue();
        double variance = expected - actualAmount;
        int mismatchCount = ((Number) actual.getOrDefault("mismatch_count", 0)).intValue();
        int totalInvoices = ((Number) totals.getOrDefault("total_invoices", 0)).intValue();
        int reconciledCount = Math.max(0, totalInvoices - mismatchCount);

        Map<String, Object> subAgg = computeScopeInvoicePaymentAgg(filters, true);
        Map<String, Object> cashAgg = computeScopeInvoicePaymentAgg(filters, false);
        long scheduleCount = countSubscriptionBillingSchedulesInScope(filters);

        Map<String, Object> subKpis = buildKpiBlock(subAgg, "SUBSCRIPTION");
        Map<String, Object> cashKpis = buildKpiBlock(cashAgg, "CASH");
        List<Map<String, Object>> subStages = buildSubscriptionChainStages(subAgg, scheduleCount);
        List<Map<String, Object>> cashStages = buildCashChainStages(cashAgg);

        int driverLimit = parsePositiveInt(filters.get("varianceDriverLimit"), 5);
        boolean includeChart = parseBooleanDefault(filters.get("includeVarianceChart"), true);
        Map<String, Object> varianceByType = buildVarianceByReconciliationType(subAgg, cashAgg, driverLimit, includeChart);

        Map<String, Object> summary = new HashMap<>();
        summary.put("kpis", Map.of("totalInvoices", totalInvoices, "totalExpectedAmount", expected, "totalActualAmount", actualAmount,
                "totalVarianceAmount", variance, "variancePct", expected == 0 ? 0 : Math.abs(variance) * 100.0 / expected,
                "unreconciledCount", mismatchCount, "partialCount", mismatchCount, "reconciledCount", reconciledCount, "criticalExceptions", 0));
        summary.put("chainSummary", List.of(Map.of("stage", "INVOICE_TO_PAYMENT", "total", totalInvoices, "mismatch", mismatchCount,
                "mismatchPct", totalInvoices == 0 ? 0 : (mismatchCount * 100.0 / totalInvoices))));
        summary.put("varianceDrivers", List.of(Map.of("driver", "Amount mismatch", "count", mismatchCount, "amount", Math.abs(variance))));
        summary.put("statusBreakdown", List.of(Map.of("status", "RECONCILED", "count", reconciledCount), Map.of("status", "UNRECONCILED", "count", mismatchCount), Map.of("status", "PARTIAL", "count", mismatchCount)));
        summary.put("reconciliationScope", Map.of(
                "subscription", Map.of("kpis", subKpis, "chainStages", subStages),
                "cash", Map.of("kpis", cashKpis, "chainStages", cashStages)
        ));
        summary.put("varianceByReconciliationType", varianceByType);
        summary.put("meta", Map.of(
                "dashboardLayoutVersion", 2,
                "dashboardLayoutSpecVersion", "1.0",
                "varianceDriverLimit", driverLimit,
                "includeVarianceChart", includeChart,
                "subscriptionCashClassification", "SUBSCRIPTION = invoice_id in client_subscription_billing.subscription_billing_schedule; else CASH",
                "invoiceStatusScope", List.of("ISSUED", "PARTIALLY_PAID", "PAID", "DUE")
        ));
        return summary;
    }

    private Map<String, Object> computeScopeInvoicePaymentAgg(Map<String, Object> filters, boolean subscription) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        String scope = subscription ? (" AND " + SQL_INVOICE_IS_SUBSCRIPTION) : (" AND NOT (" + SQL_INVOICE_IS_SUBSCRIPTION + ")");
        Map<String, Object> totals = jdbc.queryForMap("""
                SELECT COUNT(1) AS total_invoices, COALESCE(SUM(i.total_amount), 0) AS total_expected_amount
                FROM transactions.invoice i
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + scope, args.toArray());
        String sumCapturedAgg = "COALESCE(SUM(" + CPT_AMT_MAJOR_EXPR + "), 0)";
        String mismatchCountAgg = "COUNT(1) FILTER (WHERE ABS(COALESCE(i.total_amount, 0)::numeric - (" + CPT_AMT_MAJOR_EXPR + ")) > 0.01)";
        String actSql = "SELECT " + sumCapturedAgg + " AS total_actual_amount, " + mismatchCountAgg + " AS mismatch_count, "
                + "COUNT(cpi.client_payment_intent_id) FILTER (WHERE cpi.client_payment_intent_id IS NOT NULL) AS intent_rows, "
                + "COUNT(cpt.client_payment_transaction_id) FILTER (WHERE cpt.client_payment_transaction_id IS NOT NULL) AS txn_rows "
                + "FROM transactions.invoice i "
                + "LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id "
                + "LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id "
                + where + scope;
        Map<String, Object> act = jdbc.queryForMap(actSql, args.toArray());
        Map<String, Object> out = new HashMap<>();
        out.putAll(totals);
        out.putAll(act);
        return out;
    }

    private long countSubscriptionBillingSchedulesInScope(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        String where = buildInvoiceWhere(filters, args);
        Long n = jdbc.queryForObject("""
                SELECT COUNT(DISTINCT sbs.billing_schedule_id)
                FROM transactions.invoice i
                JOIN client_subscription_billing.subscription_billing_schedule sbs ON sbs.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_intent cpi ON cpi.invoice_id = i.invoice_id
                LEFT JOIN client_payments.client_payment_transaction cpt ON cpt.client_payment_intent_id = cpi.client_payment_intent_id
                """ + where + (" AND " + SQL_INVOICE_IS_SUBSCRIPTION), Long.class, args.toArray());
        return n == null ? 0L : n;
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

    private List<Map<String, Object>> buildSubscriptionChainStages(Map<String, Object> agg, long billingScheduleCount) {
        int total = ((Number) agg.getOrDefault("total_invoices", 0)).intValue();
        int mismatch = ((Number) agg.getOrDefault("mismatch_count", 0)).intValue();
        double expected = ((Number) agg.getOrDefault("total_expected_amount", 0)).doubleValue();
        double actual = ((Number) agg.getOrDefault("total_actual_amount", 0)).doubleValue();
        int intents = ((Number) agg.getOrDefault("intent_rows", 0)).intValue();
        int txns = ((Number) agg.getOrDefault("txn_rows", 0)).intValue();
        double paidPct = total == 0 ? 0.0 : (total - mismatch) * 100.0 / total;
        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(chainStage("BILLING_SCHEDULE", 1, "Billing Schedule", billingScheduleCount, 0, 0, 0, 100.0));
        stages.add(chainStage("INVOICE", 2, "Invoice", total, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_INTENT", 3, "Payment Intent", intents, 0, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_TRANSACTION", 4, "Payment Transaction", txns, mismatch, expected, actual, paidPct));
        stages.add(chainStage("GATEWAY", 5, "Gateway", txns, 0, expected, actual, paidPct));
        stages.add(chainStage("SETTLEMENT_BANK", 6, "Settlement (Bank)", txns, 0, expected, actual, paidPct));
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
        double paidPct = total == 0 ? 0.0 : (total - mismatch) * 100.0 / total;
        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(chainStage("INVOICE", 1, "Invoice", total, mismatch, expected, actual, paidPct));
        stages.add(chainStage("PAYMENT_INTENT", 2, "Payment Intent", intents, 0, expected, actual, paidPct));
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
        metrics.add(metricRow("Paid %", paidPct, paidPct, emphasis));
        tile.put("metrics", metrics);
        tile.put("drillDown", Map.of("route", "WORKSPACE_LIST", "filters", Map.of("stage", stageKey)));
        return tile;
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
            return parsed;
        } catch (JsonProcessingException e) {
            return emptyFinancialHealthSummary("RUN_SUMMARY_INVALID");
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
        } catch (Exception ignored) {
            // leave legacy payload if recompute fails
        }
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
                    rr.filters_json->>'failureMessage' AS "failureMessage"
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
        Integer maxExisting = jdbc.queryForObject("""
                SELECT COALESCE(MAX(
                    CASE
                        WHEN rr.filters_json ? 'runVersion'
                             AND (rr.filters_json->>'runVersion') ~ '^[0-9]+$'
                        THEN (rr.filters_json->>'runVersion')::int
                    END
                ), 0)
                FROM reconciliation.reconciliation_run rr
                WHERE (rr.application_id IS NOT DISTINCT FROM ?)
                  AND (rr.financial_period_id IS NOT DISTINCT FROM ?)
                """, Integer.class, tenantUuid, financialPeriodId);
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

    public List<Map<String, Object>> listExceptions(Map<String, Object> filters) {
        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    re.exception_reference_code AS "exceptionId",
                    re.mismatch_reference_code AS "mismatchId",
                    COALESCE(re.entity_reference_code, CAST(re.entity_id AS text), '') AS "entityId",
                    re.title, re.description,
                    sev.code AS "severity",
                    stg.code AS "stage",
                    es.code AS "status",
                    CAST(re.assignee_user_id AS text) AS "assigneeId",
                    re.created_on AS "createdAt",
                    re.modified_on AS "updatedAt",
                    (SELECT rr_o.run_reference_code FROM reconciliation.reconciliation_run rr_o
                     WHERE rr_o.reconciliation_run_id = re.origin_reconciliation_run_id) AS "originRunId",
                    (SELECT rr_l.run_reference_code FROM reconciliation.reconciliation_run rr_l
                     WHERE rr_l.reconciliation_run_id = re.latest_reconciliation_run_id) AS "latestRunId",
                    re.occurrence_count AS "occurrenceCount",
                    re.is_auto_resolved AS "isAutoResolved",
                    (SELECT NULLIF(TRIM(s2.attributes_json->>'reconciliationType'), '')
                     FROM reconciliation.reconciliation_run_snapshot s2
                     WHERE s2.reconciliation_run_snapshot_id = re.reconciliation_run_snapshot_id LIMIT 1) AS "reconciliationType",
                    re.first_detected_at AS "firstDetectedAt",
                    re.last_seen_at AS "lastSeenAt"
                FROM reconciliation.reconciliation_exception re
                JOIN reconciliation.lu_severity sev ON sev.severity_id = re.severity_id
                JOIN reconciliation.lu_reconciliation_stage stg ON stg.reconciliation_stage_id = re.reconciliation_stage_id
                JOIN reconciliation.lu_exception_status es ON es.exception_status_id = re.exception_status_id
                WHERE 1=1
                """);
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
            args.add(recoRunId);
        }
        sql.append(" ORDER BY re.created_on DESC LIMIT 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
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
            List<Object> args = new ArrayList<>();
            args.add(request.status().toUpperCase());
            args.add(request.note());
            args.addAll(request.exceptionIds());
            return jdbc.update("""
                    UPDATE reconciliation.reconciliation_exception
                    SET exception_status_id = (SELECT exception_status_id FROM reconciliation.lu_exception_status WHERE code=?),
                        resolution_note = COALESCE(?, resolution_note),
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

    public List<Map<String, Object>> getJournal(String period, List<String> glAccountCodes, List<String> postingStatus, String entityId, String recoRunId) {
        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
                SELECT
                    ge.gl_entry_reference_code AS "journalId",
                    COALESCE(ge.entity_reference_code, CAST(ge.entity_id AS text)) AS "entityId",
                    gc.gl_code AS "glAccountCode",
                    ge.debit_amount AS "debitAmount",
                    ge.credit_amount AS "creditAmount",
                    gps.code AS "postingStatus",
                    ge.posted_at AS "postedAt"
                FROM reconciliation.gl_entry ge
                JOIN reconciliation.lu_gl_posting_status gps ON gps.gl_posting_status_id = ge.gl_posting_status_id
                LEFT JOIN finance.gl_code gc ON gc.gl_code_id = ge.gl_code_id
                LEFT JOIN reconciliation.financial_period fp ON fp.financial_period_id = ge.financial_period_id
                WHERE 1=1
                """);
        if (period != null && !period.isBlank()) {
            sql.append(" AND fp.period_code = ? ");
            args.add(period.trim());
        }
        if (recoRunId != null && !recoRunId.isBlank()) {
            sql.append("""
                     AND ge.reconciliation_run_id = (
                        SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?
                    )
                    """);
            args.add(recoRunId);
        }
        List<String> accounts = glAccountCodes == null ? List.of() : glAccountCodes.stream().filter(s -> s != null && !s.isBlank()).toList();
        if (!accounts.isEmpty()) {
            sql.append(" AND gc.gl_code IN (");
            appendPlaceholders(sql, accounts.size());
            sql.append(") ");
            args.addAll(accounts);
        }
        List<String> pstat = postingStatus == null ? List.of() : postingStatus.stream().filter(s -> s != null && !s.isBlank()).map(String::toUpperCase).toList();
        if (!pstat.isEmpty()) {
            sql.append(" AND gps.code IN (");
            appendPlaceholders(sql, pstat.size());
            sql.append(") ");
            args.addAll(pstat);
        }
        if (entityId != null && !entityId.isBlank()) {
            sql.append(" AND (ge.entity_reference_code = ? OR CAST(ge.entity_id AS text) = ?) ");
            args.add(entityId);
            args.add(entityId);
        }
        sql.append(" ORDER BY ge.created_on DESC LIMIT 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public List<Map<String, Object>> validateJournal(List<String> journalIds) {
        if (journalIds == null || journalIds.isEmpty()) {
            return List.of();
        }
        String placeholders = journalIds.stream().map(i -> "?").reduce((a, b) -> a + "," + b).orElse("?");
        return jdbc.queryForList("""
                SELECT
                    gl_entry_reference_code AS "journalId",
                    CASE WHEN COALESCE(debit_amount,0)=COALESCE(credit_amount,0) THEN 'BALANCED' ELSE 'UNBALANCED' END AS "validationStatus"
                FROM reconciliation.gl_entry
                WHERE gl_entry_reference_code IN (%s)
                """.formatted(placeholders), journalIds.toArray());
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

    public List<Map<String, Object>> reportQuery(String slug, Object filters) {
        return List.of(Map.of("slug", slug, "filters", filters, "generatedAt", OffsetDateTime.now().toString()));
    }

    public String reportExport(String slug, String format, Object filters) {
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

    public Map<String, Object> reportExportStatus(String exportId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    re.report_export_reference_code AS "exportId",
                    re.report_slug AS "slug",
                    ef.code AS "format",
                    rs.code AS "status",
                    re.file_url AS "fileUrl",
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

        UUID runUuid = jdbc.queryForObject(
                "SELECT reconciliation_run_id FROM reconciliation.reconciliation_run WHERE run_reference_code = ?",
                UUID.class, recoRunId);

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
                "items", enrichSubscriptionChains(recoRunId, runUuid, subRoots)
        ));

        long cashTotal = countCashInvoicesInScope(invoiceFrom, invoiceScopeWhere, scopeArgs);
        List<Map<String, Object>> cashInvoices = pageCashInvoices(
                invoiceFrom, invoiceScopeWhere, new ArrayList<>(scopeArgs), cp, cs);
        out.put("cash", Map.of(
                "page", cp,
                "pageSize", cs,
                "totalCount", cashTotal,
                "items", enrichCashChains(recoRunId, runUuid, cashInvoices)
        ));
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

    private List<Map<String, Object>> enrichSubscriptionChains(String recoRunId, UUID runUuid, List<Map<String, Object>> roots) {
        if (roots.isEmpty()) {
            return List.of();
        }
        List<UUID> invoiceIds = roots.stream()
                .map(r -> parseUuidLenient(r.get("invoiceId")))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        return assembleChains(recoRunId, runUuid, roots, invoiceIds, true);
    }

    private List<Map<String, Object>> enrichCashChains(String recoRunId, UUID runUuid, List<Map<String, Object>> invoiceRows) {
        if (invoiceRows.isEmpty()) {
            return List.of();
        }
        List<UUID> invoiceIds = invoiceRows.stream()
                .map(r -> parseUuidLenient(r.get("invoiceId")))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        return assembleChains(recoRunId, runUuid, invoiceRows, invoiceIds, false);
    }

    private List<Map<String, Object>> assembleChains(
            String recoRunId,
            UUID runUuid,
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
        Set<String> gatewayRefs = txnsByIntent.values().stream()
                .flatMap(List::stream)
                .map(m -> m.get("payment_gateway_order_id"))
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<Map<String, Object>>> historyByInvoice = loadBillingHistoryGrouped(invoiceIds);
        Map<String, List<Map<String, Object>>> refundsByInvoice = loadRefundsGrouped(recoRunId, invoiceIds, invoiceNumberToId);
        Map<String, List<Map<String, Object>>> glByInvoice = loadGlGrouped(runUuid, invoiceIds, invoiceNumberToId);
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
            chain.put("glPosting", Map.of("entries", invKey == null ? List.of() : glByInvoice.getOrDefault(invKey, List.of())));
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
                "detail", intentPayloads.stream()
                        .flatMap(m -> ((List<?>) m.getOrDefault("paymentTransactions", List.of())).stream())
                        .map(o -> ((Map<?, ?>) o).get("settlementBankMatches"))
                        .toList()));
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

    private Map<String, List<Map<String, Object>>> loadGlGrouped(
            UUID runUuid,
            List<UUID> invoiceIds,
            Map<String, String> invoiceNumberToId
    ) {
        if (invoiceIds.isEmpty() || runUuid == null) {
            return Map.of();
        }
        String inList = invoiceIds.stream().map(u -> "'" + u + "'").collect(Collectors.joining(","));
        String sql = "SELECT "
                + "ge.gl_entry_reference_code AS \"glEntryId\", "
                + "et.code AS \"entityType\", "
                + "ge.entity_id::text AS entity_id, "
                + "ge.entity_reference_code AS \"entityReferenceCode\", "
                + "gc.gl_code AS \"glAccountCode\", "
                + "gc.description AS \"glAccountName\", "
                + "pcur.currency_code AS \"currencyCode\", "
                + "ge.debit_amount AS \"debitAmount\", "
                + "ge.credit_amount AS \"creditAmount\", "
                + "gps.code AS \"postingStatus\", "
                + "ge.posting_reference AS \"postingReference\", "
                + "ge.posted_at AS \"postedAt\", "
                + "ge.created_on AS \"createdOn\" "
                + "FROM reconciliation.gl_entry ge "
                + "JOIN reconciliation.lu_gl_posting_status gps ON gps.gl_posting_status_id = ge.gl_posting_status_id "
                + "LEFT JOIN reconciliation.lu_reconciliation_entity_type et ON et.reconciliation_entity_type_id = ge.entity_type_id "
                + "LEFT JOIN finance.gl_code gc ON gc.gl_code_id = ge.gl_code_id "
                + "LEFT JOIN payment_gateway.lu_payment_gateway_currency_type pcur ON pcur.payment_gateway_currency_type_id = ge.payment_currency_type_id "
                + "WHERE ge.reconciliation_run_id = ?::uuid "
                + "AND (ge.entity_id IN (" + inList + ") "
                + "OR ge.entity_reference_code IN (SELECT invoice_number FROM transactions.invoice WHERE invoice_id IN (" + inList + ")))";
        List<Map<String, Object>> rows = jdbc.queryForList(sql, runUuid);
        Map<String, List<Map<String, Object>>> byInv = new LinkedHashMap<>();
        for (UUID id : invoiceIds) {
            byInv.put(id.toString(), new ArrayList<>());
        }
        for (Map<String, Object> r : rows) {
            Object eid = r.get("entity_id");
            if (eid != null && byInv.containsKey(String.valueOf(eid))) {
                byInv.get(String.valueOf(eid)).add(r);
                continue;
            }
            String erc = r.get("entityReferenceCode") == null ? null : String.valueOf(r.get("entityReferenceCode"));
            if (erc != null) {
                String k = invoiceNumberToId.get(erc);
                if (k != null) {
                    byInv.computeIfAbsent(k, x -> new ArrayList<>()).add(r);
                }
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
