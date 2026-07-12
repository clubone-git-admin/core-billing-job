package io.clubone.billing.service;

import io.clubone.billing.api.dto.BillingCompareExportResponse;
import io.clubone.billing.api.dto.BillingCompareQueryRequest;
import io.clubone.billing.api.dto.BillingCompareQueryResponse;
import io.clubone.billing.api.dto.BillingCompareSnapshotListResponse;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.api.dto.SubscriptionBillingHistoryItemDto;
import io.clubone.billing.repo.CompareRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.repo.SubscriptionBillingHistoryRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Service for compare operations.
 */
@Service
public class CompareService {

    private final CompareRepository compareRepository;
    private final StageRunRepository stageRunRepository;
    private final SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository;
    private final DuePreviewService duePreviewService;
    /** Bounded TTL cache — previous ConcurrentHashMap retained export byte[] for 24h+ and caused OOM. */
    private final Cache<String, ExportFile> exports = Caffeine.newBuilder()
            .expireAfterWrite(2, TimeUnit.HOURS)
            .maximumWeight(64L * 1024 * 1024) // ~64MB of export payload
            .weigher((String k, ExportFile v) -> Math.max(1, v.payload().length))
            .recordStats()
            .build();

    public CompareService(
            CompareRepository compareRepository,
            StageRunRepository stageRunRepository,
            SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository,
            DuePreviewService duePreviewService) {
        this.compareRepository = compareRepository;
        this.stageRunRepository = stageRunRepository;
        this.subscriptionBillingHistoryRepository = subscriptionBillingHistoryRepository;
        this.duePreviewService = duePreviewService;
    }

    public Map<String, Object> compareRuns(UUID runA, UUID runB, String compareType) {
        Map<String, Object> summaryA = compareRepository.getBillingRunSummary(runA);
        Map<String, Object> summaryB = compareRepository.getBillingRunSummary(runB);

        if (summaryA == null || summaryB == null) {
            return null;
        }

        // Calculate differences
        Integer invoiceCountDelta = ((Number) summaryB.getOrDefault("invoices_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("invoices_count", 0)).intValue();
        Double totalAmountDelta = ((Number) summaryB.getOrDefault("total_amount", 0.0)).doubleValue() -
                ((Number) summaryA.getOrDefault("total_amount", 0.0)).doubleValue();
        Integer successCountDelta = ((Number) summaryB.getOrDefault("success_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("success_count", 0)).intValue();
        Integer failureCountDelta = ((Number) summaryB.getOrDefault("failure_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("failure_count", 0)).intValue();

        Map<String, Object> differences = Map.of(
                "invoice_count_delta", invoiceCountDelta,
                "total_amount_delta", totalAmountDelta,
                "success_count_delta", successCountDelta,
                "failure_count_delta", failureCountDelta
        );

        List<Map<String, Object>> invoiceDiffs = new ArrayList<>();
        List<Map<String, Object>> onlyInA = new ArrayList<>();
        List<Map<String, Object>> onlyInB = new ArrayList<>();

        if ("full".equals(compareType)) {
            invoiceDiffs = compareRepository.getInvoiceDifferences(runA, runB);
            onlyInA = compareRepository.getOnlyInRunA(runA, runB);
            onlyInB = compareRepository.getOnlyInRunB(runA, runB);
        }

        return Map.of(
                "run_a", Map.of(
                        "billing_run_id", summaryA.get("billing_run_id"),
                        "billing_run_code", summaryA.get("billing_run_code"),
                        "due_date", summaryA.get("due_date"),
                        "summary", Map.of(
                                "invoices_count", summaryA.get("invoices_count"),
                                "total_amount", summaryA.get("total_amount"),
                                "success_count", summaryA.get("success_count"),
                                "failure_count", summaryA.get("failure_count")
                        )
                ),
                "run_b", Map.of(
                        "billing_run_id", summaryB.get("billing_run_id"),
                        "billing_run_code", summaryB.get("billing_run_code"),
                        "due_date", summaryB.get("due_date"),
                        "summary", Map.of(
                                "invoices_count", summaryB.get("invoices_count"),
                                "total_amount", summaryB.get("total_amount"),
                                "success_count", summaryB.get("success_count"),
                                "failure_count", summaryB.get("failure_count")
                        )
                ),
                "differences", differences,
                "invoice_diffs", invoiceDiffs.stream().map(this::formatInvoiceDiff).collect(Collectors.toList()),
                "only_in_a", onlyInA.stream().map(this::formatInvoiceItem).collect(Collectors.toList()),
                "only_in_b", onlyInB.stream().map(this::formatInvoiceItem).collect(Collectors.toList())
        );
    }

    public Map<String, Object> compareWithPrevious(UUID billingRunId) {
        UUID previousRunId = compareRepository.findPreviousBillingRun(billingRunId);
        if (previousRunId == null) {
            return null;
        }

        return compareRuns(previousRunId, billingRunId, "full");
    }

    private Map<String, Object> formatInvoiceDiff(Map<String, Object> diff) {
        return Map.of(
                "subscription_instance_id", diff.get("subscription_instance_id"),
                "invoice_id_a", diff.get("invoice_id_a"),
                "invoice_id_b", diff.get("invoice_id_b"),
                "total_a", diff.get("total_a"),
                "total_b", diff.get("total_b"),
                "delta_total", diff.get("delta_total"),
                "status_a", diff.get("status_a"),
                "status_b", diff.get("status_b"),
                "failure_reason_a", diff.get("failure_reason_a"),
                "failure_reason_b", diff.get("failure_reason_b")
        );
    }

    private Map<String, Object> formatInvoiceItem(Map<String, Object> item) {
        return Map.of(
                "subscription_instance_id", item.get("subscription_instance_id"),
                "invoice_id", item.get("invoice_id"),
                "total_amount", item.get("total_amount"),
                "status", item.get("status")
        );
    }

    public BillingCompareQueryResponse query(BillingCompareQueryRequest request) {
        validateRequest(request);
        int page = request.page() != null ? request.page() : 1;
        int pageSize = request.pageSize() != null ? Math.min(request.pageSize(), 500) : 200;
        String search = request.filters() != null ? request.filters().search() : null;
        boolean diffOnly = request.filters() != null && Boolean.TRUE.equals(request.filters().diffOnly());
        String severity = request.filters() != null ? request.filters().severity() : "ALL";
        String sortBy = request.sort() != null ? normalizeSortBy(request.sort().by()) : "delta_abs";
        String sortDir = request.sort() != null ? normalizeSortDir(request.sort().dir()) : "desc";

        BillingCompareQueryResponse duePreviewSnapshotResponse = tryDuePreviewSnapshotVsSnapshot(
                request, search, diffOnly, severity, sortBy, sortDir, page, pageSize);
        if (duePreviewSnapshotResponse != null) {
            return duePreviewSnapshotResponse;
        }
        BillingCompareQueryResponse duePreviewVsInvoiceResponse = tryDuePreviewSnapshotVsInvoiceGeneration(
                request, search, diffOnly, severity, sortBy, sortDir, page, pageSize);
        if (duePreviewVsInvoiceResponse != null) {
            return duePreviewVsInvoiceResponse;
        }

        UUID leftResolvedRunId = compareRepository.resolveBillingRunIdForCompare(
                request.left().stageCode(), request.left().runId());
        UUID rightResolvedRunId = compareRepository.resolveBillingRunIdForCompare(
                request.right().stageCode(), request.right().runId());
        if (leftResolvedRunId == null) {
            throw new CompareApiException(
                    "SNAPSHOT_NOT_FOUND",
                    "Left snapshot not found",
                    HttpStatus.BAD_REQUEST,
                    Map.of("left.run_id", request.left().runId(), "left.stage_code", request.left().stageCode()));
        }
        if (rightResolvedRunId == null) {
            throw new CompareApiException(
                    "SNAPSHOT_NOT_FOUND",
                    "Right snapshot not found",
                    HttpStatus.BAD_REQUEST,
                    Map.of("right.run_id", request.right().runId(), "right.stage_code", request.right().stageCode()));
        }
        BillingCompareQueryResponse.Summary summary = compareRepository.querySummary(
                request.left().stageCode(), leftResolvedRunId,
                request.right().stageCode(), rightResolvedRunId,
                search, severity);
        int totalRows = compareRepository.queryTotalRows(
                request.left().stageCode(), leftResolvedRunId,
                request.right().stageCode(), rightResolvedRunId,
                search, diffOnly, severity);
        int offset = (page - 1) * pageSize;
        List<BillingCompareQueryResponse.Row> rows = compareRepository.queryRows(
                request.left().stageCode(), leftResolvedRunId,
                request.right().stageCode(), rightResolvedRunId,
                search, diffOnly, severity, sortBy, sortDir, pageSize, offset);
        int totalPages = totalRows == 0 ? 0 : (int) Math.ceil((double) totalRows / pageSize);
        return new BillingCompareQueryResponse(summary, rows, page, pageSize, totalPages, totalRows, page < totalPages);
    }

    private BillingCompareQueryResponse tryDuePreviewSnapshotVsSnapshot(
            BillingCompareQueryRequest request,
            String search,
            boolean diffOnly,
            String severity,
            String sortBy,
            String sortDir,
            int page,
            int pageSize) {
        if (!"DUE_PREVIEW".equalsIgnoreCase(request.left().stageCode())
                || !"DUE_PREVIEW".equalsIgnoreCase(request.right().stageCode())) {
            return null;
        }
        StageRunDto leftStageRun = stageRunRepository.findById(request.left().runId());
        StageRunDto rightStageRun = stageRunRepository.findById(request.right().runId());
        if (leftStageRun == null || rightStageRun == null) {
            return null;
        }
        if (!"DUE_PREVIEW".equalsIgnoreCase(leftStageRun.stageCode())
                || !"DUE_PREVIEW".equalsIgnoreCase(rightStageRun.stageCode())) {
            return null;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> leftInvoices = (List<Map<String, Object>>) duePreviewService
                .getDuePreviewRunDetails(leftStageRun.stageRunId())
                .getOrDefault("invoices", List.of());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rightInvoices = (List<Map<String, Object>>) duePreviewService
                .getDuePreviewRunDetails(rightStageRun.stageRunId())
                .getOrDefault("invoices", List.of());

        Map<String, Map<String, Object>> leftByKey = toDuePreviewKeyedMap(leftInvoices);
        Map<String, Map<String, Object>> rightByKey = toDuePreviewKeyedMap(rightInvoices);
        LinkedHashMap<String, Map<String, Object>> allKeys = new LinkedHashMap<>();
        leftByKey.forEach((k, v) -> allKeys.put(k, v));
        rightByKey.forEach((k, v) -> allKeys.put(k, v));

        List<BillingCompareQueryResponse.Row> built = new ArrayList<>();
        for (String key : allKeys.keySet()) {
            Map<String, Object> left = leftByKey.get(key);
            Map<String, Object> right = rightByKey.get(key);
            built.add(buildDuePreviewRow(key, left, right));
        }
        List<BillingCompareQueryResponse.Row> searched = built.stream()
                .filter(r -> matchesSearchSeverity(r, search, severity))
                .collect(Collectors.toCollection(ArrayList::new));
        BillingCompareQueryResponse.Summary summary = summarize(searched);
        List<BillingCompareQueryResponse.Row> filtered = searched.stream()
                .filter(r -> !diffOnly || (r.changedFields() != null && !r.changedFields().isEmpty()))
                .collect(Collectors.toCollection(ArrayList::new));

        Comparator<BillingCompareQueryResponse.Row> comparator = comparator(sortBy, sortDir);
        filtered.sort(comparator);

        int totalRows = filtered.size();
        int offset = Math.max((page - 1) * pageSize, 0);
        int end = Math.min(offset + pageSize, totalRows);
        List<BillingCompareQueryResponse.Row> pageRows = offset >= totalRows ? List.of() : filtered.subList(offset, end);
        int totalPages = totalRows == 0 ? 0 : (int) Math.ceil((double) totalRows / pageSize);
        return new BillingCompareQueryResponse(summary, pageRows, page, pageSize, totalPages, totalRows, page < totalPages);
    }

    private BillingCompareQueryResponse tryDuePreviewSnapshotVsInvoiceGeneration(
            BillingCompareQueryRequest request,
            String search,
            boolean diffOnly,
            String severity,
            String sortBy,
            String sortDir,
            int page,
            int pageSize) {
        if (!"DUE_PREVIEW".equalsIgnoreCase(request.left().stageCode())
                || !"INVOICE_GENERATION".equalsIgnoreCase(request.right().stageCode())) {
            return null;
        }
        StageRunDto leftStageRun = stageRunRepository.findById(request.left().runId());
        if (leftStageRun == null || !"DUE_PREVIEW".equalsIgnoreCase(leftStageRun.stageCode())) {
            return null;
        }
        UUID rightBillingRunId = compareRepository.resolveBillingRunIdForCompare(
                request.right().stageCode(), request.right().runId());
        if (rightBillingRunId == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> leftInvoices = (List<Map<String, Object>>) duePreviewService
                .getDuePreviewRunDetails(leftStageRun.stageRunId())
                .getOrDefault("invoices", List.of());
        Map<String, Map<String, Object>> leftByKey = toDuePreviewKeyedMap(leftInvoices);

        Map<String, SubscriptionBillingHistoryItemDto> rightByKey = new LinkedHashMap<>();
        int limit = 5000;
        int offset = 0;
        while (true) {
            List<SubscriptionBillingHistoryItemDto> pageRows = subscriptionBillingHistoryRepository.findByBillingRunId(
                    rightBillingRunId, null, null, null, null, null, null, limit, offset);
            if (pageRows.isEmpty()) {
                break;
            }
            for (SubscriptionBillingHistoryItemDto row : pageRows) {
                String key = row.subscriptionInstanceId() != null
                        ? row.subscriptionInstanceId().toString()
                        : (row.invoiceId() != null ? row.invoiceId().toString() : null);
                if (key != null) {
                    rightByKey.put(key, row);
                }
            }
            if (pageRows.size() < limit) {
                break;
            }
            offset += limit;
        }

        LinkedHashMap<String, Boolean> allKeys = new LinkedHashMap<>();
        leftByKey.keySet().forEach(k -> allKeys.put(k, true));
        rightByKey.keySet().forEach(k -> allKeys.put(k, true));

        List<BillingCompareQueryResponse.Row> built = new ArrayList<>();
        for (String key : allKeys.keySet()) {
            Map<String, Object> left = leftByKey.get(key);
            SubscriptionBillingHistoryItemDto right = rightByKey.get(key);
            built.add(buildDuePreviewVsInvoiceRow(key, left, right));
        }
        List<BillingCompareQueryResponse.Row> searched = built.stream()
                .filter(r -> matchesSearchSeverity(r, search, severity))
                .collect(Collectors.toCollection(ArrayList::new));
        BillingCompareQueryResponse.Summary summary = summarize(searched);
        List<BillingCompareQueryResponse.Row> filtered = searched.stream()
                .filter(r -> !diffOnly || (r.changedFields() != null && !r.changedFields().isEmpty()))
                .collect(Collectors.toCollection(ArrayList::new));

        Comparator<BillingCompareQueryResponse.Row> comparator = comparator(sortBy, sortDir);
        filtered.sort(comparator);
        int totalRows = filtered.size();
        int rowOffset = Math.max((page - 1) * pageSize, 0);
        int end = Math.min(rowOffset + pageSize, totalRows);
        List<BillingCompareQueryResponse.Row> pageRows = rowOffset >= totalRows ? List.of() : filtered.subList(rowOffset, end);
        int totalPages = totalRows == 0 ? 0 : (int) Math.ceil((double) totalRows / pageSize);
        return new BillingCompareQueryResponse(summary, pageRows, page, pageSize, totalPages, totalRows, page < totalPages);
    }

    private static Map<String, Map<String, Object>> toDuePreviewKeyedMap(List<Map<String, Object>> invoices) {
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        if (invoices == null) {
            return out;
        }
        for (Map<String, Object> row : invoices) {
            String key = compareKey(row);
            if (key != null) {
                out.put(key, row);
            }
        }
        return out;
    }

    private static String compareKey(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        String subscriptionId = str(row.get("subscription_instance_id"));
        if (subscriptionId != null) {
            return subscriptionId;
        }
        String scheduleId = str(row.get("billing_schedule_id"));
        if (scheduleId != null) {
            return scheduleId;
        }
        return null;
    }

    /**
     * Status shown for a due-preview line: real CSV / projection fields only (no synthetic labels).
     */
    private static String duePreviewStatusFromRow(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        return firstNonBlank(
                str(row.get("invoice_status")),
                firstNonBlank(
                        str(row.get("eligibility_reason")),
                        firstNonBlank(
                                str(row.get("client_agreement_status")),
                                firstNonBlank(
                                        str(row.get("agreement_status")),
                                        str(row.get("subscription_instance_status_name"))))));
    }

    private static BillingCompareQueryResponse.SideSnapshot sideFromDuePreviewMap(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        return new BillingCompareQueryResponse.SideSnapshot(
                firstNonBlank(str(row.get("invoice_number")), str(row.get("invoice_id"))),
                fullName(row),
                firstNonBlank(str(row.get("role_id")), str(row.get("client_role_id"))),
                str(row.get("agreement_name")),
                firstNonBlank(str(row.get("agreement_status")), str(row.get("client_agreement_status"))),
                str(row.get("cycle_number")),
                str(row.get("location_name")),
                amount(row),
                duePreviewStatusFromRow(row)
        );
    }

    private static BillingCompareQueryResponse.SideSnapshot sideFromInvoiceDto(SubscriptionBillingHistoryItemDto r) {
        if (r == null) {
            return null;
        }
        String cycle = null;
        if (r.billingPeriodStart() != null && r.billingPeriodEnd() != null) {
            cycle = r.billingPeriodStart() + " / " + r.billingPeriodEnd();
        }
        String invStatus = firstNonBlank(r.invoiceStatus(), r.billingStatusCode());
        return new BillingCompareQueryResponse.SideSnapshot(
                firstNonBlank(r.invoiceNumber(), r.invoiceId() != null ? r.invoiceId().toString() : null),
                r.clientName(),
                firstNonBlank(r.roleId(), r.clientRoleId() != null ? r.clientRoleId().toString() : null),
                firstNonBlank(r.agreementName(), r.agreementOrPlanName()),
                r.agreementStatus(),
                cycle,
                r.locationName(),
                r.invoiceTotalAmount(),
                invStatus
        );
    }

    private static BillingCompareQueryResponse.Row buildDuePreviewRow(String key, Map<String, Object> left, Map<String, Object> right) {
        String leftStatus = left != null ? duePreviewStatusFromRow(left) : null;
        String rightStatus = right != null ? duePreviewStatusFromRow(right) : null;
        BigDecimal leftAmount = amount(left);
        BigDecimal rightAmount = amount(right);
        BigDecimal delta = rightAmount.subtract(leftAmount);

        BillingCompareQueryResponse.SideSnapshot leftS = sideFromDuePreviewMap(left);
        BillingCompareQueryResponse.SideSnapshot rightS = sideFromDuePreviewMap(right);

        String leftClient = left != null ? fullName(left) : null;
        String rightClient = right != null ? fullName(right) : null;
        String client = firstNonBlank(leftClient, rightClient);
        String agreement = firstNonBlank(str(left != null ? left.get("agreement_name") : null), str(right != null ? right.get("agreement_name") : null));
        String location = firstNonBlank(str(left != null ? left.get("location_name") : null), str(right != null ? right.get("location_name") : null));
        String subscriptionPlan = firstNonBlank(
                str(left != null ? left.get("subscription_plan_code") : null),
                firstNonBlank(str(right != null ? right.get("subscription_plan_code") : null),
                        firstNonBlank(str(left != null ? left.get("plan_name") : null),
                                firstNonBlank(str(right != null ? right.get("plan_name") : null), firstNonBlank(agreement, key)))));

        String subId = firstNonBlank(
                str(left != null ? left.get("subscription_instance_id") : null),
                str(right != null ? right.get("subscription_instance_id") : null));
        String invoiceId = firstNonBlank(
                firstNonBlank(str(left != null ? left.get("invoice_number") : null), str(left != null ? left.get("invoice_id") : null)),
                firstNonBlank(str(right != null ? right.get("invoice_number") : null), str(right != null ? right.get("invoice_id") : null)));

        List<String> changed = new ArrayList<>();
        if (!Objects.equals(leftStatus, rightStatus)) {
            changed.add("status");
        }
        if (leftAmount.subtract(rightAmount).abs().compareTo(new BigDecimal("0.0001")) > 0) {
            changed.add("amount");
        }
        if (!Objects.equals(str(leftClient), str(rightClient))) {
            changed.add("client_name");
        }
        if (!Objects.equals(str(left != null ? left.get("agreement_name") : null), str(right != null ? right.get("agreement_name") : null))) {
            changed.add("agreement_name");
        }
        if (!Objects.equals(str(left != null ? left.get("location_name") : null), str(right != null ? right.get("location_name") : null))) {
            changed.add("location_name");
        }

        String sev = severity(left != null, right != null, changed);
        return new BillingCompareQueryResponse.Row(
                subId,
                invoiceId,
                key,
                subscriptionPlan,
                subscriptionPlan,
                leftS,
                rightS,
                client,
                agreement,
                location,
                leftStatus,
                rightStatus,
                leftAmount,
                rightAmount,
                delta,
                changed,
                sev
        );
    }

    private static BillingCompareQueryResponse.Row buildDuePreviewVsInvoiceRow(
            String key,
            Map<String, Object> left,
            SubscriptionBillingHistoryItemDto right) {
        String leftStatus = left != null ? duePreviewStatusFromRow(left) : null;
        String rightStatus = right != null ? firstNonBlank(right.billingStatusCode(), right.invoiceStatus()) : null;
        BigDecimal leftAmount = amount(left);
        BigDecimal rightAmount = right != null && right.invoiceTotalAmount() != null ? right.invoiceTotalAmount() : BigDecimal.ZERO;
        BigDecimal delta = rightAmount.subtract(leftAmount);

        BillingCompareQueryResponse.SideSnapshot leftS = sideFromDuePreviewMap(left);
        BillingCompareQueryResponse.SideSnapshot rightS = sideFromInvoiceDto(right);

        String leftClient = left != null ? fullName(left) : null;
        String rightClient = right != null ? right.clientName() : null;
        String client = firstNonBlank(leftClient, rightClient);
        String agreement = firstNonBlank(
                str(left != null ? left.get("agreement_name") : null),
                firstNonBlank(right != null ? right.agreementName() : null, right != null ? right.agreementOrPlanName() : null));
        String location = firstNonBlank(str(left != null ? left.get("location_name") : null), right != null ? right.locationName() : null);
        String subscriptionPlan = firstNonBlank(
                str(left != null ? left.get("subscription_plan_code") : null),
                firstNonBlank(str(left != null ? left.get("plan_name") : null),
                        firstNonBlank(right != null ? right.agreementOrPlanName() : null, firstNonBlank(agreement, key))));

        String subId = firstNonBlank(
                str(left != null ? left.get("subscription_instance_id") : null),
                right != null && right.subscriptionInstanceId() != null ? right.subscriptionInstanceId().toString() : null);
        String invoiceId = firstNonBlank(
                firstNonBlank(str(left != null ? left.get("invoice_number") : null), str(left != null ? left.get("invoice_id") : null)),
                firstNonBlank(right != null ? right.invoiceNumber() : null,
                        right != null && right.invoiceId() != null ? right.invoiceId().toString() : null));

        List<String> changed = new ArrayList<>();
        if (!Objects.equals(leftStatus, rightStatus)) {
            changed.add("status");
        }
        if (leftAmount.subtract(rightAmount).abs().compareTo(new BigDecimal("0.0001")) > 0) {
            changed.add("amount");
        }
        if (!Objects.equals(str(leftClient), str(rightClient))) {
            changed.add("client_name");
        }
        if (!Objects.equals(str(left != null ? left.get("agreement_name") : null), str(agreement))) {
            changed.add("agreement_name");
        }
        if (!Objects.equals(str(left != null ? left.get("location_name") : null), str(location))) {
            changed.add("location_name");
        }

        String sev = severity(left != null, right != null, changed);
        return new BillingCompareQueryResponse.Row(
                subId,
                invoiceId,
                key,
                subscriptionPlan,
                subscriptionPlan,
                leftS,
                rightS,
                client,
                agreement,
                location,
                leftStatus,
                rightStatus,
                leftAmount,
                rightAmount,
                delta,
                changed,
                sev
        );
    }

    private static String severity(boolean leftPresent, boolean rightPresent, List<String> changed) {
        if (!leftPresent || !rightPresent || changed.contains("amount")) {
            return "HIGH";
        }
        if (changed.contains("client_name") || changed.contains("agreement_name") || changed.contains("location_name")) {
            return "MEDIUM";
        }
        if (changed.contains("status")) {
            return "LOW";
        }
        return "LOW";
    }

    private static boolean matchesSearchSeverity(BillingCompareQueryResponse.Row row, String search, String severity) {
        if (severity != null && !severity.isBlank() && !"ALL".equalsIgnoreCase(severity)
                && !severity.equalsIgnoreCase(row.severity())) {
            return false;
        }
        if (search == null || search.isBlank()) {
            return true;
        }
        String s = search.toLowerCase();
        return contains(row.entityKey(), s)
                || contains(row.subscriptionPlan(), s)
                || contains(row.planName(), s)
                || contains(row.subscriptionInstanceId(), s)
                || contains(row.invoiceId(), s)
                || contains(row.clientName(), s)
                || contains(row.agreementName(), s)
                || contains(row.locationName(), s)
                || sideMatchesSearch(row.left(), s)
                || sideMatchesSearch(row.right(), s);
    }

    private static boolean sideMatchesSearch(BillingCompareQueryResponse.SideSnapshot side, String s) {
        if (side == null) {
            return false;
        }
        return contains(side.clientName(), s)
                || contains(side.clientRole(), s)
                || contains(side.agreementName(), s)
                || contains(side.agreementStatus(), s)
                || contains(side.cycleNo(), s)
                || contains(side.locationName(), s)
                || contains(side.invoiceStatus(), s);
    }

    private static Comparator<BillingCompareQueryResponse.Row> comparator(String sortBy, String sortDir) {
        Comparator<BillingCompareQueryResponse.Row> c;
        if ("entity_key".equals(sortBy)) {
            c = Comparator.comparing(r -> safe(r.entityKey()), String.CASE_INSENSITIVE_ORDER);
        } else if ("subscription_plan".equals(sortBy) || "plan_name".equals(sortBy)) {
            c = Comparator.comparing((BillingCompareQueryResponse.Row r) ->
                            safe(firstNonBlank(r.subscriptionPlan(), r.planName())),
                    String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(r -> safe(r.entityKey()), String.CASE_INSENSITIVE_ORDER);
        } else if ("delta_amount".equals(sortBy)) {
            c = Comparator.comparing(BillingCompareQueryResponse.Row::deltaAmount, Comparator.nullsFirst(BigDecimal::compareTo))
                    .thenComparing(r -> safe(r.entityKey()), String.CASE_INSENSITIVE_ORDER);
        } else {
            c = Comparator.comparing((BillingCompareQueryResponse.Row r) ->
                            r.deltaAmount() != null ? r.deltaAmount().abs() : BigDecimal.ZERO,
                    Comparator.nullsFirst(BigDecimal::compareTo))
                    .thenComparing(r -> safe(r.entityKey()), String.CASE_INSENSITIVE_ORDER);
        }
        return "asc".equalsIgnoreCase(sortDir) ? c : c.reversed();
    }

    private static BillingCompareQueryResponse.Summary summarize(List<BillingCompareQueryResponse.Row> rows) {
        int total = rows.size();
        int matched = 0;
        int changed = 0;
        int leftOnly = 0;
        int rightOnly = 0;
        BigDecimal leftAmount = BigDecimal.ZERO;
        BigDecimal rightAmount = BigDecimal.ZERO;
        BigDecimal delta = BigDecimal.ZERO;
        for (BillingCompareQueryResponse.Row r : rows) {
            leftAmount = leftAmount.add(r.leftAmount() != null ? r.leftAmount() : BigDecimal.ZERO);
            rightAmount = rightAmount.add(r.rightAmount() != null ? r.rightAmount() : BigDecimal.ZERO);
            delta = delta.add(r.deltaAmount() != null ? r.deltaAmount() : BigDecimal.ZERO);
            boolean l = r.leftStatus() != null;
            boolean rr = r.rightStatus() != null;
            if (l && rr) {
                if (r.changedFields() == null || r.changedFields().isEmpty()) matched++;
                else changed++;
            } else if (l) {
                leftOnly++;
            } else if (rr) {
                rightOnly++;
            }
        }
        return new BillingCompareQueryResponse.Summary(total, matched, changed, leftOnly, rightOnly, leftAmount, rightAmount, delta);
    }

    private static BigDecimal amount(Map<String, Object> row) {
        if (row == null) {
            return BigDecimal.ZERO;
        }
        Object v = row.get("total_amount");
        if (v == null) {
            return BigDecimal.ZERO;
        }
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        try {
            return new BigDecimal(v.toString().trim());
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private static String fullName(Map<String, Object> row) {
        String first = str(row.get("client_first_name"));
        String last = str(row.get("client_last_name"));
        if (first == null && last == null) return null;
        return ((first == null ? "" : first) + " " + (last == null ? "" : last)).trim();
    }

    private static String str(Object o) {
        if (o == null) return null;
        String s = o.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static String firstNonBlank(String a, String b) {
        return (a != null && !a.isBlank()) ? a : b;
    }

    private static boolean contains(String value, String searchLower) {
        return value != null && value.toLowerCase().contains(searchLower);
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    public BillingCompareExportResponse export(BillingCompareQueryRequest request, String baseUrl) {
        List<BillingCompareQueryResponse.Row> allRows = new ArrayList<>();
        final int maxPages = 40; // 40 * 500 = 20_000 rows max (was 10_000 pages / unbounded heap)
        for (int exportPage = 1; exportPage <= maxPages; exportPage++) {
            BillingCompareQueryRequest paged = new BillingCompareQueryRequest(
                    request.scenarioCode(),
                    request.left(),
                    request.right(),
                    request.filters(),
                    request.sort(),
                    request.format(),
                    exportPage,
                    500);
            BillingCompareQueryResponse chunk = query(paged);
            if (chunk.rows() != null && !chunk.rows().isEmpty()) {
                allRows.addAll(chunk.rows());
            }
            if (!Boolean.TRUE.equals(chunk.hasNext())) {
                break;
            }
        }
        String exportId = "cmp-exp-" + UUID.randomUUID().toString().substring(0, 8);
        String csv = toCsv(allRows);
        OffsetDateTime expiresAt = OffsetDateTime.now().plusHours(2);
        exports.put(exportId, new ExportFile(csv.getBytes(java.nio.charset.StandardCharsets.UTF_8), expiresAt));
        String downloadUrl = baseUrl + "/api/v1/billing/compare/export/" + exportId + "/download";
        return new BillingCompareExportResponse(exportId, "READY", downloadUrl, expiresAt);
    }

    public byte[] downloadExport(String exportId) {
        ExportFile exportFile = exports.getIfPresent(exportId);
        if (exportFile == null || exportFile.expiresAt().isBefore(OffsetDateTime.now())) {
            exports.invalidate(exportId);
            throw new CompareApiException(
                    "SNAPSHOT_NOT_FOUND",
                    "Export not found or expired",
                    HttpStatus.NOT_FOUND,
                    Map.of("export_id", exportId));
        }
        return exportFile.payload();
    }

    public BillingCompareSnapshotListResponse listSnapshots(
            String stageCode,
            java.time.LocalDate dueDateFrom,
            java.time.LocalDate dueDateTo,
            String search,
            Integer limit,
            Integer offset) {
        int safeLimit = limit != null ? Math.min(Math.max(limit, 1), 500) : 300;
        int safeOffset = offset != null ? Math.max(offset, 0) : 0;
        List<BillingCompareSnapshotListResponse.Item> data = compareRepository.listSnapshots(
                stageCode, dueDateFrom, dueDateTo, search, safeLimit, safeOffset);
        int total = compareRepository.countSnapshots(stageCode, dueDateFrom, dueDateTo, search);
        return new BillingCompareSnapshotListResponse(data, total, safeLimit, safeOffset);
    }

    private void validateRequest(BillingCompareQueryRequest request) {
        if (request == null || request.left() == null || request.right() == null) {
            throw new CompareApiException(
                    "COMPARE_QUERY_INVALID",
                    "Left and right snapshots are required",
                    HttpStatus.BAD_REQUEST,
                    Map.of());
        }
        validateStage(request.left().stageCode(), "left.stage_code");
        validateStage(request.right().stageCode(), "right.stage_code");
        validateScenario(request.scenarioCode(), request.left().stageCode(), request.right().stageCode());
    }

    private static void validateStage(String stageCode, String field) {
        List<String> allowed = List.of("DUE_PREVIEW", "INVOICE_GENERATION", "ACTUAL_CHARGE", "MOCK_CHARGE");
        if (stageCode == null || !allowed.contains(stageCode.toUpperCase())) {
            throw new CompareApiException(
                    "COMPARE_QUERY_INVALID",
                    "Invalid stage code",
                    HttpStatus.BAD_REQUEST,
                    Map.of(field, stageCode));
        }
    }

    private static void validateScenario(String scenarioCode, String leftStageCode, String rightStageCode) {
        String sc = scenarioCode == null ? "" : scenarioCode.toUpperCase();
        if ("CUSTOM".equals(sc)) {
            return;
        }
        Map<String, String> scenarios = Map.of(
                "DUE_PREVIEW_VS_DUE_PREVIEW", "DUE_PREVIEW:DUE_PREVIEW",
                "DUE_PREVIEW_VS_INVOICE_GENERATION", "DUE_PREVIEW:INVOICE_GENERATION",
                "INVOICE_GENERATION_VS_ACTUAL_CHARGE", "INVOICE_GENERATION:ACTUAL_CHARGE");
        String expected = scenarios.get(sc);
        String actual = (leftStageCode == null ? "" : leftStageCode.toUpperCase()) + ":" +
                (rightStageCode == null ? "" : rightStageCode.toUpperCase());
        if (expected == null || !expected.equals(actual)) {
            throw new CompareApiException(
                    "COMPARE_NOT_SUPPORTED",
                    "Scenario and stage combination is not supported",
                    HttpStatus.BAD_REQUEST,
                    Map.of("scenario_code", scenarioCode, "left.stage_code", leftStageCode, "right.stage_code", rightStageCode));
        }
    }

    private static String normalizeSortBy(String sortBy) {
        if (sortBy == null) {
            return "delta_abs";
        }
        List<String> allowed = List.of("delta_abs", "delta_amount", "entity_key", "subscription_plan", "plan_name");
        return allowed.contains(sortBy) ? sortBy : "delta_abs";
    }

    private static String normalizeSortDir(String sortDir) {
        return "asc".equalsIgnoreCase(sortDir) ? "asc" : "desc";
    }

    private static String toCsv(List<BillingCompareQueryResponse.Row> rows) {
        List<String> lines = new ArrayList<>();
        lines.add(String.join(",",
                "left_invoice_id", "left_client_name", "left_client_role", "left_agreement_name", "left_agreement_status", "left_cycle_no",
                "left_location_name", "left_amount", "left_invoice_status",
                "subscription_plan", "plan_name", "subscription_instance_id", "invoice_id", "entity_key",
                "right_invoice_id", "right_client_name", "right_client_role", "right_agreement_name", "right_agreement_status", "right_cycle_no",
                "right_location_name", "right_amount", "right_invoice_status",
                "delta_amount", "changed_fields", "severity"));
        for (BillingCompareQueryResponse.Row r : rows) {
            BillingCompareQueryResponse.SideSnapshot l = r.left();
            BillingCompareQueryResponse.SideSnapshot rt = r.right();
            lines.add(String.join(",",
                    esc(l != null ? l.invoiceId() : null),
                    esc(l != null ? l.clientName() : null),
                    esc(l != null ? l.clientRole() : null),
                    esc(l != null ? l.agreementName() : null),
                    esc(l != null ? l.agreementStatus() : null),
                    esc(l != null ? l.cycleNo() : null),
                    esc(l != null ? l.locationName() : null),
                    l != null && l.amount() != null ? String.valueOf(l.amount()) : "",
                    esc(l != null ? l.invoiceStatus() : null),
                    esc(r.subscriptionPlan()),
                    esc(r.planName()),
                    esc(r.subscriptionInstanceId()),
                    esc(r.invoiceId()),
                    esc(r.entityKey()),
                    esc(rt != null ? rt.invoiceId() : null),
                    esc(rt != null ? rt.clientName() : null),
                    esc(rt != null ? rt.clientRole() : null),
                    esc(rt != null ? rt.agreementName() : null),
                    esc(rt != null ? rt.agreementStatus() : null),
                    esc(rt != null ? rt.cycleNo() : null),
                    esc(rt != null ? rt.locationName() : null),
                    rt != null && rt.amount() != null ? String.valueOf(rt.amount()) : "",
                    esc(rt != null ? rt.invoiceStatus() : null),
                    String.valueOf(r.deltaAmount()),
                    esc(String.join("|", r.changedFields() != null ? r.changedFields() : List.of())),
                    esc(r.severity())));
        }
        return String.join("\n", lines);
    }

    private static String esc(String in) {
        if (in == null) {
            return "";
        }
        return "\"" + in.replace("\"", "\"\"") + "\"";
    }

    private record ExportFile(byte[] payload, OffsetDateTime expiresAt) {}
}

