package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import io.clubone.billing.service.ReconciliationModuleService;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/reconciliation/v1")
public class ReconciliationModuleController {

    private final ReconciliationModuleService service;

    public ReconciliationModuleController(ReconciliationModuleService service) {
        this.service = service;
    }

    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> dashboardSummary(
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate,
            @RequestParam(required = false) String recoRunId,
            @RequestParam(required = false) List<String> locationIds,
            @RequestParam(required = false) List<String> gatewayIds,
            @RequestParam(required = false) List<String> paymentTypes,
            @RequestParam(required = false, defaultValue = "5") Integer varianceDriverLimit,
            @RequestParam(required = false, defaultValue = "true") Boolean includeVarianceChart
    ) {
        Map<String, Object> filters = new HashMap<>();
        filters.put("fromDate", fromDate);
        filters.put("toDate", toDate);
        filters.put("recoRunId", recoRunId);
        filters.put("locationIds", locationIds == null ? List.of() : locationIds);
        filters.put("gatewayIds", gatewayIds == null ? List.of() : gatewayIds);
        filters.put("paymentTypes", paymentTypes == null ? List.of() : paymentTypes);
        filters.put("varianceDriverLimit", varianceDriverLimit);
        filters.put("includeVarianceChart", includeVarianceChart);
        return ResponseEntity.ok(success(service.getDashboardSummary(filters), 1, 1));
    }

    @GetMapping("/dashboard/mismatches")
    public ResponseEntity<Map<String, Object>> dashboardMismatches(
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate,
            @RequestParam(required = false) String recoRunId,
            @RequestParam(required = false) List<String> locationIds,
            @RequestParam(required = false) List<String> gatewayIds,
            @RequestParam(required = false) List<String> paymentTypes,
            @RequestParam(required = false) List<String> severity,
            @RequestParam(required = false) List<String> status,
            @RequestParam(required = false) List<String> stage,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String reconciliationType,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "25") int pageSize
    ) {
        Map<String, Object> filters = new HashMap<>();
        filters.put("fromDate", fromDate);
        filters.put("toDate", toDate);
        filters.put("recoRunId", recoRunId);
        filters.put("locationIds", locationIds == null ? List.of() : locationIds);
        filters.put("gatewayIds", gatewayIds == null ? List.of() : gatewayIds);
        filters.put("paymentTypes", paymentTypes == null ? List.of() : paymentTypes);
        filters.put("severity", severity == null ? List.of() : severity);
        filters.put("status", status == null ? List.of() : status);
        filters.put("stage", stage == null ? List.of() : stage);
        filters.put("search", search);
        filters.put("reconciliationType", reconciliationType);
        Map<String, Object> payload = service.getDashboardMismatches(filters, page, pageSize);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, page, items.size()));
    }

    @GetMapping("/dashboard/trends")
    public ResponseEntity<Map<String, Object>> dashboardTrends(
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate,
            @RequestParam(required = false) String recoRunId,
            @RequestParam(defaultValue = "variance") String metric,
            @RequestParam(defaultValue = "day") String granularity,
            @RequestParam(required = false) List<String> locationIds,
            @RequestParam(required = false) List<String> gatewayIds,
            @RequestParam(required = false) List<String> paymentTypes,
            @RequestParam(required = false) String reconciliationType
    ) {
        Map<String, Object> filters = new HashMap<>();
        filters.put("fromDate", fromDate);
        filters.put("toDate", toDate);
        filters.put("recoRunId", recoRunId);
        filters.put("metric", metric);
        filters.put("granularity", granularity);
        filters.put("locationIds", locationIds == null ? List.of() : locationIds);
        filters.put("gatewayIds", gatewayIds == null ? List.of() : gatewayIds);
        filters.put("paymentTypes", paymentTypes == null ? List.of() : paymentTypes);
        filters.put("reconciliationType", reconciliationType);
        Map<String, Object> payload = service.getDashboardTrends(filters);
        List<?> items = (List<?>) payload.getOrDefault("series", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @GetMapping("/workspace/{entityId}")
    public ResponseEntity<Map<String, Object>> workspace(
            @PathVariable String entityId,
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) String recoRunId,
            @RequestParam(defaultValue = "true") boolean recompute,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getWorkspace(entityId, entityType, recoRunId, recompute, tenantId), 1, 1));
    }

    @PostMapping("/runs")
    public ResponseEntity<Map<String, Object>> createRun(
            @RequestHeader(name = "X-Tenant-Id", defaultValue = "default-tenant") String tenantId,
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey,
            @RequestParam(defaultValue = "FULL") String runType,
            @RequestParam(defaultValue = "MANUAL") String triggerMode,
            @RequestParam(required = false) String requestedBy,
            @RequestBody(required = false) Map<String, Object> filters
    ) {
        return ResponseEntity.ok(success(
                service.idempotentCreateRun(tenantId, idempotencyKey, runType, triggerMode, requestedBy, filters), 1, 1));
    }

    @GetMapping("/lookups")
    public ResponseEntity<Map<String, Object>> reconciliationLookups() {
        Map<String, Object> data = service.getReconciliationLookups();
        return ResponseEntity.ok(success(data, 1, Math.max(1, data.size())));
    }

    @GetMapping("/runs")
    public ResponseEntity<Map<String, Object>> listRuns(
            @RequestParam(required = false) String financialPeriod,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate,
            @RequestParam(required = false) List<String> locationIds,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String runType,
            @RequestParam(required = false) String currency,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize
    ) {
        Map<String, Object> body = service.listReconciliationRuns(
                financialPeriod, fromDate, toDate, expandLocationRequest(locationIds), status, runType, currency, page, pageSize);
        long total = ((Number) body.getOrDefault("totalCount", 0L)).longValue();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        if (items == null) {
            items = List.of();
        }
        return ResponseEntity.ok(successPaged(Map.of("items", items), page, pageSize, total));
    }

    @GetMapping("/runs/{reconciliationRunId}")
    public ResponseEntity<Map<String, Object>> getRun(@PathVariable String reconciliationRunId) {
        return ResponseEntity.ok(success(service.getReconciliationRun(reconciliationRunId), 1, 1));
    }

    /**
     * Full reconciliation chain (subscription: billing schedule → … → GL; cash: invoice → … → GL), paginated per scope.
     */
    @GetMapping("/runs/{reconciliationRunId}/reconciliation-details")
    public ResponseEntity<Map<String, Object>> runReconciliationDetails(
            @PathVariable String reconciliationRunId,
            @RequestParam(defaultValue = "1") int subscriptionPage,
            @RequestParam(defaultValue = "25") int subscriptionPageSize,
            @RequestParam(defaultValue = "1") int cashPage,
            @RequestParam(defaultValue = "25") int cashPageSize
    ) {
        Map<String, Object> payload = service.getRunReconciliationDetails(
                reconciliationRunId, subscriptionPage, subscriptionPageSize, cashPage, cashPageSize);
        @SuppressWarnings("unchecked")
        Map<String, Object> sub = (Map<String, Object>) payload.getOrDefault("subscription", Map.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> cash = (Map<String, Object>) payload.getOrDefault("cash", Map.of());
        long total = ((Number) sub.getOrDefault("totalCount", 0L)).longValue()
                + ((Number) cash.getOrDefault("totalCount", 0L)).longValue();
        return ResponseEntity.ok(success(payload, 1, (int) Math.min(Integer.MAX_VALUE, total)));
    }

    @GetMapping("/workspace/{entityId}/timeline")
    public ResponseEntity<Map<String, Object>> workspaceTimeline(@PathVariable String entityId) {
        return ResponseEntity.ok(success(service.getWorkspaceTimeline(entityId), 1, 1));
    }

    @GetMapping("/workspace/{entityId}/payloads")
    public ResponseEntity<Map<String, Object>> workspacePayloads(
            @PathVariable String entityId,
            @RequestParam(defaultValue = "all") String source
    ) {
        return ResponseEntity.ok(success(service.getWorkspacePayloads(entityId, source), 1, 1));
    }

    @PostMapping("/workspace/{entityId}/actions/retry-sync")
    public ResponseEntity<Map<String, Object>> retrySync(
            @PathVariable String entityId,
            @RequestHeader(name = "X-Tenant-Id", defaultValue = "default-tenant") String tenantId,
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody ReconciliationRequests.RetrySyncRequest request
    ) {
        return ResponseEntity.accepted().body(success(service.idempotentRetrySync(tenantId, idempotencyKey, entityId, request), 1, 1));
    }

    @GetMapping("/exceptions")
    public ResponseEntity<Map<String, Object>> listExceptions(
            @RequestParam(required = false) String recoRunId,
            @RequestParam(required = false) List<String> severity,
            @RequestParam(required = false) List<String> status,
            @RequestParam(required = false) List<String> assigneeIds,
            @RequestParam(required = false) List<String> stage,
            @RequestParam(required = false) Boolean slaBreached,
            @RequestParam(required = false) String search
    ) {
        Map<String, Object> filters = new HashMap<>();
        filters.put("recoRunId", recoRunId);
        filters.put("severity", severity == null ? List.of() : severity);
        filters.put("status", status == null ? List.of() : status);
        filters.put("assigneeIds", assigneeIds == null ? List.of() : assigneeIds);
        filters.put("stage", stage == null ? List.of() : stage);
        filters.put("slaBreached", slaBreached);
        filters.put("search", search);
        Map<String, Object> payload = service.listExceptions(filters);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/exceptions")
    public ResponseEntity<Map<String, Object>> createException(
            @RequestHeader(name = "X-Tenant-Id", defaultValue = "default-tenant") String tenantId,
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody ReconciliationRequests.CreateExceptionRequest request
    ) {
        return ResponseEntity.status(201).body(success(service.idempotentCreateException(tenantId, idempotencyKey, request), 1, 1));
    }

    @PatchMapping("/exceptions/{exceptionId}/assign")
    public ResponseEntity<Map<String, Object>> assignException(
            @PathVariable String exceptionId,
            @Valid @RequestBody ReconciliationRequests.AssignExceptionRequest request
    ) {
        return ResponseEntity.ok(success(service.assignException(exceptionId, request), 1, 1));
    }

    @PatchMapping("/exceptions/{exceptionId}/status")
    public ResponseEntity<Map<String, Object>> updateExceptionStatus(
            @PathVariable String exceptionId,
            @Valid @RequestBody ReconciliationRequests.UpdateExceptionStatusRequest request
    ) {
        return ResponseEntity.ok(success(service.updateExceptionStatus(exceptionId, request), 1, 1));
    }

    @PostMapping("/exceptions/bulk-action")
    public ResponseEntity<Map<String, Object>> bulkAction(@Valid @RequestBody ReconciliationRequests.BulkActionRequest request) {
        return ResponseEntity.ok(success(service.bulkExceptionAction(request), 1, 1));
    }

    @PostMapping("/exceptions/{exceptionId}/notes")
    public ResponseEntity<Map<String, Object>> addNote(
            @PathVariable String exceptionId,
            @Valid @RequestBody ReconciliationRequests.AddNoteRequest request
    ) {
        return ResponseEntity.ok(success(service.addExceptionNote(exceptionId, request), 1, 1));
    }

    @GetMapping("/accounting/journal")
    public ResponseEntity<Map<String, Object>> journal(
            @RequestParam(required = false) String period,
            @RequestParam(required = false) List<String> glAccountCodes,
            @RequestParam(required = false) List<String> postingStatus,
            @RequestParam(required = false) String entityId,
            @RequestParam(required = false) String recoRunId
    ) {
        Map<String, Object> payload = service.getJournal(period, glAccountCodes, postingStatus, entityId, recoRunId);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/accounting/journal/validate")
    public ResponseEntity<Map<String, Object>> validateJournal(@Valid @RequestBody ReconciliationRequests.JournalValidateRequest request) {
        return ResponseEntity.ok(success(service.validateJournal(request), 1, request.journalIds().size()));
    }

    @PostMapping(value = "/bank/statements", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> uploadStatement(
            @RequestPart("file") MultipartFile file,
            @RequestParam String bankAccountId,
            @RequestParam String statementDateFrom,
            @RequestParam String statementDateTo,
            @RequestParam String format
    ) {
        return ResponseEntity.status(201).body(success(
                service.uploadBankStatement(file, bankAccountId, statementDateFrom, statementDateTo, format), 1, 1));
    }

    @PostMapping("/bank/reconcile")
    public ResponseEntity<Map<String, Object>> triggerReconcile(
            @Valid @RequestBody ReconciliationRequests.TriggerBankReconcileRequest request
    ) {
        return ResponseEntity.accepted().body(success(service.triggerAutoMatch(request), 1, 1));
    }

    @GetMapping("/bank/reconcile/{reconcileRunId}")
    public ResponseEntity<Map<String, Object>> reconcileResult(@PathVariable String reconcileRunId) {
        return ResponseEntity.ok(success(service.getBankReconcileResult(reconcileRunId), 1, 1));
    }

    @PostMapping("/bank/reconcile/{reconcileRunId}/manual-match")
    public ResponseEntity<Map<String, Object>> manualMatch(
            @PathVariable String reconcileRunId,
            @Valid @RequestBody ReconciliationRequests.ManualMatchRequest request
    ) {
        return ResponseEntity.ok(success(service.manualMatch(reconcileRunId, request), 1, 1));
    }

    @PostMapping("/bank/reconcile/{reconcileRunId}/flag-exception")
    public ResponseEntity<Map<String, Object>> flagException(
            @PathVariable String reconcileRunId,
            @Valid @RequestBody ReconciliationRequests.FlagExceptionRequest request
    ) {
        return ResponseEntity.ok(success(service.flagBankException(reconcileRunId, request), 1, 1));
    }

    @GetMapping("/refunds")
    public ResponseEntity<Map<String, Object>> refunds(@RequestParam(required = false) String recoRunId) {
        Map<String, Object> payload = service.listRefunds(recoRunId);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @GetMapping("/refunds/{refundId}")
    public ResponseEntity<Map<String, Object>> refundDetail(@PathVariable String refundId) {
        return ResponseEntity.ok(success(service.refundDetail(refundId), 1, 1));
    }

    @PostMapping("/refunds/{refundId}/retry")
    public ResponseEntity<Map<String, Object>> retryRefund(@PathVariable String refundId) {
        return ResponseEntity.accepted().body(success(service.retryRefund(refundId), 1, 1));
    }

    @PatchMapping("/refunds/{refundId}/link")
    public ResponseEntity<Map<String, Object>> linkRefund(
            @PathVariable String refundId,
            @Valid @RequestBody ReconciliationRequests.LinkRefundRequest request
    ) {
        return ResponseEntity.ok(success(service.linkRefund(refundId, request), 1, 1));
    }

    @GetMapping("/reports/catalog")
    public ResponseEntity<Map<String, Object>> reportCatalog() {
        Map<String, Object> payload = service.reportCatalog();
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/reports/{slug}/query")
    public ResponseEntity<Map<String, Object>> reportQuery(
            @PathVariable String slug,
            @Valid @RequestBody ReconciliationRequests.ReportQueryRequest request
    ) {
        return ResponseEntity.ok(success(service.reportQuery(slug, request), 1, 1));
    }

    @PostMapping("/reports/{slug}/export")
    public ResponseEntity<Map<String, Object>> reportExport(
            @PathVariable String slug,
            @RequestHeader(name = "X-Tenant-Id", defaultValue = "default-tenant") String tenantId,
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody ReconciliationRequests.ReportExportRequest request
    ) {
        return ResponseEntity.accepted().body(success(service.idempotentReportExport(tenantId, idempotencyKey, slug, request), 1, 1));
    }

    @GetMapping("/reports/exports/{exportId}")
    public ResponseEntity<Map<String, Object>> reportExportStatus(@PathVariable String exportId) {
        return ResponseEntity.ok(success(service.reportExportStatus(exportId), 1, 1));
    }

    @GetMapping("/audit")
    public ResponseEntity<Map<String, Object>> listAudit() {
        Map<String, Object> payload = service.listAudit();
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/audit/export")
    public ResponseEntity<Map<String, Object>> auditExport() {
        return ResponseEntity.accepted().body(success(service.auditExport(), 1, 1));
    }

    @GetMapping("/config/{configType}")
    public ResponseEntity<Map<String, Object>> getConfig(@PathVariable String configType) {
        return ResponseEntity.ok(success(service.getConfig(configType), 1, 1));
    }

    @PutMapping("/config/{configType}")
    public ResponseEntity<Map<String, Object>> putConfig(
            @PathVariable String configType,
            @Valid @RequestBody ReconciliationRequests.ConfigUpdateRequest request
    ) {
        return ResponseEntity.ok(success(service.updateConfig(configType, request), 1, 1));
    }

    private Map<String, Object> success(Object data, int page, int totalRecords) {
        return Map.of(
                "success", true,
                "data", data,
                "meta", Map.of(
                        "requestId", UUID.randomUUID().toString(),
                        "timestamp", OffsetDateTime.now().toString(),
                        "page", page,
                        "pageSize", totalRecords,
                        "totalRecords", totalRecords,
                        "totalPages", 1
                ),
                "errors", List.of()
        );
    }

    private Map<String, Object> successPaged(Object data, int page, int pageSize, long totalRecords) {
        int safePageSize = pageSize <= 0 ? 50 : pageSize;
        int totalPages = (int) Math.max(1, Math.ceil(totalRecords / (double) safePageSize));
        Map<String, Object> meta = new HashMap<>();
        meta.put("requestId", UUID.randomUUID().toString());
        meta.put("timestamp", OffsetDateTime.now().toString());
        meta.put("page", page);
        meta.put("pageSize", safePageSize);
        meta.put("totalRecords", totalRecords);
        meta.put("totalPages", totalPages);
        return Map.of("success", true, "data", data, "meta", meta, "errors", List.of());
    }

    private static List<String> expandLocationRequest(List<String> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String s : locationIds) {
            if (s == null || s.isBlank()) {
                continue;
            }
            for (String p : s.split(",")) {
                if (!p.isBlank()) {
                    out.add(p.trim());
                }
            }
        }
        return out;
    }
}
