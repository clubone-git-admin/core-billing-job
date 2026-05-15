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

    @PostMapping("/runs/{reconciliationRunId}/resync")
    public ResponseEntity<Map<String, Object>> resyncReconciliationRun(
            @PathVariable String reconciliationRunId,
            @RequestParam(required = false) String requestedBy,
            @RequestHeader(name = "X-Tenant-Id", defaultValue = "default-tenant") String tenantId,
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey
    ) {
        return ResponseEntity.ok(success(
                service.idempotentResyncRun(tenantId, idempotencyKey, reconciliationRunId, requestedBy), 1, 1));
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
    public ResponseEntity<Map<String, Object>> listAudit(
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) String entityId,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer pageSize
    ) {
        Map<String, Object> payload = service.listAudit(entityType, entityId, limit, page, pageSize);
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

    @GetMapping("/config/enterprise/lookups")
    public ResponseEntity<Map<String, Object>> enterpriseConfigLookups(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseConfigLookups(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/draft")
    public ResponseEntity<Map<String, Object>> enterpriseConfigDraft(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseConfigDraftSummary(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/control-center/shell")
    public ResponseEntity<Map<String, Object>> enterpriseControlCenterShell() {
        return ResponseEntity.ok(success(service.getEnterpriseControlCenterShell(), 1, 1));
    }

    @GetMapping("/config/enterprise/control-center/overview")
    public ResponseEntity<Map<String, Object>> enterpriseControlCenterOverview(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseControlCenterOverview(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/thresholds")
    public ResponseEntity<Map<String, Object>> enterpriseThresholds(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseThresholdsBundle(tenantId), 1, 1));
    }

    @PostMapping("/config/enterprise/thresholds/preview-impact")
    public ResponseEntity<Map<String, Object>> previewEnterpriseThresholdImpact(
            @RequestBody(required = false) ReconciliationRequests.ThresholdPreviewImpactRequest request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseThresholdImpact(request), 1, 1));
    }

    @GetMapping("/config/enterprise/thresholds/{kind}")
    public ResponseEntity<Map<String, Object>> listEnterpriseThresholdsByKind(
            @PathVariable String kind,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String applicationId,
            @RequestParam(required = false) String recoDomain,
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String gatewayCode,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String paymentMethodCode,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String effectiveFrom,
            @RequestParam(required = false) String effectiveTo
    ) {
        Map<String, Object> data = service.listEnterpriseThresholdsByKind(
                kind, tenantId, activeOnly, search, page, pageSize, applicationId,
                recoDomain, stage, level, gatewayCode, currencyCode, paymentMethodCode,
                severity, status, effectiveFrom, effectiveTo);
        return ResponseEntity.ok(success(data, 1, 1));
    }

    @GetMapping("/config/enterprise/thresholds/{kind}/{id}")
    public ResponseEntity<Map<String, Object>> getEnterpriseThresholdRow(
            @PathVariable String kind,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseThresholdRow(kind, tenantId, id), 1, 1));
    }

    @GetMapping("/config/enterprise/thresholds/{kind}/{id}/audit-events")
    public ResponseEntity<Map<String, Object>> listThresholdAuditEvents(
            @PathVariable String kind,
            @PathVariable String id
    ) {
        Map<String, Object> payload = service.listThresholdAuditEvents(kind, id);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/config/enterprise/thresholds/{kind}")
    public ResponseEntity<Map<String, Object>> createEnterpriseThreshold(
            @PathVariable String kind,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseThreshold(kind, tenantId, request, null, actorUserId), 1, 1));
    }

    @PutMapping("/config/enterprise/thresholds/{kind}/{id}")
    public ResponseEntity<Map<String, Object>> updateEnterpriseThreshold(
            @PathVariable String kind,
            @PathVariable String id,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseThreshold(kind, tenantId, request, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/thresholds/{kind}/{id}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateEnterpriseThreshold(
            @PathVariable String kind,
            @PathVariable String id,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId
    ) {
        return ResponseEntity.ok(success(service.deactivateEnterpriseThreshold(kind, tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/thresholds/{kind}/{id}/clone")
    public ResponseEntity<Map<String, Object>> cloneEnterpriseThreshold(
            @PathVariable String kind,
            @PathVariable String id,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId
    ) {
        return ResponseEntity.ok(success(service.cloneEnterpriseThreshold(kind, tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/thresholds/{kind}/test")
    public ResponseEntity<Map<String, Object>> testEnterpriseThreshold(
            @PathVariable String kind,
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) ReconciliationRequests.ThresholdTestRequest request
    ) {
        return ResponseEntity.ok(success(service.testEnterpriseThreshold(kind, tenantId, request), 1, 1));
    }

    @GetMapping("/config/enterprise/access-control")
    public ResponseEntity<Map<String, Object>> listEnterpriseAccessControl(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false, defaultValue = "roles") String view,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String roleId,
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String recoDomain,
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String permissionCode,
            @RequestParam(required = false) String status
    ) {
        Map<String, Object> data = service.listEnterpriseAccessControl(
                tenantId, view, activeOnly, search, page, pageSize,
                roleId, userId, recoDomain, stage, level, permissionCode, status);
        return ResponseEntity.ok(success(data, 1, 1));
    }

    @GetMapping("/config/enterprise/access-control/export-matrix")
    public ResponseEntity<Map<String, Object>> exportEnterpriseAccessMatrix(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.exportEnterpriseAccessMatrix(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/access-control/roles/{id}")
    public ResponseEntity<Map<String, Object>> getEnterpriseAccessRole(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseAccessRole(tenantId, id), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/roles")
    public ResponseEntity<Map<String, Object>> createEnterpriseAccessRole(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseAccessRole(tenantId, request, null, actorUserId), 1, 1));
    }

    @PutMapping("/config/enterprise/access-control/roles/{id}")
    public ResponseEntity<Map<String, Object>> updateEnterpriseAccessRole(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseAccessRole(tenantId, request, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/roles/{id}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateEnterpriseAccessRole(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.deactivateEnterpriseAccessRole(tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/roles/{id}/clone")
    public ResponseEntity<Map<String, Object>> cloneEnterpriseAccessRole(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.cloneEnterpriseAccessRole(tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/user-assignments")
    public ResponseEntity<Map<String, Object>> assignEnterpriseAccessUser(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.assignEnterpriseAccessUser(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/preview-effective-access")
    public ResponseEntity<Map<String, Object>> previewEnterpriseEffectiveAccess(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseEffectiveAccess(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/preview-impact")
    public ResponseEntity<Map<String, Object>> previewEnterpriseAccessImpact(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseAccessImpact(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/access-control/sod-violations/{violationId}/acknowledge")
    public ResponseEntity<Map<String, Object>> acknowledgeEnterpriseSodViolation(
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String violationId
    ) {
        return ResponseEntity.ok(success(service.acknowledgeEnterpriseSodViolation(violationId, actorUserId), 1, 1));
    }

    @GetMapping("/config/enterprise/roles")
    public ResponseEntity<Map<String, Object>> enterpriseRoles(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        Map<String, Object> payload = service.listEnterpriseRoles(tenantId);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @GetMapping("/config/enterprise/notifications")
    public ResponseEntity<Map<String, Object>> listEnterpriseNotifications(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false, defaultValue = "rules") String view,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String notificationEvent,
            @RequestParam(required = false) String channel,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String recoDomain,
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) String gatewayCode,
            @RequestParam(required = false) String status
    ) {
        Map<String, Object> data = service.listEnterpriseNotifications(
                tenantId, view, activeOnly, search, page, pageSize,
                notificationEvent, channel, severity, recoDomain, stage, gatewayCode, status);
        return ResponseEntity.ok(success(data, 1, 1));
    }

    @GetMapping("/config/enterprise/notifications/rules/{id}")
    public ResponseEntity<Map<String, Object>> getEnterpriseNotificationRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseNotificationRule(tenantId, id), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/rules")
    public ResponseEntity<Map<String, Object>> createEnterpriseNotificationRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseNotificationRule(tenantId, request, null, actorUserId), 1, 1));
    }

    @PutMapping("/config/enterprise/notifications/rules/{id}")
    public ResponseEntity<Map<String, Object>> updateEnterpriseNotificationRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertEnterpriseNotificationRule(tenantId, request, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/rules/{id}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateEnterpriseNotificationRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.deactivateEnterpriseNotificationRule(tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/rules/{id}/clone")
    public ResponseEntity<Map<String, Object>> cloneEnterpriseNotificationRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String id
    ) {
        return ResponseEntity.ok(success(service.cloneEnterpriseNotificationRule(tenantId, id, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/test")
    public ResponseEntity<Map<String, Object>> testEnterpriseNotification(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.testEnterpriseNotification(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/send-test-alert")
    public ResponseEntity<Map<String, Object>> sendEnterpriseTestAlert(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.sendEnterpriseTestAlert(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/preview-recipients")
    public ResponseEntity<Map<String, Object>> previewEnterpriseNotificationRecipients(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseNotificationRecipients(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/preview")
    public ResponseEntity<Map<String, Object>> previewEnterpriseNotification(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseNotification(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/failed/{notificationId}/retry")
    public ResponseEntity<Map<String, Object>> retryFailedEnterpriseNotification(
            @PathVariable String notificationId
    ) {
        return ResponseEntity.ok(success(service.retryFailedEnterpriseNotification(notificationId), 1, 1));
    }

    @PostMapping("/config/enterprise/notifications/history/{historyId}/resend")
    public ResponseEntity<Map<String, Object>> resendEnterpriseNotificationHistory(
            @PathVariable String historyId
    ) {
        return ResponseEntity.ok(success(service.resendEnterpriseNotificationHistory(historyId), 1, 1));
    }

    @GetMapping("/config/enterprise/notification-rules")
    public ResponseEntity<Map<String, Object>> enterpriseNotificationRules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        Map<String, Object> payload = service.listEnterpriseNotificationRules(tenantId);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @GetMapping("/config/enterprise/matching-rules")
    public ResponseEntity<Map<String, Object>> listEnterpriseMatchingRules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false) String recoDomain,
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String locationId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize
    ) {
        Map<String, Object> body = service.listEnterpriseMatchingRules(tenantId, recoDomain, stage, activeOnly, search, locationId, page, pageSize);
        long total = ((Number) body.getOrDefault("totalCount", 0L)).longValue();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        if (items == null) {
            items = List.of();
        }
        return ResponseEntity.ok(successPaged(Map.of("items", items), page, pageSize, total));
    }

    @PostMapping("/config/enterprise/matching-rules")
    public ResponseEntity<Map<String, Object>> createEnterpriseMatchingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseMatchingRuleWriteRequest request
    ) {
        return ResponseEntity.ok(success(service.createEnterpriseMatchingRule(tenantId, request, actorUserId), 1, 1));
    }

    @GetMapping("/config/enterprise/matching-rules/{matchingRuleId}")
    public ResponseEntity<Map<String, Object>> getEnterpriseMatchingRule(@PathVariable String matchingRuleId) {
        return ResponseEntity.ok(success(service.getEnterpriseMatchingRuleDetail(matchingRuleId), 1, 1));
    }

    @PutMapping("/config/enterprise/matching-rules/{matchingRuleId}")
    public ResponseEntity<Map<String, Object>> updateEnterpriseMatchingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String matchingRuleId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseMatchingRuleWriteRequest request
    ) {
        return ResponseEntity.ok(success(service.updateEnterpriseMatchingRule(tenantId, matchingRuleId, request, actorUserId), 1, 1));
    }

    @PatchMapping("/config/enterprise/matching-rules/{matchingRuleId}/active")
    public ResponseEntity<Map<String, Object>> patchMatchingRuleActive(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String matchingRuleId,
            @Valid @RequestBody ReconciliationRequests.MatchingRuleActivePatchRequest request
    ) {
        return ResponseEntity.ok(success(service.patchMatchingRuleActive(tenantId, matchingRuleId, request), 1, 1));
    }

    @DeleteMapping("/config/enterprise/matching-rules/{matchingRuleId}")
    public ResponseEntity<Map<String, Object>> deleteEnterpriseMatchingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String matchingRuleId
    ) {
        return ResponseEntity.ok(success(service.deleteEnterpriseMatchingRule(tenantId, matchingRuleId), 1, 1));
    }

    @PostMapping("/config/enterprise/matching-rules/{matchingRuleId}/test")
    public ResponseEntity<Map<String, Object>> testEnterpriseMatchingRule(
            @PathVariable String matchingRuleId,
            @RequestBody(required = false) ReconciliationRequests.MatchingRuleTestRequest request
    ) {
        return ResponseEntity.ok(success(service.testMatchingRulePreview(matchingRuleId, request), 1, 1));
    }

    @GetMapping("/config/enterprise/gl-mapping-rules/export")
    public ResponseEntity<Map<String, Object>> exportEnterpriseGlMappingRules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.exportGlMappingRules(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/gl-mapping-rules/lookups")
    public ResponseEntity<Map<String, Object>> glMappingRulesLookups(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.getGlMappingRuleLookupsBundle(tenantId), 1, 1));
    }

    @GetMapping("/config/enterprise/gl-mapping-rules")
    public ResponseEntity<Map<String, Object>> listEnterpriseGlMappingRules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String locationId,
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String transactionType,
            @RequestParam(required = false) String lineOfBusiness,
            @RequestParam(required = false) String journalType,
            @RequestParam(required = false) String itemCategory,
            @RequestParam(required = false) String paymentMethodCode,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String effectiveFrom,
            @RequestParam(required = false) String effectiveTo,
            @RequestParam(required = false) String refundType,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize
    ) {
        Map<String, Object> body = service.listEnterpriseGlMappingRules(
                tenantId, activeOnly, search, locationId,
                sourceType, transactionType, lineOfBusiness, journalType, itemCategory,
                paymentMethodCode, currencyCode, status, effectiveFrom, effectiveTo, refundType,
                page, pageSize);
        long total = ((Number) body.getOrDefault("totalCount", 0L)).longValue();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        if (items == null) {
            items = List.of();
        }
        Map<String, Object> data = new HashMap<>();
        data.put("items", items);
        data.put("kpis", body.get("kpis"));
        data.put("missingGlMappings", body.get("missingGlMappings"));
        return ResponseEntity.ok(successPaged(data, page, pageSize, total));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules/import")
    public ResponseEntity<Map<String, Object>> importGlMappingRules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.GlMappingImportRequest request
    ) {
        return ResponseEntity.ok(success(service.importGlMappingRules(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules/test")
    public ResponseEntity<Map<String, Object>> testGlMappingJournal(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) ReconciliationRequests.GlMappingJournalTestRequest request
    ) {
        return ResponseEntity.ok(success(service.previewGlMappingJournal(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules/preview-journal")
    public ResponseEntity<Map<String, Object>> previewGlMappingJournal(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) ReconciliationRequests.GlMappingJournalTestRequest request
    ) {
        return ResponseEntity.ok(success(service.previewGlMappingJournal(tenantId, request), 1, 1));
    }

    @GetMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}")
    public ResponseEntity<Map<String, Object>> getEnterpriseGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String glMappingRuleId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseGlMappingRuleDetail(tenantId, glMappingRuleId), 1, 1));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}/clone")
    public ResponseEntity<Map<String, Object>> cloneGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String glMappingRuleId
    ) {
        return ResponseEntity.ok(success(service.cloneGlMappingRule(tenantId, glMappingRuleId, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String glMappingRuleId
    ) {
        return ResponseEntity.ok(success(service.deactivateGlMappingRule(tenantId, glMappingRuleId, actorUserId), 1, 1));
    }

    @GetMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}/usage")
    public ResponseEntity<Map<String, Object>> glMappingRuleUsage(
            @PathVariable String glMappingRuleId,
            @RequestParam(required = false) Integer limit
    ) {
        Map<String, Object> payload = service.glMappingRuleUsage(glMappingRuleId, limit);
        List<?> items = (List<?>) payload.getOrDefault("items", List.of());
        return ResponseEntity.ok(success(payload, 1, items.size()));
    }

    @PostMapping("/config/enterprise/gl-mapping-rules")
    public ResponseEntity<Map<String, Object>> createGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertGlMappingRule(tenantId, request, null, actorUserId), 1, 1));
    }

    @PutMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}")
    public ResponseEntity<Map<String, Object>> updateGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String glMappingRuleId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertGlMappingRule(tenantId, request, glMappingRuleId, actorUserId), 1, 1));
    }

    @PatchMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}/active")
    public ResponseEntity<Map<String, Object>> patchGlMappingActive(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String glMappingRuleId,
            @Valid @RequestBody ReconciliationRequests.MatchingRuleActivePatchRequest request
    ) {
        return ResponseEntity.ok(success(service.patchGlMappingActive(tenantId, glMappingRuleId, request), 1, 1));
    }

    @DeleteMapping("/config/enterprise/gl-mapping-rules/{glMappingRuleId}")
    public ResponseEntity<Map<String, Object>> deleteGlMappingRule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String glMappingRuleId
    ) {
        return ResponseEntity.ok(success(service.deleteGlMappingRule(tenantId, glMappingRuleId), 1, 1));
    }

    @GetMapping("/config/enterprise/schedules")
    public ResponseEntity<Map<String, Object>> listEnterpriseSchedules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false, defaultValue = "active") String view,
            @RequestParam(required = false) Boolean activeOnly,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String recoDomain,
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String gatewayCode,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String paymentMethodCode,
            @RequestParam(required = false) String sourceSystem,
            @RequestParam(required = false) String frequency,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String locationId
    ) {
        Map<String, Object> data = service.listEnterpriseSchedules(
                tenantId, view, activeOnly, search, page, pageSize,
                recoDomain, stage, level, gatewayCode, currencyCode, paymentMethodCode,
                sourceSystem, frequency, status, from, to, locationId);
        return ResponseEntity.ok(success(data, 1, 1));
    }

    @GetMapping("/config/enterprise/schedules/{scheduleId}")
    public ResponseEntity<Map<String, Object>> getEnterpriseScheduleDetail(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(service.getEnterpriseScheduleDetail(tenantId, scheduleId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/preview")
    public ResponseEntity<Map<String, Object>> previewEnterpriseSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterpriseSchedule(tenantId, request), 1, 1));
    }

    @GetMapping("/config/enterprise/schedules/{scheduleId}/runs")
    public ResponseEntity<Map<String, Object>> listEnterpriseScheduleRuns(
            @PathVariable String scheduleId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize
    ) {
        return ResponseEntity.ok(success(service.listEnterpriseScheduleRuns(scheduleId, page, pageSize), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules")
    public ResponseEntity<Map<String, Object>> createSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertSchedule(tenantId, request, null, actorUserId), 1, 1));
    }

    @PutMapping("/config/enterprise/schedules/{scheduleId}")
    public ResponseEntity<Map<String, Object>> updateSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String scheduleId,
            @Valid @RequestBody ReconciliationRequests.EnterpriseConfigRowRequest request
    ) {
        return ResponseEntity.ok(success(service.upsertSchedule(tenantId, request, scheduleId, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/{scheduleId}/clone")
    public ResponseEntity<Map<String, Object>> cloneEnterpriseSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(service.cloneEnterpriseSchedule(tenantId, scheduleId, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/{scheduleId}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateEnterpriseSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(service.deactivateEnterpriseSchedule(tenantId, scheduleId, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/{scheduleId}/run-now")
    public ResponseEntity<Map<String, Object>> runEnterpriseScheduleNow(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(service.runScheduleNow(tenantId, scheduleId, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/{scheduleId}/pause")
    public ResponseEntity<Map<String, Object>> pauseEnterpriseSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(
                service.patchScheduleActive(tenantId, scheduleId, new ReconciliationRequests.MatchingRuleActivePatchRequest(false)),
                1, 1));
    }

    @PostMapping("/config/enterprise/schedules/{scheduleId}/resume")
    public ResponseEntity<Map<String, Object>> resumeEnterpriseSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(
                service.patchScheduleActive(tenantId, scheduleId, new ReconciliationRequests.MatchingRuleActivePatchRequest(true)),
                1, 1));
    }

    @PostMapping("/config/enterprise/schedules/pause-all")
    public ResponseEntity<Map<String, Object>> pauseAllEnterpriseSchedules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.pauseAllEnterpriseSchedules(tenantId), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/resume-all")
    public ResponseEntity<Map<String, Object>> resumeAllEnterpriseSchedules(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId
    ) {
        return ResponseEntity.ok(success(service.resumeAllEnterpriseSchedules(tenantId), 1, 1));
    }

    @PatchMapping("/config/enterprise/schedules/{scheduleId}/active")
    public ResponseEntity<Map<String, Object>> patchScheduleActive(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String scheduleId,
            @Valid @RequestBody ReconciliationRequests.MatchingRuleActivePatchRequest request
    ) {
        return ResponseEntity.ok(success(service.patchScheduleActive(tenantId, scheduleId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/queue/{runReference}/cancel")
    public ResponseEntity<Map<String, Object>> cancelScheduleQueuedRun(@PathVariable String runReference) {
        return ResponseEntity.ok(success(service.cancelScheduleQueuedRun(runReference), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/queue/{runReference}/force-start")
    public ResponseEntity<Map<String, Object>> forceStartScheduleQueuedRun(@PathVariable String runReference) {
        return ResponseEntity.ok(success(service.forceStartScheduleQueuedRun(runReference), 1, 1));
    }

    @PostMapping("/config/enterprise/schedules/queue/{runReference}/retry")
    public ResponseEntity<Map<String, Object>> retryScheduleQueuedRun(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @PathVariable String runReference
    ) {
        return ResponseEntity.ok(success(service.retryScheduleQueuedRun(tenantId, runReference, actorUserId), 1, 1));
    }

    @PatchMapping("/config/enterprise/schedules/queue/{runReference}/priority")
    public ResponseEntity<Map<String, Object>> patchScheduleQueuePriority(
            @PathVariable String runReference,
            @RequestBody Map<String, Object> body
    ) {
        String priority = body == null || body.get("priority") == null ? "MEDIUM" : String.valueOf(body.get("priority"));
        return ResponseEntity.ok(success(service.patchScheduleQueuePriority(runReference, priority), 1, 1));
    }

    @DeleteMapping("/config/enterprise/schedules/{scheduleId}")
    public ResponseEntity<Map<String, Object>> deleteSchedule(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String scheduleId
    ) {
        return ResponseEntity.ok(success(service.deleteSchedule(tenantId, scheduleId), 1, 1));
    }

    @GetMapping("/config/enterprise/publish")
    public ResponseEntity<Map<String, Object>> listEnterprisePublish(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false, defaultValue = "current") String view,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String environment,
            @RequestParam(required = false) String version,
            @RequestParam(required = false) String publishedBy,
            @RequestParam(required = false) String publishStatus,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String versionA,
            @RequestParam(required = false) String versionB
    ) {
        Map<String, Object> data = service.listEnterprisePublish(
                tenantId, view, search, page, pageSize, environment, version,
                publishedBy, publishStatus, from, to, versionA, versionB);
        return ResponseEntity.ok(success(data, 1, 1));
    }

    @GetMapping("/config/enterprise/publish/export-snapshot")
    public ResponseEntity<Map<String, Object>> exportEnterprisePublishSnapshot(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(required = false) String version
    ) {
        return ResponseEntity.ok(success(service.exportEnterprisePublishSnapshot(tenantId, version), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/save-draft")
    public ResponseEntity<Map<String, Object>> saveEnterprisePublishDraft(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.saveEnterprisePublishDraft(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/preview-draft")
    public ResponseEntity<Map<String, Object>> previewEnterprisePublishDraft(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterprisePublishDraft(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/validate")
    public ResponseEntity<Map<String, Object>> validateEnterprisePublish(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.validateEnterprisePublish(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/preview-impact")
    public ResponseEntity<Map<String, Object>> previewEnterprisePublishImpact(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.previewEnterprisePublishImpact(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/execute")
    public ResponseEntity<Map<String, Object>> executeEnterprisePublish(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> request
    ) {
        if (service.isEnterprisePublishFrozen(tenantId, request)) {
            Map<String, Object> freeze = service.getEnterprisePublishFreezeWindow(tenantId, request);
            @SuppressWarnings("unchecked")
            Map<String, Object> fw = (Map<String, Object>) freeze.get("freezeWindow");
            String message = fw == null ? "Configuration freeze window is active" : String.valueOf(fw.getOrDefault("message", "Configuration freeze window is active"));
            return ResponseEntity.status(409).body(Map.of(
                    "success", false,
                    "message", message,
                    "freezeWindow", fw == null ? Map.of() : fw
            ));
        }
        return ResponseEntity.ok(success(service.executeEnterprisePublish(tenantId, request, actorUserId, idempotencyKey), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/rollback")
    public ResponseEntity<Map<String, Object>> rollbackEnterprisePublishConfig(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody Map<String, Object> request
    ) {
        if (service.isEnterprisePublishFrozen(tenantId, request)) {
            Map<String, Object> freeze = service.getEnterprisePublishFreezeWindow(tenantId, request);
            @SuppressWarnings("unchecked")
            Map<String, Object> fw = (Map<String, Object>) freeze.get("freezeWindow");
            String message = fw == null ? "Configuration freeze window is active" : String.valueOf(fw.getOrDefault("message", "Configuration freeze window is active"));
            return ResponseEntity.status(409).body(Map.of("success", false, "message", message, "freezeWindow", fw == null ? Map.of() : fw));
        }
        return ResponseEntity.ok(success(service.rollbackEnterprisePublishConfig(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/promote")
    public ResponseEntity<Map<String, Object>> promoteEnterprisePublish(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody Map<String, Object> request
    ) {
        if (service.isEnterprisePublishFrozen(tenantId, request)) {
            Map<String, Object> freeze = service.getEnterprisePublishFreezeWindow(tenantId, request);
            @SuppressWarnings("unchecked")
            Map<String, Object> fw = (Map<String, Object>) freeze.get("freezeWindow");
            String message = fw == null ? "Configuration freeze window is active" : String.valueOf(fw.getOrDefault("message", "Configuration freeze window is active"));
            return ResponseEntity.status(409).body(Map.of("success", false, "message", message, "freezeWindow", fw == null ? Map.of() : fw));
        }
        return ResponseEntity.ok(success(service.promoteEnterprisePublish(tenantId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/draft-changes/{draftChangeId}/approve")
    public ResponseEntity<Map<String, Object>> approveEnterpriseDraftChange(
            @PathVariable String draftChangeId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.approveEnterpriseDraftChange(draftChangeId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/draft-changes/{draftChangeId}/reject")
    public ResponseEntity<Map<String, Object>> rejectEnterpriseDraftChange(
            @PathVariable String draftChangeId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.rejectEnterpriseDraftChange(draftChangeId, request, actorUserId), 1, 1));
    }

    @PostMapping("/config/enterprise/publish/draft-changes/{draftChangeId}/discard")
    public ResponseEntity<Map<String, Object>> discardEnterpriseDraftChange(
            @PathVariable String draftChangeId,
            @RequestHeader(name = "X-Actor-Id", required = false) String actorUserId,
            @RequestBody(required = false) Map<String, Object> request
    ) {
        return ResponseEntity.ok(success(service.discardEnterpriseDraftChange(draftChangeId, request, actorUserId), 1, 1));
    }

    @GetMapping("/config/enterprise/publishes")
    public ResponseEntity<Map<String, Object>> listEnterpriseConfigPublishes(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int pageSize
    ) {
        Map<String, Object> body = service.listEnterpriseConfigPublishes(tenantId, page, pageSize);
        long total = ((Number) body.getOrDefault("totalCount", 0L)).longValue();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        if (items == null) {
            items = List.of();
        }
        return ResponseEntity.ok(successPaged(Map.of("items", items), page, pageSize, total));
    }

    @GetMapping("/config/enterprise/publishes/compare")
    public ResponseEntity<Map<String, Object>> compareEnterprisePublishes(
            @RequestParam String a,
            @RequestParam String b
    ) {
        return ResponseEntity.ok(success(service.compareEnterprisePublishes(a, b), 1, 1));
    }

    @PostMapping("/config/enterprise/publishes")
    public ResponseEntity<Map<String, Object>> publishEnterpriseConfig(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody ReconciliationRequests.PublishRecoConfigRequest request
    ) {
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            return ResponseEntity.ok(success(service.publishEnterpriseConfigIdempotent(tenantId, idempotencyKey, request), 1, 1));
        }
        return ResponseEntity.ok(success(service.publishEnterpriseConfig(tenantId, request), 1, 1));
    }

    @PostMapping("/config/enterprise/publishes/{recoConfigPublishId}/rollback")
    public ResponseEntity<Map<String, Object>> rollbackEnterprisePublish(
            @RequestHeader(name = "X-Tenant-Id", required = false) String tenantId,
            @PathVariable String recoConfigPublishId
    ) {
        return ResponseEntity.ok(success(service.rollbackEnterprisePublish(tenantId, recoConfigPublishId), 1, 1));
    }

    @PostMapping("/runs/{reconciliationRunId}/config-publish")
    public ResponseEntity<Map<String, Object>> attachRunConfigPublish(
            @PathVariable String reconciliationRunId,
            @Valid @RequestBody ReconciliationRequests.AttachRunConfigPublishRequest request
    ) {
        return ResponseEntity.ok(success(service.attachRunConfigPublish(reconciliationRunId, request), 1, 1));
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
