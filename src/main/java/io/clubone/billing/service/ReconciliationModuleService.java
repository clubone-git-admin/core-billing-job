package io.clubone.billing.service;

import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import io.clubone.billing.repo.ReconciliationEnterpriseConfigRepository;
import io.clubone.billing.repo.ReconciliationEnterpriseAccessControlRepository;
import io.clubone.billing.repo.ReconciliationEnterprisePublishRepository;
import io.clubone.billing.repo.ReconciliationEnterpriseNotificationRepository;
import io.clubone.billing.repo.ReconciliationEnterpriseScheduleRepository;
import io.clubone.billing.repo.ReconciliationModuleRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

@Service
public class ReconciliationModuleService {

    private final ReconciliationModuleRepository repository;
    private final ReconciliationEnterpriseConfigRepository enterpriseConfigRepository;
    private final ReconciliationEnterpriseScheduleRepository scheduleRepository;
    private final ReconciliationEnterpriseNotificationRepository notificationRepository;
    private final ReconciliationEnterpriseAccessControlRepository accessControlRepository;
    private final ReconciliationEnterprisePublishRepository publishRepository;
    private final ObjectMapper objectMapper;

    public ReconciliationModuleService(
            ReconciliationModuleRepository repository,
            ReconciliationEnterpriseConfigRepository enterpriseConfigRepository,
            ReconciliationEnterpriseScheduleRepository scheduleRepository,
            ReconciliationEnterpriseNotificationRepository notificationRepository,
            ReconciliationEnterpriseAccessControlRepository accessControlRepository,
            ReconciliationEnterprisePublishRepository publishRepository,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.enterpriseConfigRepository = enterpriseConfigRepository;
        this.scheduleRepository = scheduleRepository;
        this.notificationRepository = notificationRepository;
        this.accessControlRepository = accessControlRepository;
        this.publishRepository = publishRepository;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> getWorkspace(String entityId, String entityType, String recoRunId, boolean recompute, String tenantId) {
        return repository.getWorkspace(entityId, entityType, recoRunId, recompute, tenantId);
    }

    public Map<String, Object> getDashboardSummary(Map<String, Object> filters) {
        return repository.getDashboardSummary(filters);
    }

    public Map<String, Object> getDashboardMismatches(Map<String, Object> filters, int page, int pageSize) {
        return Map.of("items", repository.getDashboardMismatches(filters, page, pageSize));
    }

    public Map<String, Object> getDashboardTrends(Map<String, Object> filters) {
        return repository.getDashboardTrendsPayload(filters);
    }

    public Map<String, Object> getWorkspaceTimeline(String entityId) {
        return Map.of("events", repository.getWorkspaceTimeline(entityId));
    }

    public Map<String, Object> getWorkspacePayloads(String entityId, String source) {
        return Map.of("payloads", repository.getWorkspacePayloads(entityId, source));
    }

    @Transactional
    public Map<String, Object> createReconciliationRun(String runType, String triggerMode, String requestedBy, String tenantId, Object filters) {
        return repository.createReconciliationRun(runType, triggerMode, requestedBy, tenantId, filters);
    }

    public Map<String, Object> getReconciliationRun(String runId) {
        return repository.getReconciliationRun(runId);
    }

    public Map<String, Object> getRunReconciliationDetails(
            String recoRunId,
            int subscriptionPage,
            int subscriptionPageSize,
            int cashPage,
            int cashPageSize
    ) {
        return repository.getRunReconciliationDetails(recoRunId, subscriptionPage, subscriptionPageSize, cashPage, cashPageSize);
    }

    public Map<String, Object> getReconciliationLookups() {
        return repository.getReconciliationLookups();
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
        return repository.listReconciliationRuns(financialPeriod, fromDate, toDate, expandCommaSeparated(locationIds), status, runType, currency, page, pageSize);
    }

    private static List<String> expandCommaSeparated(List<String> raw) {
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String s : raw) {
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

    public Map<String, Object> retrySync(String entityId, ReconciliationRequests.RetrySyncRequest request) {
        return Map.of(
                "jobId", "JOB-RETRY-" + Math.abs(UUID.randomUUID().hashCode()),
                "status", "QUEUED",
                "entityId", entityId,
                "message", "Retry sync queued",
                "requestedBy", request.requestedBy() == null ? "system" : request.requestedBy(),
                "requestedAt", OffsetDateTime.now()
        );
    }

    @Transactional
    public Map<String, Object> idempotentCreateRun(String tenantId, String idempotencyKey, String runType, String triggerMode, String requestedBy, Object filters) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("runType", runType);
        payload.put("triggerMode", triggerMode);
        payload.put("requestedBy", requestedBy);
        payload.put("filters", filters != null ? filters : Map.of());
        return executeIdempotent(tenantId, idempotencyKey, "POST", "/reconciliation/v1/runs", payload, () -> createReconciliationRun(runType, triggerMode, requestedBy, tenantId, filters));
    }

    @Transactional
    public Map<String, Object> idempotentResyncRun(String tenantId, String idempotencyKey, String reconciliationRunId, String requestedBy) {
        Map<String, Object> payload = Map.of("reconciliationRunId", reconciliationRunId);
        String path = "/reconciliation/v1/runs/" + reconciliationRunId + "/resync";
        return executeIdempotent(tenantId, idempotencyKey, "POST", path, payload, () -> repository.resyncReconciliationRun(reconciliationRunId, requestedBy, tenantId));
    }

    @Transactional
    public Map<String, Object> idempotentCreateException(String tenantId, String idempotencyKey, ReconciliationRequests.CreateExceptionRequest request) {
        return executeIdempotent(tenantId, idempotencyKey, "POST", "/reconciliation/v1/exceptions", request, () -> createException(request, tenantId));
    }

    @Transactional
    public Map<String, Object> idempotentRetrySync(String tenantId, String idempotencyKey, String entityId, ReconciliationRequests.RetrySyncRequest request) {
        return executeIdempotent(tenantId, idempotencyKey, "POST", "/reconciliation/v1/workspace/" + entityId + "/actions/retry-sync", request, () -> retrySync(entityId, request));
    }

    @Transactional
    public Map<String, Object> idempotentReportExport(String tenantId, String idempotencyKey, String slug, ReconciliationRequests.ReportExportRequest request) {
        return executeIdempotent(tenantId, idempotencyKey, "POST", "/reconciliation/v1/reports/" + slug + "/export", request, () -> reportExport(slug, request));
    }

    public Map<String, Object> listExceptions(Map<String, Object> filters) {
        return Map.of("items", repository.listExceptions(filters));
    }

    @Transactional
    public Map<String, Object> createException(ReconciliationRequests.CreateExceptionRequest request, String tenantId) {
        String id = repository.createException(request, tenantId);
        return Map.of("exceptionId", id, "status", "OPEN");
    }

    @Transactional
    public Map<String, Object> assignException(String exceptionId, ReconciliationRequests.AssignExceptionRequest request) {
        repository.assignException(exceptionId, request.assigneeId());
        return Map.of("exceptionId", exceptionId, "assigneeId", request.assigneeId(), "status", "IN_PROGRESS");
    }

    @Transactional
    public Map<String, Object> updateExceptionStatus(String exceptionId, ReconciliationRequests.UpdateExceptionStatusRequest request) {
        repository.updateExceptionStatus(exceptionId, request.status(), request.reason());
        return Map.of("exceptionId", exceptionId, "status", request.status());
    }

    @Transactional
    public Map<String, Object> bulkExceptionAction(ReconciliationRequests.BulkActionRequest request) {
        int updated = repository.bulkExceptionAction(request);
        return Map.of("updatedCount", updated, "action", request.action());
    }

    @Transactional
    public Map<String, Object> addExceptionNote(String exceptionId, ReconciliationRequests.AddNoteRequest request) {
        repository.addExceptionNote(exceptionId, request.note(), request.noteTypeCode());
        return Map.of("exceptionId", exceptionId, "noteAdded", true);
    }

    public Map<String, Object> getJournal(String period, List<String> glAccountCodes, List<String> postingStatus, String entityId, String recoRunId) {
        return Map.of("items", repository.getJournal(period, glAccountCodes, postingStatus, entityId, recoRunId));
    }

    @Transactional
    public Map<String, Object> validateJournal(ReconciliationRequests.JournalValidateRequest request) {
        return Map.of("items", repository.validateJournal(request.journalIds()));
    }

    @Transactional
    public Map<String, Object> uploadBankStatement(MultipartFile file, String bankAccountId, String statementDateFrom, String statementDateTo, String format) {
        String statementId = repository.saveBankStatement(file.getOriginalFilename(), bankAccountId, statementDateFrom, statementDateTo, format);
        return Map.of("statementId", statementId, "status", "UPLOADED");
    }

    @Transactional
    public Map<String, Object> triggerAutoMatch(ReconciliationRequests.TriggerBankReconcileRequest request) {
        String runId = repository.createBankReconcileRun(request);
        return Map.of("reconcileRunId", runId, "status", "QUEUED");
    }

    public Map<String, Object> getBankReconcileResult(String reconcileRunId) {
        return repository.getBankReconcileResult(reconcileRunId);
    }

    @Transactional
    public Map<String, Object> manualMatch(String reconcileRunId, ReconciliationRequests.ManualMatchRequest request) {
        repository.manualMatch(reconcileRunId, request);
        return Map.of("reconcileRunId", reconcileRunId, "status", "MATCHED");
    }

    @Transactional
    public Map<String, Object> flagBankException(String reconcileRunId, ReconciliationRequests.FlagExceptionRequest request) {
        repository.flagBankException(reconcileRunId, request);
        return Map.of("reconcileRunId", reconcileRunId, "status", "EXCEPTION_FLAGGED");
    }

    public Map<String, Object> listRefunds(String recoRunId) {
        return Map.of("items", repository.listRefunds(recoRunId));
    }

    public Map<String, Object> refundDetail(String refundId) {
        return repository.refundDetail(refundId);
    }

    @Transactional
    public Map<String, Object> retryRefund(String refundId) {
        repository.retryRefund(refundId);
        return Map.of("refundId", refundId, "status", "RETRY_QUEUED");
    }

    @Transactional
    public Map<String, Object> linkRefund(String refundId, ReconciliationRequests.LinkRefundRequest request) {
        repository.linkRefund(refundId, request.invoiceId());
        return Map.of("refundId", refundId, "invoiceId", request.invoiceId(), "linked", true);
    }

    public Map<String, Object> reportCatalog() {
        return Map.of("items", repository.reportCatalog());
    }

    public Map<String, Object> reportQuery(String slug, ReconciliationRequests.ReportQueryRequest request) {
        return Map.of("slug", slug, "rows", repository.reportQuery(slug, request.filters()));
    }

    @Transactional
    public Map<String, Object> reportExport(String slug, ReconciliationRequests.ReportExportRequest request) {
        String exportId = repository.reportExport(slug, request.format(), request.filters());
        return Map.of("exportId", exportId, "status", "QUEUED");
    }

    public Map<String, Object> reportExportStatus(String exportId) {
        return repository.reportExportStatus(exportId);
    }

    public Map<String, Object> listAudit(String entityType, String entityId, Integer limit, Integer page, Integer pageSize) {
        return Map.of("items", repository.listAudit(entityType, entityId, limit, page, pageSize));
    }

    public Map<String, Object> auditExport() {
        return Map.of("exportId", "AUD-" + Math.abs(UUID.randomUUID().hashCode()), "status", "QUEUED");
    }

    public Map<String, Object> getConfig(String type) {
        return Map.of("configType", type, "config", repository.getConfig(type));
    }

    @Transactional
    public Map<String, Object> updateConfig(String type, ReconciliationRequests.ConfigUpdateRequest request) {
        repository.updateConfig(type, request.config());
        return Map.of("configType", type, "updated", true);
    }

    public Map<String, Object> getEnterpriseConfigLookups(String tenantId) {
        Map<String, Object> lookups = new HashMap<>(enterpriseConfigRepository.getEnterpriseConfigLookups(tenantId));
        List<Map<String, Object>> perms = accessControlRepository.listRecoPermissionsForLookups(parseOptionalUuidForService(tenantId));
        lookups.put("recoPermissions", perms);
        lookups.put("accessPermissions", perms);
        return lookups;
    }

    public Map<String, Object> getGlMappingRuleLookupsBundle(String tenantId) {
        return enterpriseConfigRepository.getGlMappingRuleLookupsBundle(tenantId);
    }

    public Map<String, Object> listEnterpriseMatchingRules(
            String tenantId,
            String recoDomain,
            String stage,
            Boolean activeOnly,
            String search,
            String locationId,
            int page,
            int pageSize
    ) {
        return enterpriseConfigRepository.listEnterpriseMatchingRules(tenantId, recoDomain, stage, activeOnly, search, locationId, page, pageSize);
    }

    public Map<String, Object> getEnterpriseMatchingRuleDetail(String matchingRuleId) {
        return enterpriseConfigRepository.getEnterpriseMatchingRuleDetail(matchingRuleId);
    }

    public Map<String, Object> listEnterpriseGlMappingRules(
            String tenantId,
            Boolean activeOnly,
            String search,
            String locationId,
            String sourceType,
            String transactionType,
            String lineOfBusiness,
            String journalType,
            String itemCategory,
            String paymentMethodCode,
            String currencyCode,
            String status,
            String effectiveFrom,
            String effectiveTo,
            String refundType,
            int page,
            int pageSize
    ) {
        return enterpriseConfigRepository.listEnterpriseGlMappingRules(
                tenantId, activeOnly, search, locationId,
                sourceType, transactionType, lineOfBusiness, journalType, itemCategory,
                paymentMethodCode, currencyCode, status, effectiveFrom, effectiveTo, refundType,
                page, pageSize);
    }

    public Map<String, Object> getEnterpriseGlMappingRuleDetail(String tenantId, String glMappingRuleId) {
        return enterpriseConfigRepository.getEnterpriseGlMappingRuleDetail(tenantId, glMappingRuleId);
    }

    @Transactional
    public Map<String, Object> cloneGlMappingRule(String tenantId, String glMappingRuleId, String actorUserId) {
        return enterpriseConfigRepository.cloneGlMappingRule(tenantId, glMappingRuleId, actorUserId);
    }

    @Transactional
    public Map<String, Object> deactivateGlMappingRule(String tenantId, String glMappingRuleId, String actorUserId) {
        return enterpriseConfigRepository.deactivateGlMappingRule(tenantId, glMappingRuleId, actorUserId);
    }

    public Map<String, Object> glMappingRuleUsage(String glMappingRuleId, Integer limit) {
        int lim = limit == null ? 50 : limit;
        return Map.of("items", enterpriseConfigRepository.glMappingRuleUsage(glMappingRuleId, lim));
    }

    public Map<String, Object> exportGlMappingRules(String tenantId) {
        return Map.of("items", enterpriseConfigRepository.exportGlMappingRules(tenantId));
    }

    @Transactional
    public Map<String, Object> importGlMappingRules(String tenantId, ReconciliationRequests.GlMappingImportRequest request, String actorUserId) {
        return enterpriseConfigRepository.importGlMappingRules(tenantId, request.items(), actorUserId);
    }

    public Map<String, Object> previewGlMappingJournal(String tenantId, ReconciliationRequests.GlMappingJournalTestRequest request) {
        Map<String, Object> sample = request == null || request.sample() == null ? Map.of() : request.sample();
        return enterpriseConfigRepository.previewGlMappingJournal(tenantId, sample);
    }

    public Map<String, Object> listEnterpriseSchedules(
            String tenantId,
            String view,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String recoDomain,
            String stage,
            String level,
            String gatewayCode,
            String currencyCode,
            String paymentMethodCode,
            String sourceSystem,
            String frequency,
            String status,
            String from,
            String to,
            String locationId
    ) {
        return scheduleRepository.listEnterpriseSchedules(
                tenantId, view, activeOnly, search, page, pageSize,
                recoDomain, stage, level, gatewayCode, currencyCode, paymentMethodCode,
                sourceSystem, frequency, status, from, to, locationId);
    }

    public Map<String, Object> getEnterpriseScheduleDetail(String tenantId, String scheduleId) {
        return scheduleRepository.getEnterpriseScheduleDetail(tenantId, scheduleId);
    }

    public Map<String, Object> previewEnterpriseSchedule(String tenantId, Map<String, Object> body) {
        return scheduleRepository.previewSchedule(tenantId, body);
    }

    public Map<String, Object> listEnterpriseScheduleRuns(String scheduleId, int page, int pageSize) {
        List<Map<String, Object>> items = scheduleRepository.listScheduleRuns(scheduleId, page, pageSize);
        return Map.of("items", items, "page", page, "pageSize", pageSize);
    }

    @Transactional
    public Map<String, Object> runScheduleNow(String tenantId, String scheduleId, String actorUserId) {
        Map<String, Object> detail = scheduleRepository.getEnterpriseScheduleDetail(tenantId, scheduleId);
        if (!"OK".equals(detail.get("status"))) {
            return Map.of("status", "NOT_FOUND", "scheduleId", scheduleId);
        }
        Map<String, Object> filters = scheduleRepository.buildScheduleFiltersForRun(scheduleId, tenantId);
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) detail.get("row");
        Object ej = row.get("executionJson");
        if (ej instanceof Map<?, ?> em) {
            @SuppressWarnings("unchecked")
            Map<String, Object> exec = (Map<String, Object>) em;
            if (exec.get("financialPeriodScope") != null) {
                filters.put("financialPeriod", exec.get("financialPeriodScope"));
            }
            if (exec.get("currencyCode") != null) {
                filters.put("currency", exec.get("currencyCode"));
            }
        }
        String runType = "FULL";
        return repository.createReconciliationRun(runType, "SCHEDULED", actorUserId, tenantId, filters);
    }

    @Transactional
    public Map<String, Object> cloneEnterpriseSchedule(String tenantId, String scheduleId, String actorUserId) {
        return scheduleRepository.cloneSchedule(tenantId, scheduleId, actorUserId);
    }

    @Transactional
    public Map<String, Object> deactivateEnterpriseSchedule(String tenantId, String scheduleId, String actorUserId) {
        return scheduleRepository.deactivateSchedule(tenantId, scheduleId, actorUserId);
    }

    @Transactional
    public Map<String, Object> pauseAllEnterpriseSchedules(String tenantId) {
        int n = scheduleRepository.pauseAllSchedules(tenantId);
        return Map.of("updatedCount", n);
    }

    @Transactional
    public Map<String, Object> resumeAllEnterpriseSchedules(String tenantId) {
        int n = scheduleRepository.resumeAllSchedules(tenantId);
        return Map.of("updatedCount", n);
    }

    @Transactional
    public Map<String, Object> cancelScheduleQueuedRun(String runReference) {
        return scheduleRepository.cancelQueuedRun(runReference);
    }

    @Transactional
    public Map<String, Object> forceStartScheduleQueuedRun(String runReference) {
        return scheduleRepository.forceStartQueuedRun(runReference);
    }

    @Transactional
    public Map<String, Object> retryScheduleQueuedRun(String tenantId, String runReference, String actorUserId) {
        Map<String, Object> loaded = scheduleRepository.loadRunFiltersForRetry(runReference);
        if (loaded.isEmpty()) {
            return Map.of("status", "NOT_FOUND", "runReference", runReference);
        }
        String runType = loaded.get("runType") == null ? "FULL" : String.valueOf(loaded.get("runType"));
        return repository.createReconciliationRun(runType, "SCHEDULED", actorUserId, tenantId, loaded.get("filters"));
    }

    @Transactional
    public Map<String, Object> patchScheduleQueuePriority(String runReference, String priority) {
        return scheduleRepository.updateRunQueuePriority(runReference, priority);
    }

    public Map<String, Object> listEnterpriseConfigPublishes(String tenantId, int page, int pageSize) {
        return enterpriseConfigRepository.listEnterpriseConfigPublishes(tenantId, page, pageSize);
    }

    @Transactional
    public Map<String, Object> publishEnterpriseConfig(String tenantId, ReconciliationRequests.PublishRecoConfigRequest request) {
        Object snapshot = request.snapshot();
        if (snapshot == null || (snapshot instanceof Map<?, ?> m && m.isEmpty())) {
            snapshot = publishRepository.buildLiveSnapshot(parseOptionalUuidForService(tenantId));
        }
        return enterpriseConfigRepository.publishEnterpriseConfig(
                tenantId,
                request.versionLabel(),
                snapshot,
                request.publishedBy(),
                request.notes()
        );
    }

    private static UUID parseOptionalUuidForService(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Transactional
    public Map<String, Object> publishEnterpriseConfigIdempotent(
            String tenantId,
            String idempotencyKey,
            ReconciliationRequests.PublishRecoConfigRequest request
    ) {
        return executeIdempotent(
                tenantId,
                idempotencyKey,
                "POST",
                "/reconciliation/v1/config/enterprise/publishes",
                request,
                () -> publishEnterpriseConfig(tenantId, request)
        );
    }

    @Transactional
    public Map<String, Object> attachRunConfigPublish(String reconciliationRunId, ReconciliationRequests.AttachRunConfigPublishRequest request) {
        return enterpriseConfigRepository.attachConfigPublishToRun(reconciliationRunId, request.recoConfigPublishId());
    }

    @Transactional
    public Map<String, Object> createEnterpriseMatchingRule(String tenantId, ReconciliationRequests.EnterpriseMatchingRuleWriteRequest request, String actorUserId) {
        return enterpriseConfigRepository.createEnterpriseMatchingRule(tenantId, request, actorUserId);
    }

    @Transactional
    public Map<String, Object> updateEnterpriseMatchingRule(String tenantId, String matchingRuleId, ReconciliationRequests.EnterpriseMatchingRuleWriteRequest request, String actorUserId) {
        return enterpriseConfigRepository.updateEnterpriseMatchingRule(tenantId, matchingRuleId, request, actorUserId);
    }

    @Transactional
    public Map<String, Object> patchMatchingRuleActive(String tenantId, String matchingRuleId, ReconciliationRequests.MatchingRuleActivePatchRequest request) {
        return enterpriseConfigRepository.patchMatchingRuleActive(tenantId, matchingRuleId, request.active());
    }

    @Transactional
    public Map<String, Object> deleteEnterpriseMatchingRule(String tenantId, String matchingRuleId) {
        return enterpriseConfigRepository.softDeleteMatchingRule(tenantId, matchingRuleId);
    }

    @Transactional
    public Map<String, Object> upsertGlMappingRule(String tenantId, ReconciliationRequests.EnterpriseConfigRowRequest body, String idOrNull, String actorUserId) {
        return enterpriseConfigRepository.upsertGlMappingRule(tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> patchGlMappingActive(String tenantId, String id, ReconciliationRequests.MatchingRuleActivePatchRequest request) {
        return enterpriseConfigRepository.patchGlMappingActive(tenantId, id, request.active());
    }

    @Transactional
    public Map<String, Object> deleteGlMappingRule(String tenantId, String id) {
        return enterpriseConfigRepository.softDeleteGlMappingRule(tenantId, id);
    }

    @Transactional
    public Map<String, Object> upsertSchedule(String tenantId, ReconciliationRequests.EnterpriseConfigRowRequest body, String idOrNull, String actorUserId) {
        return scheduleRepository.upsertSchedule(tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> patchScheduleActive(String tenantId, String id, ReconciliationRequests.MatchingRuleActivePatchRequest request) {
        return scheduleRepository.patchScheduleActive(tenantId, id, request.active());
    }

    @Transactional
    public Map<String, Object> deleteSchedule(String tenantId, String id) {
        return scheduleRepository.softDeleteSchedule(tenantId, id);
    }

    public Map<String, Object> getEnterpriseThresholdsBundle(String tenantId) {
        return enterpriseConfigRepository.getEnterpriseThresholdsBundle(tenantId);
    }

    public Map<String, Object> listEnterpriseThresholdsByKind(
            String kind,
            String tenantId,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String applicationId,
            String recoDomain,
            String stage,
            String level,
            String gatewayCode,
            String currencyCode,
            String paymentMethodCode,
            String severity,
            String status,
            String effectiveFrom,
            String effectiveTo
    ) {
        return enterpriseConfigRepository.listEnterpriseThresholdsByKind(
                kind, tenantId, activeOnly, search, page, pageSize, applicationId,
                recoDomain, stage, level, gatewayCode, currencyCode, paymentMethodCode,
                severity, status, effectiveFrom, effectiveTo);
    }

    public Map<String, Object> getEnterpriseThresholdRow(String kind, String tenantId, String id) {
        return enterpriseConfigRepository.getEnterpriseThresholdRow(kind, tenantId, id);
    }

    @Transactional
    public Map<String, Object> upsertEnterpriseThreshold(String kind, String tenantId, ReconciliationRequests.EnterpriseConfigRowRequest body, String idOrNull, String actorUserId) {
        return enterpriseConfigRepository.upsertEnterpriseThreshold(kind, tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> deactivateEnterpriseThreshold(String kind, String tenantId, String id, String actorUserId) {
        return enterpriseConfigRepository.deactivateEnterpriseThreshold(kind, tenantId, id, actorUserId);
    }

    @Transactional
    public Map<String, Object> cloneEnterpriseThreshold(String kind, String tenantId, String id, String actorUserId) {
        return enterpriseConfigRepository.cloneEnterpriseThreshold(kind, tenantId, id, actorUserId);
    }

    public Map<String, Object> testEnterpriseThreshold(String kind, String tenantId, ReconciliationRequests.ThresholdTestRequest request) {
        Map<String, Object> sample = request == null || request.sample() == null ? Map.of() : request.sample();
        return enterpriseConfigRepository.testEnterpriseThreshold(kind, tenantId, sample);
    }

    public Map<String, Object> previewEnterpriseThresholdImpact(ReconciliationRequests.ThresholdPreviewImpactRequest request) {
        Map<String, Object> body = new HashMap<>();
        if (request != null) {
            if (request.kind() != null) {
                body.put("kind", request.kind());
            }
            if (request.filters() != null) {
                body.put("filters", request.filters());
            }
            if (request.search() != null) {
                body.put("search", request.search());
            }
        }
        return enterpriseConfigRepository.previewEnterpriseThresholdImpact(body);
    }

    public Map<String, Object> listThresholdAuditEvents(String kind, String id) {
        return Map.of("items", enterpriseConfigRepository.listThresholdAuditEvents(kind, id));
    }

    public Map<String, Object> getEnterpriseConfigDraftSummary(String tenantId) {
        return enterpriseConfigRepository.getEnterpriseConfigDraftSummary(tenantId);
    }

    /**
     * Shell metadata for the Flutter reconciliation control center (navigation and route hints).
     */
    public Map<String, Object> getEnterpriseControlCenterShell() {
        return Map.of(
                "title", "Reconciliation Control Center",
                "apiVersion", "v1",
                "defaultTab", "overview",
                "tabs", List.of(
                        Map.of("id", "overview", "label", "Overview", "path", "/reconciliation/v1/config/enterprise/control-center/overview"),
                        Map.of("id", "matching-rules", "label", "Matching rules", "path", "/reconciliation/v1/config/enterprise/matching-rules"),
                        Map.of("id", "gl-mapping", "label", "GL mapping", "path", "/reconciliation/v1/config/enterprise/gl-mapping-rules"),
                        Map.of("id", "schedules", "label", "Schedules", "path", "/reconciliation/v1/config/enterprise/schedules"),
                        Map.of("id", "publishes", "label", "Publishes", "path", "/reconciliation/v1/config/enterprise/publishes"),
                        Map.of("id", "audit", "label", "Audit", "path", "/reconciliation/v1/audit")
                )
        );
    }

    /**
     * Overview metrics for the control center; composes relational draft counts with placeholder KPI slots for FE.
     */
    public Map<String, Object> getEnterpriseControlCenterOverview(String tenantId) {
        Map<String, Object> draft = enterpriseConfigRepository.getEnterpriseConfigDraftSummary(tenantId);
        Map<String, Object> kpis = new HashMap<>();
        kpis.put("openExceptionsApprox", 0);
        kpis.put("lastPublishAt", null);
        Map<String, Object> body = new HashMap<>();
        body.put("draftSummary", draft == null ? Map.of() : draft);
        body.put("kpis", kpis);
        return body;
    }

    public Map<String, Object> compareEnterprisePublishes(String aId, String bId) {
        return enterpriseConfigRepository.compareEnterprisePublishes(aId, bId);
    }

    @Transactional
    public Map<String, Object> rollbackEnterprisePublish(String tenantId, String fromPublishId) {
        return enterpriseConfigRepository.rollbackEnterprisePublish(tenantId, fromPublishId);
    }

    public Map<String, Object> testMatchingRulePreview(String matchingRuleId, ReconciliationRequests.MatchingRuleTestRequest request) {
        Map<String, Object> sample = (request == null || request.samplePayload() == null) ? Map.of() : request.samplePayload();
        return enterpriseConfigRepository.testMatchingRulePreview(matchingRuleId, sample);
    }

    public Map<String, Object> listEnterpriseRoles(String tenantId) {
        return Map.of("items", enterpriseConfigRepository.listEnterpriseRoles(tenantId));
    }

    public Map<String, Object> listEnterpriseNotificationRules(String tenantId) {
        return Map.of("items", enterpriseConfigRepository.listEnterpriseNotificationRules(tenantId));
    }

    public Map<String, Object> listEnterpriseNotifications(
            String tenantId,
            String view,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String notificationEvent,
            String channel,
            String severity,
            String recoDomain,
            String stage,
            String gatewayCode,
            String status
    ) {
        return notificationRepository.listEnterpriseNotifications(
                tenantId, view, activeOnly, search, page, pageSize,
                notificationEvent, channel, severity, recoDomain, stage, gatewayCode, status);
    }

    public Map<String, Object> getEnterpriseNotificationRule(String tenantId, String id) {
        return notificationRepository.getNotificationRuleDetail(tenantId, id);
    }

    @Transactional
    public Map<String, Object> upsertEnterpriseNotificationRule(
            String tenantId,
            ReconciliationRequests.EnterpriseConfigRowRequest body,
            String idOrNull,
            String actorUserId
    ) {
        return notificationRepository.upsertNotificationRule(tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> deactivateEnterpriseNotificationRule(String tenantId, String id, String actorUserId) {
        return notificationRepository.deactivateNotificationRule(tenantId, id, actorUserId);
    }

    @Transactional
    public Map<String, Object> cloneEnterpriseNotificationRule(String tenantId, String id, String actorUserId) {
        return notificationRepository.cloneNotificationRule(tenantId, id, actorUserId);
    }

    public Map<String, Object> testEnterpriseNotification(String tenantId, Map<String, Object> body) {
        return notificationRepository.testNotification(tenantId, body);
    }

    public Map<String, Object> previewEnterpriseNotification(String tenantId, Map<String, Object> body) {
        return notificationRepository.previewNotificationPayload(tenantId, body);
    }

    public Map<String, Object> previewEnterpriseNotificationRecipients(String tenantId, Map<String, Object> body) {
        return notificationRepository.previewRecipients(tenantId, body);
    }

    @Transactional
    public Map<String, Object> sendEnterpriseTestAlert(String tenantId, Map<String, Object> body, String actorUserId) {
        return notificationRepository.sendTestAlert(tenantId, body, actorUserId);
    }

    @Transactional
    public Map<String, Object> retryFailedEnterpriseNotification(String notificationId) {
        return notificationRepository.retryFailedNotification(notificationId);
    }

    @Transactional
    public Map<String, Object> resendEnterpriseNotificationHistory(String historyId) {
        return notificationRepository.resendHistoryNotification(historyId);
    }

    public Map<String, Object> listEnterpriseAccessControl(
            String tenantId,
            String view,
            Boolean activeOnly,
            String search,
            int page,
            int pageSize,
            String roleId,
            String userId,
            String recoDomain,
            String stage,
            String level,
            String permissionCode,
            String status
    ) {
        return accessControlRepository.listEnterpriseAccessControl(
                tenantId, view, activeOnly, search, page, pageSize,
                roleId, userId, recoDomain, stage, level, permissionCode, status);
    }

    public Map<String, Object> getEnterpriseAccessRole(String tenantId, String id) {
        return accessControlRepository.getRoleDetail(tenantId, id);
    }

    @Transactional
    public Map<String, Object> upsertEnterpriseAccessRole(
            String tenantId,
            ReconciliationRequests.EnterpriseConfigRowRequest body,
            String idOrNull,
            String actorUserId
    ) {
        return accessControlRepository.upsertRole(tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> deactivateEnterpriseAccessRole(String tenantId, String id, String actorUserId) {
        return accessControlRepository.deactivateRole(tenantId, id, actorUserId);
    }

    @Transactional
    public Map<String, Object> cloneEnterpriseAccessRole(String tenantId, String id, String actorUserId) {
        return accessControlRepository.cloneRole(tenantId, id, actorUserId);
    }

    @Transactional
    public Map<String, Object> assignEnterpriseAccessUser(
            String tenantId,
            ReconciliationRequests.EnterpriseConfigRowRequest body,
            String actorUserId
    ) {
        return accessControlRepository.assignUser(tenantId, body.row(), actorUserId);
    }

    public Map<String, Object> previewEnterpriseEffectiveAccess(String tenantId, Map<String, Object> body) {
        return accessControlRepository.previewEffectiveAccess(tenantId, body);
    }

    public Map<String, Object> previewEnterpriseAccessImpact(String tenantId, Map<String, Object> body) {
        return accessControlRepository.previewImpact(tenantId, body);
    }

    public Map<String, Object> exportEnterpriseAccessMatrix(String tenantId) {
        return accessControlRepository.exportAccessMatrix(tenantId);
    }

    @Transactional
    public Map<String, Object> acknowledgeEnterpriseSodViolation(String violationId, String actorUserId) {
        return accessControlRepository.acknowledgeSodViolation(violationId, actorUserId);
    }

    public Map<String, Object> listEnterprisePublish(
            String tenantId,
            String view,
            String search,
            int page,
            int pageSize,
            String environment,
            String version,
            String publishedBy,
            String publishStatus,
            String from,
            String to,
            String versionA,
            String versionB
    ) {
        return publishRepository.listEnterprisePublish(
                tenantId, view, search, page, pageSize, environment, version,
                publishedBy, publishStatus, from, to, versionA, versionB);
    }

    @Transactional
    public Map<String, Object> saveEnterprisePublishDraft(String tenantId, Map<String, Object> body, String actorUserId) {
        return publishRepository.saveDraft(tenantId, body, actorUserId);
    }

    public Map<String, Object> previewEnterprisePublishDraft(String tenantId, Map<String, Object> body) {
        return publishRepository.previewDraft(tenantId, body);
    }

    public Map<String, Object> validateEnterprisePublish(String tenantId, Map<String, Object> body) {
        return publishRepository.validatePublish(tenantId, body);
    }

    public Map<String, Object> previewEnterprisePublishImpact(String tenantId, Map<String, Object> body) {
        return publishRepository.previewImpact(tenantId, body);
    }

    @Transactional
    public Map<String, Object> executeEnterprisePublish(
            String tenantId, Map<String, Object> body, String actorUserId, String idempotencyKey
    ) {
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            return executeIdempotent(
                    tenantId,
                    idempotencyKey,
                    "POST",
                    "/reconciliation/v1/config/enterprise/publish/execute",
                    body,
                    () -> publishRepository.executePublish(tenantId, body, actorUserId)
            );
        }
        return publishRepository.executePublish(tenantId, body, actorUserId);
    }

    public boolean isEnterprisePublishFrozen(String tenantId, Map<String, Object> body) {
        UUID tenantUuid = parseOptionalUuidForService(tenantId);
        String env = body == null || body.get("environment") == null ? "PROD" : String.valueOf(body.get("environment"));
        Map<String, Object> check = publishRepository.checkFreezeWindow(tenantUuid, env);
        return Boolean.TRUE.equals(check.get("frozen"));
    }

    public Map<String, Object> getEnterprisePublishFreezeWindow(String tenantId, Map<String, Object> body) {
        UUID tenantUuid = parseOptionalUuidForService(tenantId);
        String env = body == null || body.get("environment") == null ? "PROD" : String.valueOf(body.get("environment"));
        return publishRepository.checkFreezeWindow(tenantUuid, env);
    }

    @Transactional
    public Map<String, Object> rollbackEnterprisePublishConfig(String tenantId, Map<String, Object> body, String actorUserId) {
        return publishRepository.rollbackPublish(tenantId, body, actorUserId);
    }

    @Transactional
    public Map<String, Object> promoteEnterprisePublish(String tenantId, Map<String, Object> body, String actorUserId) {
        return publishRepository.promoteEnvironment(tenantId, body, actorUserId);
    }

    public Map<String, Object> exportEnterprisePublishSnapshot(String tenantId, String version) {
        return publishRepository.exportSnapshot(tenantId, version);
    }

    @Transactional
    public Map<String, Object> approveEnterpriseDraftChange(String draftChangeId, Map<String, Object> body, String actorUserId) {
        String comment = body == null ? null : body.get("comment") == null ? null : String.valueOf(body.get("comment"));
        return publishRepository.approveDraftChange(draftChangeId, comment, actorUserId);
    }

    @Transactional
    public Map<String, Object> rejectEnterpriseDraftChange(String draftChangeId, Map<String, Object> body, String actorUserId) {
        String comment = body == null ? null : body.get("comment") == null ? null : String.valueOf(body.get("comment"));
        return publishRepository.rejectDraftChange(draftChangeId, comment, actorUserId);
    }

    @Transactional
    public Map<String, Object> discardEnterpriseDraftChange(String draftChangeId, Map<String, Object> body, String actorUserId) {
        String comment = body == null ? null : body.get("comment") == null ? null : String.valueOf(body.get("comment"));
        return publishRepository.discardDraftChange(draftChangeId, comment, actorUserId);
    }

    private Map<String, Object> executeIdempotent(
            String tenantId,
            String idempotencyKey,
            String method,
            String path,
            Object requestPayload,
            Supplier<Map<String, Object>> action
    ) {
        validateIdempotencyKey(idempotencyKey);
        String requestHash = hash(requestPayload);
        Map<String, Object> existing = repository.findIdempotency(tenantId, idempotencyKey);
        if (existing != null) {
            String existingHash = String.valueOf(existing.getOrDefault("requestHash", ""));
            if (!existingHash.equals(requestHash)) {
                throw new IllegalStateException("Idempotency key reused with different payload");
            }
            Object body = existing.get("responseBodyJson");
            if (body instanceof Map<?, ?> bodyMap) {
                @SuppressWarnings("unchecked")
                Map<String, Object> typed = (Map<String, Object>) bodyMap;
                return typed;
            }
        }

        repository.reserveIdempotency(tenantId, idempotencyKey, method, path, requestHash, requestPayload);
        Map<String, Object> response = action.get();
        String ref = detectResponseReference(response);
        repository.completeIdempotency(tenantId, idempotencyKey, 200, response, ref);
        return response;
    }

    private void validateIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("Idempotency-Key header is required");
        }
    }

    private String hash(Object payload) {
        try {
            return Integer.toHexString(objectMapper.writeValueAsString(payload).hashCode());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to hash idempotent payload", e);
        }
    }

    private String detectResponseReference(Map<String, Object> response) {
        if (response.containsKey("reconciliationRunId")) return String.valueOf(response.get("reconciliationRunId"));
        if (response.containsKey("exceptionId")) return String.valueOf(response.get("exceptionId"));
        if (response.containsKey("jobId")) return String.valueOf(response.get("jobId"));
        if (response.containsKey("exportId")) return String.valueOf(response.get("exportId"));
        if (response.containsKey("recoConfigPublishId")) return String.valueOf(response.get("recoConfigPublishId"));
        return null;
    }
}
