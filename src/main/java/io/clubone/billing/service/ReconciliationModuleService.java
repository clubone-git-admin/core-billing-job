package io.clubone.billing.service;

import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import io.clubone.billing.repo.ReconciliationEnterpriseConfigRepository;
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
    private final ObjectMapper objectMapper;

    public ReconciliationModuleService(
            ReconciliationModuleRepository repository,
            ReconciliationEnterpriseConfigRepository enterpriseConfigRepository,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.enterpriseConfigRepository = enterpriseConfigRepository;
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
        repository.addExceptionNote(exceptionId, request.note());
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

    public Map<String, Object> listAudit(String entityType, String entityId, Integer limit) {
        return Map.of("items", repository.listAudit(entityType, entityId, limit));
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

    public Map<String, Object> getEnterpriseConfigLookups() {
        return enterpriseConfigRepository.getEnterpriseConfigLookups();
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
            String tenantId, Boolean activeOnly, String search, String locationId, int page, int pageSize
    ) {
        return enterpriseConfigRepository.listEnterpriseGlMappingRules(tenantId, activeOnly, search, locationId, page, pageSize);
    }

    public Map<String, Object> listEnterpriseSchedules(
            String tenantId, Boolean activeOnly, String search, String locationId, int page, int pageSize
    ) {
        return enterpriseConfigRepository.listEnterpriseSchedules(tenantId, activeOnly, search, locationId, page, pageSize);
    }

    public Map<String, Object> listEnterpriseConfigPublishes(String tenantId, int page, int pageSize) {
        return enterpriseConfigRepository.listEnterpriseConfigPublishes(tenantId, page, pageSize);
    }

    @Transactional
    public Map<String, Object> publishEnterpriseConfig(String tenantId, ReconciliationRequests.PublishRecoConfigRequest request) {
        return enterpriseConfigRepository.publishEnterpriseConfig(
                tenantId,
                request.versionLabel(),
                request.snapshot(),
                request.publishedBy(),
                request.notes()
        );
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
        return enterpriseConfigRepository.upsertSchedule(tenantId, body.row(), idOrNull, actorUserId);
    }

    @Transactional
    public Map<String, Object> patchScheduleActive(String tenantId, String id, ReconciliationRequests.MatchingRuleActivePatchRequest request) {
        return enterpriseConfigRepository.patchScheduleActive(tenantId, id, request.active());
    }

    @Transactional
    public Map<String, Object> deleteSchedule(String tenantId, String id) {
        return enterpriseConfigRepository.softDeleteSchedule(tenantId, id);
    }

    public Map<String, Object> getEnterpriseThresholdsBundle(String tenantId) {
        return enterpriseConfigRepository.getEnterpriseThresholdsBundle(tenantId);
    }

    public Map<String, Object> getEnterpriseConfigDraftSummary(String tenantId) {
        return enterpriseConfigRepository.getEnterpriseConfigDraftSummary(tenantId);
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
