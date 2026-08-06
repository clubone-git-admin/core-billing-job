package io.clubone.billing.service;

import io.clubone.billing.api.dto.DLQItemDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import io.clubone.billing.service.invoicegen.InvoiceGenerationStageDlqSummaryService;
import io.clubone.billing.util.BillingReadExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Service for Dead Letter Queue operations.
 */
@Service
public class DLQService {

    private static final Logger log = LoggerFactory.getLogger(DLQService.class);

    private final DLQRepository dlqRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService;
    private final BillingReadExecutors readExecutors;
    /** Spring proxy — bulk ops call single-item {@code @Transactional} methods correctly. */
    private final DLQService self;

    public DLQService(
            DLQRepository dlqRepository,
            LocationLevelRepository locationLevelRepository,
            InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService,
            BillingReadExecutors readExecutors,
            @Lazy DLQService self) {
        this.dlqRepository = dlqRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.invoiceGenerationStageDlqSummaryService = invoiceGenerationStageDlqSummaryService;
        this.readExecutors = readExecutors;
        this.self = self;
    }

    /** Parallel list+count — no outer TX (workers need separate pool connections). */
    public PageResponse<DLQItemDto> listDLQItems(
            UUID billingRunId,
            UUID locationLevelId,
            Boolean includeChildLocations,
            UUID stageRunId,
            String failureTypeCode,
            String errorType,
            Boolean resolved,
            Integer limit, Integer offset, String sortBy, String sortOrder) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);
        return listDLQItemsParallel(
                billingRunId, stageRunId, locationIds, failureTypeCode, errorType, resolved,
                limit, offset, sortBy, sortOrder);
    }

    private PageResponse<DLQItemDto> listDLQItemsParallel(
            UUID billingRunId,
            UUID stageRunId,
            List<UUID> locationIds,
            String failureTypeCode,
            String errorType,
            Boolean resolved,
            Integer limit,
            Integer offset,
            String sortBy,
            String sortOrder) {
        CompletableFuture<List<DLQItemDto>> itemsF = readExecutors.supplyAsync(() ->
                dlqRepository.findDLQItems(
                        billingRunId,
                        stageRunId,
                        locationIds,
                        failureTypeCode,
                        errorType,
                        resolved,
                        limit,
                        offset,
                        sortBy,
                        sortOrder));
        CompletableFuture<Integer> totalF = readExecutors.supplyAsync(() ->
                dlqRepository.countDLQItems(
                        billingRunId, stageRunId, locationIds, failureTypeCode, errorType, resolved));
        CompletableFuture.allOf(itemsF, totalF).join();
        return PageResponse.of(itemsF.join(), totalF.join(), limit, offset);
    }

    private List<UUID> resolveLocationIds(UUID locationLevelId, Boolean includeChildLocations) {
        if (locationLevelId == null) {
            return List.of();
        }
        boolean includeChildren = includeChildLocations == null || includeChildLocations;
        return locationLevelRepository
                .resolveLocationsForLevel(locationLevelId, includeChildren)
                .stream()
                .map(LocationLevelRepository.LocationRow::locationId)
                .toList();
    }

    @Transactional(readOnly = true)
    public DLQItemDto getDLQItem(UUID dlqId) {
        return dlqRepository.findById(dlqId);
    }

    @Transactional
    public DLQItemDto retryDLQItem(UUID dlqId, Map<String, Object> retryConfig) {
        DLQItemDto item = dlqRepository.findById(dlqId);
        if (item == null) {
            return null;
        }

        dlqRepository.updateRetry(dlqId, retryConfig);

        return dlqRepository.findById(dlqId);
    }

    @Transactional
    public DLQItemDto resolveDLQItem(UUID dlqId, Map<String, Object> request) {
        DLQItemDto item = dlqRepository.findById(dlqId);
        if (item == null) {
            return null;
        }

        String resolvedBy = (String) request.getOrDefault("resolved_by", "system");
        String resolutionNotes = (String) request.getOrDefault("resolution_notes", "");

        dlqRepository.resolve(dlqId, resolvedBy, resolutionNotes);

        if (item.stageRunId() != null) {
            invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(item.stageRunId());
        }

        return dlqRepository.findById(dlqId);
    }

    /**
     * Not {@code @Transactional}: each item runs in its own TX via {@link #self} so one failure
     * does not roll back the whole bulk, and connections are not held for the full loop.
     */
    public Map<String, Object> bulkRetryDLQItems(Map<String, Object> request) {
        @SuppressWarnings("unchecked")
        List<String> dlqIds = (List<String>) request.getOrDefault("dlq_ids", Collections.emptyList());
        @SuppressWarnings("unchecked")
        Map<String, Object> retryConfig = (Map<String, Object>) request.getOrDefault("retry_config", Map.of());

        int retried = 0;
        int failed = 0;
        List<Map<String, Object>> results = new ArrayList<>();

        for (String dlqIdStr : dlqIds) {
            try {
                UUID dlqId = UUID.fromString(dlqIdStr);
                DLQItemDto item = self.retryDLQItem(dlqId, retryConfig);
                if (item != null) {
                    retried++;
                    results.add(Map.of(
                            "dlq_id", dlqIdStr,
                            "status", "retried",
                            "new_retry_count", item.retryCount()
                    ));
                } else {
                    failed++;
                    results.add(Map.of(
                            "dlq_id", dlqIdStr,
                            "status", "failed",
                            "error", "DLQ item not found"
                    ));
                }
            } catch (Exception e) {
                failed++;
                log.error("Failed to retry DLQ item {}: {}", dlqIdStr, e.getMessage(), e);
                results.add(Map.of(
                        "dlq_id", dlqIdStr,
                        "status", "failed",
                        "error", e.getMessage()
                ));
            }
        }

        return Map.of(
                "retried", retried,
                "failed", failed,
                "results", results
        );
    }

    /**
     * Not {@code @Transactional}: per-item TX via {@link #self}.
     */
    public Map<String, Object> bulkResolveDLQItems(Map<String, Object> request) {
        @SuppressWarnings("unchecked")
        List<String> dlqIds = (List<String>) request.getOrDefault("dlq_ids", Collections.emptyList());
        String resolvedBy = (String) request.getOrDefault("resolved_by", "system");
        String resolutionNotes = (String) request.getOrDefault("resolution_notes", "");
        String resolutionAction = (String) request.getOrDefault("resolution_action", "MANUAL_FIX");

        int resolved = 0;
        int failed = 0;
        List<Map<String, Object>> results = new ArrayList<>();

        for (String dlqIdStr : dlqIds) {
            try {
                UUID dlqId = UUID.fromString(dlqIdStr);
                Map<String, Object> resolveRequest = Map.of(
                        "resolved_by", resolvedBy,
                        "resolution_notes", resolutionNotes,
                        "resolution_action", resolutionAction
                );
                DLQItemDto item = self.resolveDLQItem(dlqId, resolveRequest);
                if (item != null && item.resolved()) {
                    resolved++;
                    results.add(Map.of(
                            "dlq_id", dlqIdStr,
                            "status", "resolved"
                    ));
                } else {
                    failed++;
                    results.add(Map.of(
                            "dlq_id", dlqIdStr,
                            "status", "failed",
                            "error", "Could not resolve DLQ item"
                    ));
                }
            } catch (Exception e) {
                failed++;
                log.error("Failed to resolve DLQ item {}: {}", dlqIdStr, e.getMessage(), e);
                results.add(Map.of(
                        "dlq_id", dlqIdStr,
                        "status", "failed",
                        "error", e.getMessage()
                ));
            }
        }

        return Map.of(
                "resolved", resolved,
                "failed", failed,
                "results", results
        );
    }
}
