package io.clubone.billing.service;

import io.clubone.billing.api.dto.DLQItemDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.repo.DLQRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Service for Dead Letter Queue operations.
 */
@Service
public class DLQService {

    private static final Logger log = LoggerFactory.getLogger(DLQService.class);

    private final DLQRepository dlqRepository;

    public DLQService(DLQRepository dlqRepository) {
        this.dlqRepository = dlqRepository;
    }

    public PageResponse<DLQItemDto> listDLQItems(
            UUID billingRunId, String failureTypeCode, Boolean resolved,
            Integer limit, Integer offset, String sortBy, String sortOrder) {

        List<DLQItemDto> items = dlqRepository.findDLQItems(
                billingRunId, failureTypeCode, resolved, limit, offset, sortBy, sortOrder);

        Integer total = dlqRepository.countDLQItems(billingRunId, failureTypeCode, resolved);

        return PageResponse.of(items, total, limit, offset);
    }

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

        return dlqRepository.findById(dlqId);
    }

    @Transactional
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
                DLQItemDto item = retryDLQItem(dlqId, retryConfig);
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

    @Transactional
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
                DLQItemDto item = resolveDLQItem(dlqId, resolveRequest);
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
