package io.clubone.billing.service;

import io.clubone.billing.repo.SnapshotRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for snapshot operations.
 */
@Service
public class SnapshotService {

    private final SnapshotRepository snapshotRepository;

    public SnapshotService(SnapshotRepository snapshotRepository) {
        this.snapshotRepository = snapshotRepository;
    }

    @Transactional
    public Map<String, Object> createSnapshot(UUID billingRunId, Map<String, Object> request) {
        String snapshotTypeCode = (String) request.get("snapshot_type_code");
        String stageCode = (String) request.get("stage_code");
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshotData = (Map<String, Object>) request.getOrDefault("snapshot_data", Map.of());
        String s3Path = (String) request.get("s3_path");
        String createdByStr = (String) request.getOrDefault("created_by", "");
        UUID createdBy = null;
        try {
            createdBy = UUID.fromString(createdByStr);
        } catch (Exception e) {
            // Use null if invalid
        }

        UUID snapshotId = snapshotRepository.createSnapshot(
                billingRunId, snapshotTypeCode, stageCode, snapshotData, s3Path, createdBy);

        return getSnapshot(snapshotId);
    }

    public Map<String, Object> listSnapshots(UUID billingRunId, String snapshotTypeCode, String stageCode) {
        List<Map<String, Object>> snapshots = snapshotRepository.findByBillingRunId(
                billingRunId, snapshotTypeCode, stageCode);

        List<Map<String, Object>> snapshotList = snapshots.stream()
                .map(this::formatSnapshot)
                .collect(Collectors.toList());

        return Map.of("data", snapshotList);
    }

    public Map<String, Object> getSnapshot(UUID snapshotId) {
        Map<String, Object> snapshot = snapshotRepository.findById(snapshotId);
        if (snapshot == null) {
            return null;
        }
        return formatSnapshotWithData(snapshot);
    }

    private Map<String, Object> formatSnapshot(Map<String, Object> snapshot) {
        return Map.of(
                "snapshot_id", snapshot.get("snapshot_id"),
                "billing_run_id", snapshot.get("billing_run_id"),
                "snapshot_type", Map.of(
                        "snapshot_type_code", snapshot.get("snapshot_type_code"),
                        "display_name", snapshot.get("snapshot_type_display_name")
                ),
                "stage_code", snapshot.getOrDefault("stage_code", ""),
                "s3_path", snapshot.getOrDefault("s3_path", ""),
                "created_on", snapshot.get("created_on")
        );
    }

    private Map<String, Object> formatSnapshotWithData(Map<String, Object> snapshot) {
        Map<String, Object> snapshotData = new HashMap<>();
        String snapshotDataStr = (String) snapshot.get("snapshot_data");
        if (snapshotDataStr != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                snapshotData = mapper.readValue(snapshotDataStr, Map.class);
            } catch (Exception e) {
                // Keep empty map
            }
        }

        Map<String, Object> result = new HashMap<>(formatSnapshot(snapshot));
        result.put("snapshot_data", snapshotData);
        return result;
    }
}

