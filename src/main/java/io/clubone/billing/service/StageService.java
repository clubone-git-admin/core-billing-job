package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for stage operations.
 */
@Service
public class StageService {

    private static final Logger log = LoggerFactory.getLogger(StageService.class);

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;

    public StageService(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
    }

    public List<StageRunDto> listStages(UUID billingRunId) {
        return stageRunRepository.findByBillingRunId(billingRunId);
    }

    public StageRunDto getStage(UUID billingRunId, String stageCode) {
        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
    }

    @Transactional
    public StageRunDto startStage(UUID billingRunId, String stageCode, StartStageRequest request) {
        // Validate billing run exists
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            throw new IllegalArgumentException("Billing run not found: " + billingRunId);
        }

        // Check if stage already exists
        StageRunDto existing = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
        UUID stageRunId;
        
        if (existing != null) {
            stageRunId = existing.stageRunId();
        } else {
            // Create new stage run
            stageRunId = stageRunRepository.createStageRun(
                    billingRunId, stageCode, request.scheduledFor(),
                    request.idempotencyKey(), null);
        }

        // Start the stage
        stageRunRepository.startStageRun(stageRunId);

        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
    }

    @Transactional
    public StageRunDto completeStage(UUID billingRunId, String stageCode, CompleteStageRequest request) {
        StageRunDto stage = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
        if (stage == null) {
            return null;
        }

        stageRunRepository.completeStageRun(stage.stageRunId(), request.summaryJson());

        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
    }

    @Transactional
    public StageRunDto failStage(UUID billingRunId, String stageCode, FailStageRequest request) {
        StageRunDto stage = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
        if (stage == null) {
            return null;
        }

        stageRunRepository.failStageRun(
                stage.stageRunId(), request.errorMessage(), request.errorDetails());

        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
    }

    @Transactional
    public StageRunDto retryStage(UUID billingRunId, String stageCode, RetryStageRequest request) {
        StageRunDto stage = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
        if (stage == null) {
            return null;
        }

        // Create a new stage run with incremented attempt number
        UUID newStageRunId = stageRunRepository.createStageRun(
                billingRunId, stageCode, OffsetDateTime.now(),
                null, null);

        // Start the new stage run
        stageRunRepository.startStageRun(newStageRunId);

        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, stageCode);
    }

    /**
     * Complete a stage run by stage run ID.
     *
     * @param stageRunId The stage run ID to complete
     * @param summaryJson Optional summary JSON to store
     * @return The updated stage run DTO, or null if not found
     */
    @Transactional
    public StageRunDto completeStageRunById(UUID stageRunId, Map<String, Object> summaryJson) {
        StageRunDto stage = stageRunRepository.findById(stageRunId);
        if (stage == null) {
            return null;
        }

        stageRunRepository.completeStageRun(stageRunId, summaryJson);

        return stageRunRepository.findById(stageRunId);
    }
}
