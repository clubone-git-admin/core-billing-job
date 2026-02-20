package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.StageService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for stage operations.
 * Provides operations for managing billing run stages.
 */
@RestController
@RequestMapping("/api/v1/billing/runs/{billingRunId}/stages")
public class StageOperationsController {

    private static final Logger log = LoggerFactory.getLogger(StageOperationsController.class);

    private final StageService stageService;

    public StageOperationsController(StageService stageService) {
        this.stageService = stageService;
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/stages
     * List all stages for a billing run.
     */
    @GetMapping
    public ResponseEntity<List<StageRunDto>> listStages(@PathVariable UUID billingRunId) {
        log.debug("Listing stages for billing run: billingRunId={}", billingRunId);
        
        List<StageRunDto> stages = stageService.listStages(billingRunId);
        return ResponseEntity.ok(stages);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/stages/{stage_code}
     * Get a specific stage by code.
     */
    @GetMapping("/{stageCode}")
    public ResponseEntity<StageRunDto> getStage(
            @PathVariable UUID billingRunId,
            @PathVariable String stageCode) {
        
        log.debug("Getting stage: billingRunId={}, stageCode={}", billingRunId, stageCode);
        
        StageRunDto stage = stageService.getStage(billingRunId, stageCode);
        if (stage == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(stage);
    }

    /**
     * POST /api/v1/billing/runs/{billing_run_id}/stages/{stage_code}/start
     * Start a stage.
     */
    @PostMapping("/{stageCode}/start")
    public ResponseEntity<StageRunDto> startStage(
            @PathVariable UUID billingRunId,
            @PathVariable String stageCode,
            @Valid @RequestBody StartStageRequest request) {
        
        log.info("Starting stage: billingRunId={}, stageCode={}", billingRunId, stageCode);
        
        try {
            StageRunDto stage = stageService.startStage(billingRunId, stageCode, request);
            return ResponseEntity.ok(stage);
        } catch (IllegalStateException e) {
            // Prerequisites not met
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * POST /api/v1/billing/runs/{billing_run_id}/stages/{stage_code}/complete
     * Complete a stage.
     */
    @PostMapping("/{stageCode}/complete")
    public ResponseEntity<StageRunDto> completeStage(
            @PathVariable UUID billingRunId,
            @PathVariable String stageCode,
            @Valid @RequestBody CompleteStageRequest request) {
        
        log.info("Completing stage: billingRunId={}, stageCode={}", billingRunId, stageCode);
        
        StageRunDto stage = stageService.completeStage(billingRunId, stageCode, request);
        if (stage == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(stage);
    }

    /**
     * POST /api/v1/billing/runs/{billing_run_id}/stages/{stage_code}/fail
     * Fail a stage.
     */
    @PostMapping("/{stageCode}/fail")
    public ResponseEntity<StageRunDto> failStage(
            @PathVariable UUID billingRunId,
            @PathVariable String stageCode,
            @Valid @RequestBody FailStageRequest request) {
        
        log.info("Failing stage: billingRunId={}, stageCode={}", billingRunId, stageCode);
        
        StageRunDto stage = stageService.failStage(billingRunId, stageCode, request);
        if (stage == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(stage);
    }

    /**
     * POST /api/v1/billing/runs/{billing_run_id}/stages/{stage_code}/retry
     * Retry a stage.
     */
    @PostMapping("/{stageCode}/retry")
    public ResponseEntity<StageRunDto> retryStage(
            @PathVariable UUID billingRunId,
            @PathVariable String stageCode,
            @Valid @RequestBody RetryStageRequest request) {
        
        log.info("Retrying stage: billingRunId={}, stageCode={}", billingRunId, stageCode);
        
        StageRunDto stage = stageService.retryStage(billingRunId, stageCode, request);
        if (stage == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(stage);
    }

}
