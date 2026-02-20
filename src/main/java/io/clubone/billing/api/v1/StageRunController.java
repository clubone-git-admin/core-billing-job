package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.service.StageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for stage run operations by stage run ID.
 * Provides direct operations on stage runs without requiring billing run ID.
 */
@RestController
@RequestMapping("/api/v1/billing/stage-runs")
@Tag(name = "Stage Runs", description = "API for managing stage runs directly by stage run ID")
public class StageRunController {

    private static final Logger log = LoggerFactory.getLogger(StageRunController.class);

    private final StageService stageService;

    public StageRunController(StageService stageService) {
        this.stageService = stageService;
    }

    /**
     * POST /api/v1/billing/stage-runs/{stageRunId}/complete
     * Mark a stage run as COMPLETED by stage run ID.
     */
    @PostMapping("/{stageRunId}/complete")
    @Operation(
            summary = "Complete stage run by ID",
            description = "Marks a stage run as COMPLETED status by stage run ID. Optionally accepts summary JSON to store."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Stage run completed successfully",
                    content = @Content(schema = @Schema(implementation = StageRunDto.class))
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Stage run not found"
            )
    })
    public ResponseEntity<StageRunDto> completeStageRunById(
            @Parameter(description = "Stage run ID (billing_stage_run.stage_run_id)") @PathVariable UUID stageRunId,
            @RequestBody(required = false) Map<String, Object> requestBody) {
        
        log.info("Completing stage run by ID: stageRunId={}", stageRunId);
        
        Map<String, Object> summaryJson = requestBody != null && requestBody.containsKey("summaryJson") 
                ? (Map<String, Object>) requestBody.get("summaryJson") 
                : null;
        
        StageRunDto stage = stageService.completeStageRunById(stageRunId, summaryJson);
        if (stage == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(stage);
    }
}
