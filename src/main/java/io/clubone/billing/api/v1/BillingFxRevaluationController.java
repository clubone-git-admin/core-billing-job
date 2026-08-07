package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.FxRevaluationRunDto;
import io.clubone.billing.service.currency.FxRevaluationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Tag(name = "Billing FX revaluation", description = "Unrealized FX gain/loss on open AR")
@RestController
@RequestMapping("/api/v1/billing/fx-revaluations")
public class BillingFxRevaluationController {

    private final FxRevaluationService revaluationService;

    public BillingFxRevaluationController(FxRevaluationService revaluationService) {
        this.revaluationService = revaluationService;
    }

    @Operation(summary = "List revaluation runs")
    @GetMapping
    public ResponseEntity<List<FxRevaluationRunDto>> list(
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(revaluationService.list(limit));
    }

    @Operation(summary = "Get a revaluation run with lines")
    @GetMapping("/{runId}")
    public ResponseEntity<FxRevaluationRunDto> get(@PathVariable UUID runId) {
        return ResponseEntity.ok(revaluationService.get(runId, true));
    }

    @Operation(summary = "Run FX revaluation for a period (YYYY-MM)")
    @PostMapping("/run")
    public ResponseEntity<FxRevaluationRunDto> run(
            @RequestParam String periodYearMonth) {
        return ResponseEntity.ok(revaluationService.run(periodYearMonth));
    }
}
