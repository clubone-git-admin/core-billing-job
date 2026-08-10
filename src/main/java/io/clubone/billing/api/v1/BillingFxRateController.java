package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.FxRateDto;
import io.clubone.billing.api.dto.currency.RejectFxRateRequest;
import io.clubone.billing.api.dto.currency.UpsertFxRateRequest;
import io.clubone.billing.service.currency.FxRateService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Tag(name = "Billing FX rates", description = "FX rates with single-step approval for reporting conversion")
@RestController
@RequestMapping("/api/v1/billing/fx-rates")
public class BillingFxRateController {

    private final FxRateService fxRateService;

    public BillingFxRateController(FxRateService fxRateService) {
        this.fxRateService = fxRateService;
    }

    @Operation(summary = "List recent FX rates for the tenant")
    @GetMapping
    public ResponseEntity<List<FxRateDto>> list(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(fxRateService.list(limit));
    }

    @Operation(summary = "Submit an FX rate for approval (PENDING until approved)")
    @PostMapping
    public ResponseEntity<FxRateDto> submit(@RequestBody UpsertFxRateRequest request) {
        return ResponseEntity.ok(fxRateService.submit(request));
    }

    @Operation(summary = "Approve a PENDING FX rate (submitter may approve)")
    @PostMapping("/{fxRateId}/approve")
    public ResponseEntity<FxRateDto> approve(@PathVariable UUID fxRateId) {
        return ResponseEntity.ok(fxRateService.approve(fxRateId));
    }

    @Operation(summary = "Reject a PENDING FX rate (submitter may reject)")
    @PostMapping("/{fxRateId}/reject")
    public ResponseEntity<FxRateDto> reject(
            @PathVariable UUID fxRateId,
            @RequestBody(required = false) RejectFxRateRequest request) {
        String reason = request != null ? request.reason() : null;
        return ResponseEntity.ok(fxRateService.reject(fxRateId, reason));
    }
}
