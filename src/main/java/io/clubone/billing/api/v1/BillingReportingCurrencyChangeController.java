package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.RejectFxRateRequest;
import io.clubone.billing.api.dto.currency.ReportingCurrencyChangeDto;
import io.clubone.billing.api.dto.currency.SubmitReportingCurrencyChangeRequest;
import io.clubone.billing.service.currency.ReportingCurrencyChangeService;
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

@Tag(name = "Billing reporting currency changes", description = "Dual-control for org reporting currency")
@RestController
@RequestMapping("/api/v1/billing/tenant-settings/reporting-currency-changes")
public class BillingReportingCurrencyChangeController {

    private final ReportingCurrencyChangeService changeService;

    public BillingReportingCurrencyChangeController(ReportingCurrencyChangeService changeService) {
        this.changeService = changeService;
    }

    @Operation(summary = "List reporting currency change requests")
    @GetMapping
    public ResponseEntity<List<ReportingCurrencyChangeDto>> list(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(changeService.list(limit));
    }

    @Operation(summary = "Submit a reporting currency change for approval")
    @PostMapping
    public ResponseEntity<ReportingCurrencyChangeDto> submit(
            @RequestBody SubmitReportingCurrencyChangeRequest request) {
        return ResponseEntity.ok(changeService.submit(request));
    }

    @Operation(summary = "Approve a PENDING reporting currency change")
    @PostMapping("/{changeId}/approve")
    public ResponseEntity<ReportingCurrencyChangeDto> approve(@PathVariable UUID changeId) {
        return ResponseEntity.ok(changeService.approve(changeId));
    }

    @Operation(summary = "Reject a PENDING reporting currency change")
    @PostMapping("/{changeId}/reject")
    public ResponseEntity<ReportingCurrencyChangeDto> reject(
            @PathVariable UUID changeId,
            @RequestBody(required = false) RejectFxRateRequest request) {
        return ResponseEntity.ok(changeService.reject(changeId, request));
    }
}
