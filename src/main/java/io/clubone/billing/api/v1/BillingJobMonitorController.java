package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.BillingJobMonitorItemDto;
import io.clubone.billing.api.dto.BillingJobMonitorSummaryDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.service.BillingJobMonitorService;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Monitor scheduled / in-flight invoice-generation and mock-charge jobs.
 */
@RestController
@RequestMapping("/api/billing/jobs")
@Tag(name = "Billing jobs", description = "Monitor and act on scheduled / in-flight billing stage jobs")
public class BillingJobMonitorController {

    private final BillingJobMonitorService billingJobMonitorService;

    public BillingJobMonitorController(BillingJobMonitorService billingJobMonitorService) {
        this.billingJobMonitorService = billingJobMonitorService;
    }

    @GetMapping
    public ResponseEntity<PageResponse<BillingJobMonitorItemDto>> list(
            @RequestParam(required = false) String stage,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        return ResponseEntity.ok(billingJobMonitorService.list(stage, status, limit, offset));
    }

    @GetMapping("/summary")
    public ResponseEntity<BillingJobMonitorSummaryDto> summary() {
        return ResponseEntity.ok(billingJobMonitorService.summary());
    }

    @PostMapping("/{stageRunId}/cancel")
    public ResponseEntity<Void> cancel(@PathVariable UUID stageRunId) {
        billingJobMonitorService.cancel(stageRunId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{stageRunId}/redispatch")
    public ResponseEntity<BillingJobMonitorItemDto> redispatch(@PathVariable UUID stageRunId) {
        return ResponseEntity.ok(billingJobMonitorService.redispatch(stageRunId));
    }
}
