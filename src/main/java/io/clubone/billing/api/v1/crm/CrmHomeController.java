package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmHomeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * REST API for CRM Home dashboard (/crm).
 * Base path: /api/crm/home
 */
@RestController
@RequestMapping("/api/crm/home")
public class CrmHomeController {

    private static final Logger log = LoggerFactory.getLogger(CrmHomeController.class);

    private final CrmHomeService homeService;

    public CrmHomeController(CrmHomeService homeService) {
        this.homeService = homeService;
    }

    @GetMapping("/pipeline-summary")
    public ResponseEntity<CrmPipelineSummaryDto> getPipelineSummary() {
        log.debug("Getting CRM home pipeline summary");
        return ResponseEntity.ok(homeService.getPipelineSummary());
    }

    @GetMapping("/key-deals")
    public ResponseEntity<CrmKeyDealsResponse> getKeyDeals(
            @RequestParam(name = "filter", required = false) String filter) {
        log.debug("Getting CRM home key deals: filter={}", filter);
        return ResponseEntity.ok(homeService.getKeyDeals(filter));
    }

    @GetMapping("/todays-events")
    public ResponseEntity<CrmTodaysEventsResponse> getTodaysEvents(
            @RequestParam(name = "date", required = false) String date) {
        log.debug("Getting CRM home today's events: date={}", date);
        return ResponseEntity.ok(homeService.getTodaysEvents(date));
    }

    @GetMapping("/todays-tasks")
    public ResponseEntity<CrmTodaysTasksResponse> getTodaysTasks(
            @RequestParam(name = "scope", required = false) String scope) {
        log.debug("Getting CRM home today's tasks: scope={}", scope);
        return ResponseEntity.ok(homeService.getTodaysTasks(scope));
    }

    @GetMapping("/items-to-approve")
    public ResponseEntity<CrmItemsToApproveResponse> getItemsToApprove() {
        log.debug("Getting CRM home items to approve");
        return ResponseEntity.ok(homeService.getItemsToApprove());
    }

    @PostMapping("/approvals/{approvalId}/approve")
    public ResponseEntity<Void> approve(@PathVariable("approvalId") UUID approvalId) {
        log.debug("Approving: approvalId={}", approvalId);
        if (!homeService.approve(approvalId)) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/approvals/{approvalId}/reject")
    public ResponseEntity<Void> reject(@PathVariable("approvalId") UUID approvalId) {
        log.debug("Rejecting: approvalId={}", approvalId);
        if (!homeService.reject(approvalId)) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }
}
