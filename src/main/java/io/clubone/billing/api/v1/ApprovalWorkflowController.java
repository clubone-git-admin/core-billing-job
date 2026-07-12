package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.ApprovalService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST API v1 for approval workflow operations.
 * Provides operations for managing billing run approvals.
 */
@RestController
@RequestMapping("/api/v1/billing/runs/{billingRunId}/approvals")
@Tag(name = "Approval Workflow", description = "Billing run approve / reject workflow")
public class ApprovalWorkflowController {

    private static final Logger log = LoggerFactory.getLogger(ApprovalWorkflowController.class);

    private final ApprovalService approvalService;

    public ApprovalWorkflowController(ApprovalService approvalService) {
        this.approvalService = approvalService;
    }

    @GetMapping
    @Operation(summary = "List approvals for a billing run")
    public ResponseEntity<List<ApprovalDto>> listApprovals(@PathVariable UUID billingRunId) {
        log.debug("Listing approvals for billing run: billingRunId={}", billingRunId);
        
        List<ApprovalDto> approvals = approvalService.listApprovals(billingRunId);
        return ResponseEntity.ok(approvals);
    }

    @PostMapping("/approve")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Approve a billing run")
    public ResponseEntity<BillingRunDto> approveBillingRun(
            @PathVariable UUID billingRunId,
            @Valid @RequestBody ApproveBillingRunRequest request) {
        
        log.info("Approving billing run: billingRunId={}, approvalLevel={}", 
                billingRunId, request.approvalLevel());
        
        BillingRunDto billingRun = approvalService.approveBillingRun(billingRunId, request);
        if (billingRun == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(billingRun);
    }

    @PostMapping("/reject")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Reject a billing run")
    public ResponseEntity<BillingRunDto> rejectBillingRun(
            @PathVariable UUID billingRunId,
            @Valid @RequestBody RejectBillingRunRequest request) {
        
        log.info("Rejecting billing run: billingRunId={}, approvalLevel={}", 
                billingRunId, request.approvalLevel());
        
        BillingRunDto billingRun = approvalService.rejectBillingRun(billingRunId, request);
        if (billingRun == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(billingRun);
    }

    @GetMapping("/pending")
    @Operation(summary = "List pending approvals")
    public ResponseEntity<PageResponse<BillingRunDto>> listPendingApprovals(
            @RequestParam(required = false) String approverRole,
            @RequestParam(required = false) Integer approvalLevel,
            @RequestParam(defaultValue = "20") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Listing pending approvals: approverRole={}, approvalLevel={}", 
                approverRole, approvalLevel);
        
        PageResponse<BillingRunDto> response = approvalService.listPendingApprovals(
                approverRole, approvalLevel, limit, offset);
        
        return ResponseEntity.ok(response);
    }
}
