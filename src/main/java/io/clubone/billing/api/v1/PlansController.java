package io.clubone.billing.api.v1;

import io.clubone.billing.service.PlansService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for subscription plans operations.
 */
@RestController
@RequestMapping("/api/v1/billing/plans")
public class PlansController {

    private static final Logger log = LoggerFactory.getLogger(PlansController.class);

    private final PlansService plansService;

    public PlansController(PlansService plansService) {
        this.plansService = plansService;
    }

    /**
     * GET /api/v1/billing/plans
     * List subscription plans.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listPlans(
            @RequestParam(required = false) Boolean isActive,
            @RequestParam(required = false) UUID clientAgreementId,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Listing plans: isActive={}, clientAgreementId={}", isActive, clientAgreementId);
        
        Map<String, Object> response = plansService.listPlans(isActive, clientAgreementId, limit, offset);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/plans/{subscription_plan_id}
     * Get a specific subscription plan.
     */
    @GetMapping("/{subscriptionPlanId}")
    public ResponseEntity<Map<String, Object>> getPlan(@PathVariable UUID subscriptionPlanId) {
        log.debug("Getting plan: subscriptionPlanId={}", subscriptionPlanId);
        
        Map<String, Object> plan = plansService.getPlan(subscriptionPlanId);
        if (plan == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(plan);
    }

    /**
     * GET /api/v1/billing/plans/{subscription_plan_id}/instances
     * Get instances for a subscription plan.
     */
    @GetMapping("/{subscriptionPlanId}/instances")
    public ResponseEntity<Map<String, Object>> getPlanInstances(
            @PathVariable UUID subscriptionPlanId,
            @RequestParam(required = false) String statusCode,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Getting plan instances: subscriptionPlanId={}, statusCode={}", subscriptionPlanId, statusCode);
        
        Map<String, Object> response = plansService.getPlanInstances(subscriptionPlanId, statusCode, limit, offset);
        return ResponseEntity.ok(response);
    }
}
