package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.DashboardKPIDto;
import io.clubone.billing.service.DashboardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for dashboard operations.
 * Provides KPIs, trends, and summary information.
 */
@RestController
@RequestMapping("/api/v1/billing/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    /**
     * GET /api/v1/billing/dashboard/kpis
     * Get dashboard KPIs.
     */
    @GetMapping("/kpis")
    public ResponseEntity<DashboardKPIDto> getKPIs(
            @RequestParam(required = false) UUID locationId,
            @RequestParam(required = false, defaultValue = "30d") String dateRange) {
        
        log.debug("Getting dashboard KPIs: locationId={}, dateRange={}", locationId, dateRange);
        
        DashboardKPIDto kpis = dashboardService.getKPIs(locationId, dateRange);
        return ResponseEntity.ok(kpis);
    }

    /**
     * GET /api/v1/billing/dashboard/trends
     * Get dashboard trends.
     */
    @GetMapping("/trends")
    public ResponseEntity<Map<String, Object>> getTrends(
            @RequestParam(required = false, defaultValue = "amount") String metric,
            @RequestParam(required = false, defaultValue = "30d") String period,
            @RequestParam(required = false, defaultValue = "day") String groupBy,
            @RequestParam(required = false) UUID locationId) {
        
        log.debug("Getting dashboard trends: metric={}, period={}, groupBy={}", metric, period, groupBy);
        
        Map<String, Object> trends = dashboardService.getTrends(metric, period, groupBy, locationId);
        return ResponseEntity.ok(trends);
    }

    /**
     * GET /api/v1/billing/dashboard/overview
     * Single payload overview for billing dashboard.
     */
    @GetMapping("/overview")
    public ResponseEntity<?> getOverview(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) String locationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String currentStage,
            @RequestParam(required = false, defaultValue = "10") int limitRuns,
            @RequestParam(required = false, defaultValue = "0") int offsetRuns,
            @RequestParam(required = false, defaultValue = "200") int limitPlans,
            @RequestParam(required = false, defaultValue = "0") int offsetPlans) {
        try {
            return ResponseEntity.ok(
                    dashboardService.getOverview(
                            dueDateFrom,
                            dueDateTo,
                            null,
                            null,
                            locationId,
                            locationLevelId,
                            includeChildLocations,
                            status,
                            currentStage,
                            Math.max(1, limitRuns),
                            Math.max(0, offsetRuns),
                            Math.max(1, limitPlans),
                            Math.max(0, offsetPlans)));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "message", e.getMessage(),
                            "code", "BILLING_DASHBOARD_OVERVIEW_FAILED",
                            "details", Map.of()));
        } catch (Exception e) {
            log.error("dashboard overview failed", e);
            return ResponseEntity.internalServerError()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "message", "Failed to load billing dashboard overview",
                            "code", "BILLING_DASHBOARD_OVERVIEW_FAILED",
                            "details", Map.of()));
        }
    }
}
