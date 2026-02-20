package io.clubone.billing.api.v1;

import io.clubone.billing.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for metrics and reporting operations.
 */
@RestController
@RequestMapping("/api/v1/billing/metrics")
public class MetricsController {

    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    /**
     * GET /api/v1/billing/metrics/runs
     * Get billing run metrics.
     */
    @GetMapping("/runs")
    public ResponseEntity<Map<String, Object>> getRunMetrics(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false, defaultValue = "day") String groupBy,
            @RequestParam(required = false, defaultValue = "success_rate") String metric) {
        
        log.debug("Getting run metrics: fromDate={}, toDate={}, groupBy={}, metric={}", 
                fromDate, toDate, groupBy, metric);
        
        Map<String, Object> metrics = metricsService.getRunMetrics(fromDate, toDate, groupBy, metric);
        return ResponseEntity.ok(metrics);
    }

    /**
     * GET /api/v1/billing/metrics/stages
     * Get stage metrics.
     */
    @GetMapping("/stages")
    public ResponseEntity<Map<String, Object>> getStageMetrics(
            @RequestParam(required = false) String stageCode,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate) {
        
        log.debug("Getting stage metrics: stageCode={}, fromDate={}, toDate={}", stageCode, fromDate, toDate);
        
        Map<String, Object> metrics = metricsService.getStageMetrics(stageCode, fromDate, toDate);
        return ResponseEntity.ok(metrics);
    }

    /**
     * GET /api/v1/billing/metrics/charges
     * Get charge metrics.
     */
    @GetMapping("/charges")
    public ResponseEntity<Map<String, Object>> getChargeMetrics(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false, defaultValue = "failure_type") String groupBy) {
        
        log.debug("Getting charge metrics: fromDate={}, toDate={}, groupBy={}", fromDate, toDate, groupBy);
        
        Map<String, Object> metrics = metricsService.getChargeMetrics(fromDate, toDate, groupBy);
        return ResponseEntity.ok(metrics);
    }

    /**
     * GET /api/v1/billing/reports/executive-summary
     * Get executive summary report.
     */
    @GetMapping("/reports/executive-summary")
    public ResponseEntity<Map<String, Object>> getExecutiveSummary(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) UUID locationId) {
        
        log.debug("Getting executive summary: fromDate={}, toDate={}, locationId={}", 
                fromDate, toDate, locationId);
        
        Map<String, Object> summary = metricsService.getExecutiveSummary(fromDate, toDate, locationId);
        return ResponseEntity.ok(summary);
    }
}
