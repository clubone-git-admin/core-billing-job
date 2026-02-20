package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.ForecastItemDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.service.ForecastService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for forecast operations.
 */
@RestController
@RequestMapping("/api/v1/billing/forecast")
public class ForecastController {

    private static final Logger log = LoggerFactory.getLogger(ForecastController.class);

    private final ForecastService forecastService;

    public ForecastController(ForecastService forecastService) {
        this.forecastService = forecastService;
    }

    /**
     * GET /api/v1/billing/forecast
     * Get forecast data.
     */
    @GetMapping
    public ResponseEntity<?> getForecast(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false, defaultValue = "true") Boolean aggregate,
            @RequestParam(required = false, defaultValue = "day") String groupBy) {
        
        log.debug("Getting forecast: from={}, to={}, aggregate={}, groupBy={}", from, to, aggregate, groupBy);
        
        if (aggregate) {
            List<Map<String, Object>> data = forecastService.getForecastAggregated(from, to, groupBy);
            return ResponseEntity.ok(Map.of("data", data));
        } else {
            PageResponse<ForecastItemDto> response = forecastService.getForecast(from, to);
            return ResponseEntity.ok(response);
        }
    }

    /**
     * GET /api/v1/billing/forecast/{date}/summary
     * Get forecast summary for a specific date.
     */
    @GetMapping("/{date}/summary")
    public ResponseEntity<Map<String, Object>> getForecastSummary(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        
        log.debug("Getting forecast summary: date={}", date);
        
        Map<String, Object> summary = forecastService.getForecastSummary(date);
        return ResponseEntity.ok(summary);
    }

    /**
     * GET /api/v1/billing/forecast/{date}/invoices
     * Get forecast invoices for a specific date.
     */
    @GetMapping("/{date}/invoices")
    public ResponseEntity<PageResponse<ForecastItemDto>> getForecastInvoices(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) UUID locationId,
            @RequestParam(required = false) Boolean hasWarnings,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Getting forecast invoices: date={}, search={}, locationId={}", date, search, locationId);
        
        PageResponse<ForecastItemDto> response = forecastService.getForecastInvoices(
                date, search, locationId, hasWarnings, limit, offset);
        
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/subscriptions/{subscriptionInstanceId}/forecast
     * Get forecast for a specific subscription instance.
     */
    @GetMapping("/subscriptions/{subscriptionInstanceId}/forecast")
    public ResponseEntity<List<ForecastItemDto>> getSubscriptionForecast(
            @PathVariable UUID subscriptionInstanceId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        
        log.debug("Getting subscription forecast: subscriptionInstanceId={}, from={}, to={}", 
                subscriptionInstanceId, from, to);
        
        List<ForecastItemDto> forecast = forecastService.getSubscriptionForecast(
                subscriptionInstanceId, from, to);
        
        return ResponseEntity.ok(forecast);
    }
}
