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
import java.util.LinkedHashMap;
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
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") Boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "true") Boolean aggregate,
            @RequestParam(required = false, defaultValue = "day") String groupBy,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String currency) {

        String ccy = currencyCode != null && !currencyCode.isBlank() ? currencyCode : currency;
        log.debug(
                "Getting forecast: from={}, to={}, locationLevelId={}, includeChildLocations={}, aggregate={}, groupBy={}, currencyCode={}",
                from, to, locationLevelId, includeChildLocations, aggregate, groupBy, ccy);

        if (aggregate) {
            List<Map<String, Object>> data =
                    forecastService.getForecastAggregated(
                            from, to, groupBy, locationLevelId, includeChildLocations, ccy);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("data", data);
            return ResponseEntity.ok(body);
        } else {
            PageResponse<ForecastItemDto> response =
                    forecastService.getForecast(from, to, locationLevelId, includeChildLocations, ccy);
            return ResponseEntity.ok(response);
        }
    }

    /**
     * GET /api/v1/billing/forecast/{date}/summary
     * Get forecast summary for a specific date.
     */
    @GetMapping("/{date}/summary")
    public ResponseEntity<Map<String, Object>> getForecastSummary(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String currency) {

        String ccy = currencyCode != null && !currencyCode.isBlank() ? currencyCode : currency;
        log.debug("Getting forecast summary: date={}, currencyCode={}", date, ccy);

        Map<String, Object> summary = forecastService.getForecastSummary(date, ccy);
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
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String currency,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {

        String ccy = currencyCode != null && !currencyCode.isBlank() ? currencyCode : currency;
        log.debug("Getting forecast invoices: date={}, search={}, locationId={}, currencyCode={}",
                date, search, locationId, ccy);

        PageResponse<ForecastItemDto> response = forecastService.getForecastInvoices(
                date, search, locationId, hasWarnings, limit, offset, ccy);

        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/forecast/{date}/reports
     * Breakdown reports for forecast detail dialog (client/location/agreement).
     */
    @GetMapping("/{date}/reports")
    public ResponseEntity<Map<String, Object>> getForecastReports(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(required = false) String reportType,
            @RequestParam(required = false, name = "report_type") String reportTypeSnake,
            @RequestParam(required = false) String currencyCode,
            @RequestParam(required = false) String currency) {

        String type = reportType != null && !reportType.isBlank() ? reportType : reportTypeSnake;
        String ccy = currencyCode != null && !currencyCode.isBlank() ? currencyCode : currency;
        log.debug("Getting forecast reports: date={}, reportType={}, currencyCode={}", date, type, ccy);
        return ResponseEntity.ok(forecastService.getForecastReports(date, type, ccy));
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
