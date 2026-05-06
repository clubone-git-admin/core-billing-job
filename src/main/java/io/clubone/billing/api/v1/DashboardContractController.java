package io.clubone.billing.api.v1;

import io.clubone.billing.service.DashboardService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Map;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardContractController {

    private final DashboardService dashboardService;

    public DashboardContractController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/overview")
    public ResponseEntity<?> overview(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) String locationLevelId,
            @RequestParam(required = false, defaultValue = "7D") String segment,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations) {
        try {
            return ResponseEntity.ok(
                    dashboardService.getDashboardOverviewContract(
                            fromDate, toDate, segment, locationLevelId, includeChildLocations));
        } catch (NoSuchElementException e) {
            return ResponseEntity.status(404)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "error", "INVALID_LOCATION_LEVEL",
                            "message", "locationLevelId does not exist or is not accessible",
                            "status", 404));
        } catch (IllegalStateException e) {
            if ("DATE_SPAN_TOO_LARGE".equals(e.getMessage())) {
                return ResponseEntity.status(422)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of(
                                "error", "DATE_SPAN_TOO_LARGE",
                                "message", "date span exceeds allowed maximum",
                                "status", 422));
            }
            return ResponseEntity.internalServerError()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "error", "INTERNAL_ERROR",
                            "message", "Failed to load dashboard overview",
                            "status", 500));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "error", "INVALID_REQUEST",
                            "message", e.getMessage(),
                            "status", 400));
        }
    }
}

