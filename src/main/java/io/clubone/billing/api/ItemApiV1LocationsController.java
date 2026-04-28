package io.clubone.billing.api;

import io.clubone.billing.service.BillingReportingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Aliases for paths documented in the items module so a single deployment can serve the same
 * location tree as {@code GET /api/v1/billing/reports/location-hierarchy} (see billing reports API
 * spec §2).
 */
@RestController
public class ItemApiV1LocationsController {

    private final BillingReportingService reportingService;

    public ItemApiV1LocationsController(BillingReportingService reportingService) {
        this.reportingService = reportingService;
    }

    @GetMapping("/item/apiV1/locations/hierarchy")
    public ResponseEntity<List<Map<String, Object>>> locationsHierarchy(
            @RequestParam UUID applicationId) {
        return ResponseEntity.ok(reportingService.getLocationHierarchy(applicationId));
    }
}
