package io.clubone.billing.api.v1;

import io.clubone.billing.service.LookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API v1 for lookup data operations.
 */
@RestController
@RequestMapping("/api/v1/billing/lookups")
public class LookupController {

    private static final Logger log = LoggerFactory.getLogger(LookupController.class);

    private final LookupService lookupService;

    public LookupController(LookupService lookupService) {
        this.lookupService = lookupService;
    }

    /**
     * GET /api/v1/billing/lookups/statuses
     * Get status lookup data.
     */
    @GetMapping("/statuses")
    public ResponseEntity<Map<String, Object>> getStatuses(
            @RequestParam(required = false) String lookupType) {
        
        log.debug("Getting statuses: lookupType={}", lookupType);
        
        Map<String, Object> response = lookupService.getStatuses(lookupType);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/lookups/stages
     * Get stage lookup data.
     */
    @GetMapping("/stages")
    public ResponseEntity<Map<String, Object>> getStages() {
        log.debug("Getting stages");
        
        Map<String, Object> response = lookupService.getStages();
        return ResponseEntity.ok(response);
    }
}
