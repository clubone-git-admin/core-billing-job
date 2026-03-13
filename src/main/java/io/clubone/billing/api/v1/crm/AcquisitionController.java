package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.acquisition.AcqSessionListResponse;
import io.clubone.billing.service.AcquisitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * REST API for acquisition session details by client_id.
 * Base path: /api/crm – same header support as other CRM APIs (enforced by interceptor):
 * X-Application-Id, X-Location-Id, X-Actor-Id. No values hardcoded; scope from context.
 */
@RestController
@RequestMapping("/api/crm")
public class AcquisitionController {

    private static final Logger log = LoggerFactory.getLogger(AcquisitionController.class);

    private final AcquisitionService acquisitionService;

    public AcquisitionController(AcquisitionService acquisitionService) {
        this.acquisitionService = acquisitionService;
    }

    /**
     * GET /api/crm/acquisition/clients/{clientId}/sessions
     * Returns acquisition session details for the given client_id, scoped by application (from headers).
     * Dates (created_on, modified_on, expires_at, opened_at, submitted_at) as stored, no conversion.
     */
    @GetMapping("/acquisition/clients/{clientId}/sessions")
    public ResponseEntity<AcqSessionListResponse> getSessionsByClientId(
            @PathVariable("clientId") UUID clientId) {
        log.debug("Getting acquisition sessions for client: clientId={}", clientId);
        AcqSessionListResponse response = acquisitionService.getSessionsByClientId(clientId);
        return ResponseEntity.ok(response);
    }
}
