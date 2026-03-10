package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmActivityService;
import io.clubone.billing.service.CrmOpportunityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API for CRM Opportunities (list, detail, create, update, delete, bulk, contact methods).
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class OpportunitiesController {

    private static final Logger log = LoggerFactory.getLogger(OpportunitiesController.class);

    private final CrmOpportunityService opportunityService;
    private final CrmActivityService activityService;

    public OpportunitiesController(CrmOpportunityService opportunityService, CrmActivityService activityService) {
        this.opportunityService = opportunityService;
        this.activityService = activityService;
    }

    @GetMapping("/opportunities")
    public ResponseEntity<CrmOpportunityListResponse> listOpportunities(
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "opportunity_stage_id", required = false) UUID opportunityStageId,
            @RequestParam(name = "stage_code", required = false) String stageCode,
            @RequestParam(name = "owner_user_id", required = false) UUID ownerUserId,
            @RequestParam(name = "lead_type_id", required = false) UUID leadTypeId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset
    ) {
        log.debug("Listing opportunities: search={}, stage={}, owner={}, limit={}, offset={}",
                search, opportunityStageId, ownerUserId, limit, offset);
        CrmOpportunityListResponse response = opportunityService.list(search, opportunityStageId, stageCode, ownerUserId, leadTypeId, limit, offset);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/opportunities/{opportunityId}")
    public ResponseEntity<CrmOpportunityDetailDto> getOpportunity(@PathVariable("opportunityId") UUID opportunityId) {
        CrmOpportunityDetailDto dto = opportunityService.getById(opportunityId);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    @PostMapping("/opportunities")
    public ResponseEntity<CrmOpportunityDetailDto> createOpportunity(@RequestBody CrmCreateOpportunityRequest request) {
        CrmOpportunityDetailDto created = opportunityService.create(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PatchMapping("/opportunities/{opportunityId}")
    public ResponseEntity<CrmOpportunityDetailDto> updateOpportunity(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestBody Map<String, Object> body
    ) {
        CrmOpportunityDetailDto dto = opportunityService.update(opportunityId, body);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    @DeleteMapping("/opportunities/{opportunityId}")
    public ResponseEntity<Void> deleteOpportunity(@PathVariable("opportunityId") UUID opportunityId) {
        boolean deleted = opportunityService.delete(opportunityId);
        if (!deleted) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/opportunities/bulk/update-stage")
    public ResponseEntity<Map<String, Object>> bulkUpdateStage(@RequestBody CrmBulkUpdateStageRequest request) {
        int updated = opportunityService.bulkUpdateStage(request);
        return ResponseEntity.ok(Map.of("updated", updated));
    }

    @PostMapping("/opportunities/bulk/change-owner")
    public ResponseEntity<Map<String, Object>> bulkChangeOwner(@RequestBody CrmBulkChangeOwnerRequest request) {
        int updated = opportunityService.bulkChangeOwner(request);
        return ResponseEntity.ok(Map.of("updated", updated));
    }

    @GetMapping("/opportunities/{opportunityId}/contact-methods")
    public ResponseEntity<CrmOpportunityContactMethodListResponse> listContactMethods(
            @PathVariable("opportunityId") UUID opportunityId
    ) {
        CrmOpportunityContactMethodListResponse response = opportunityService.listContactMethods(opportunityId);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    @PostMapping("/opportunities/{opportunityId}/contact-methods")
    public ResponseEntity<CrmOpportunityContactMethodDto> addContactMethod(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestBody CrmAddContactMethodRequest request
    ) {
        CrmOpportunityContactMethodDto dto = opportunityService.addContactMethod(opportunityId, request);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @DeleteMapping("/opportunities/{opportunityId}/contact-methods/{clientContactMethodId}")
    public ResponseEntity<Void> removeContactMethod(
            @PathVariable("opportunityId") UUID opportunityId,
            @PathVariable("clientContactMethodId") UUID clientContactMethodId
    ) {
        boolean removed = opportunityService.removeContactMethod(opportunityId, clientContactMethodId);
        if (!removed) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * GET /api/crm/opportunities/{opportunityId}/activities
     * List activities (communications timeline) for the opportunity. Same response shape as lead/contact activities.
     * Query params: limit (default 50, max 500), offset, type_code, status_code, search, outcome_code, from, to (ISO 8601).
     */
    @GetMapping("/opportunities/{opportunityId}/activities")
    public ResponseEntity<CrmLeadActivitiesResponse> getOpportunityActivities(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset,
            @RequestParam(name = "type_code", required = false) String typeCode,
            @RequestParam(name = "status_code", required = false) String statusCode,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "outcome_code", required = false) String outcomeCode,
            @RequestParam(name = "from", required = false) String from,
            @RequestParam(name = "to", required = false) String to
    ) {
        log.debug("Getting activities for opportunity: opportunityId={}", opportunityId);
        CrmLeadActivitiesResponse response = activityService.getOpportunityActivities(
                opportunityId, typeCode, statusCode, outcomeCode, from, to, search, limit, offset);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/crm/opportunities/{opportunityId}/activities
     * Log activity (Send & log email / SMS / WhatsApp, or other types). Same request body as leads/contacts.
     * Returns 201 with activity object; 404 if opportunity not found; 400 if required fields missing or invalid.
     */
    @PostMapping("/opportunities/{opportunityId}/activities")
    public ResponseEntity<CrmLeadActivityDto> logActivity(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestBody CrmLogActivityRequest request
    ) {
        if (opportunityService.getById(opportunityId) == null) {
            return ResponseEntity.notFound().build();
        }
        CrmLeadActivityDto dto = activityService.logActivityForOpportunity(opportunityId, request);
        return dto != null ? ResponseEntity.status(HttpStatus.CREATED).body(dto) : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    /**
     * GET /api/crm/opportunities/{opportunityId}/placeholder
     * Placeholder API: brand name, opportunity first/last/display name, sales advisor name and title.
     * Query param: sales_advisor_id (UUID). 404 if opportunity does not exist.
     */
    @GetMapping("/opportunities/{opportunityId}/placeholder")
    public ResponseEntity<CrmOpportunityPlaceholderDto> getOpportunityPlaceholder(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestParam(name = "sales_advisor_id") UUID salesAdvisorId
    ) {
        log.debug("Getting placeholder for opportunity: opportunityId={}, salesAdvisorId={}", opportunityId, salesAdvisorId);
        CrmOpportunityPlaceholderDto dto = opportunityService.getOpportunityPlaceholder(opportunityId, salesAdvisorId);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    /**
     * GET /api/crm/opportunities/{opportunityId}/linked
     * Linked tab: linked contact, related cases, related account. 404 if opportunity not found.
     */
    @GetMapping("/opportunities/{opportunityId}/linked")
    public ResponseEntity<CrmOpportunityLinkedDto> getLinked(@PathVariable("opportunityId") UUID opportunityId) {
        CrmOpportunityLinkedDto dto = opportunityService.getLinked(opportunityId);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    /**
     * GET /api/crm/opportunities/{opportunityId}/trials
     * Trial tab: Trial agreements available at the opportunity's home location. 404 if opportunity not found.
     */
    @GetMapping("/opportunities/{opportunityId}/trials")
    public ResponseEntity<CrmOpportunityTrialResponse> getTrials(@PathVariable("opportunityId") UUID opportunityId) {
        CrmOpportunityTrialResponse response = opportunityService.getTrials(opportunityId);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/crm/opportunities/{opportunityId}/trials/active
     * All active trial client agreements for that opportunity (Trial type + Active status). 404 if opportunity not found.
     */
    @GetMapping("/opportunities/{opportunityId}/trials/active")
    public ResponseEntity<ActiveTrialAgreementResponse> getActiveTrialAgreements(@PathVariable("opportunityId") UUID opportunityId) {
        ActiveTrialAgreementResponse response = opportunityService.getActiveTrialAgreements(opportunityId);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/crm/opportunities/{opportunityId}/trials/client-agreement
     * Create a client agreement for the selected trial; updates client_role_status.
     * Body: agreement_id, agreement_location_id, start_date (required). end_date is auto-calculated from agreement term.
     */
    @PostMapping("/opportunities/{opportunityId}/trials/client-agreement")
    public ResponseEntity<CreateTrialClientAgreementResponse> createTrialClientAgreement(
            @PathVariable("opportunityId") UUID opportunityId,
            @RequestBody CreateTrialClientAgreementRequest request) {
        if (request == null || request.agreementId() == null || request.agreementId().isBlank()
                || request.agreementLocationId() == null || request.agreementLocationId().isBlank()
                || request.startDate() == null || request.startDate().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        CreateTrialClientAgreementResponse response = opportunityService.createTrialClientAgreement(opportunityId, request);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
