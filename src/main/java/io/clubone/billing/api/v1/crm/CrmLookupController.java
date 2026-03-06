package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmLookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * REST API for CRM lead-related lookups.
 *
 * Base path: /api/crm/lookups
 */
@RestController
@RequestMapping("/api/crm/lookups")
public class CrmLookupController {

    private static final Logger log = LoggerFactory.getLogger(CrmLookupController.class);

    private final CrmLookupService lookupService;

    public CrmLookupController(CrmLookupService lookupService) {
        this.lookupService = lookupService;
    }

    @GetMapping("/lead-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getLeadStatuses() {
        log.debug("Getting CRM lead statuses");
        return ResponseEntity.ok(lookupService.getLeadStatuses());
    }

    @GetMapping("/lead-sources")
    public ResponseEntity<List<CrmLookupItemDto>> getLeadSources() {
        log.debug("Getting CRM lead sources");
        return ResponseEntity.ok(lookupService.getLeadSources());
    }

    @GetMapping("/lead-types")
    public ResponseEntity<List<CrmLookupItemDto>> getLeadTypes() {
        log.debug("Getting CRM lead types");
        return ResponseEntity.ok(lookupService.getLeadTypes());
    }

    @GetMapping("/lead-record-types")
    public ResponseEntity<List<CrmLookupItemDto>> getLeadRecordTypes() {
        log.debug("Getting CRM lead record types");
        return ResponseEntity.ok(lookupService.getLeadRecordTypes());
    }

    @GetMapping("/salutations")
    public ResponseEntity<List<CrmLookupItemDto>> getSalutations() {
        log.debug("Getting CRM salutations");
        return ResponseEntity.ok(lookupService.getSalutations());
    }

    @GetMapping("/genders")
    public ResponseEntity<List<CrmLookupItemDto>> getGenders() {
        log.debug("Getting CRM genders");
        return ResponseEntity.ok(lookupService.getGenders());
    }

    @GetMapping("/home-clubs")
    public ResponseEntity<List<CrmLookupItemDto>> getHomeClubs() {
        log.debug("Getting CRM home clubs");
        return ResponseEntity.ok(lookupService.getHomeClubs());
    }

    @GetMapping("/users")
    public ResponseEntity<List<CrmLookupItemDto>> getUsers() {
        log.debug("Getting CRM users with Member Advisor role");
        return ResponseEntity.ok(lookupService.getUsers());
    }

    @GetMapping("/contact-lifecycles")
    public ResponseEntity<List<CrmLookupItemDto>> getContactLifecycles() {
        log.debug("Getting CRM contact lifecycles");
        return ResponseEntity.ok(lookupService.getContactLifecycles());
    }

    @GetMapping("/countries")
    public ResponseEntity<List<CrmLookupItemDto>> getCountries() {
        log.debug("Getting CRM countries");
        return ResponseEntity.ok(lookupService.getCountries());
    }

    @GetMapping("/states")
    public ResponseEntity<List<CrmLookupItemDto>> getStates(@RequestParam(name = "country_code", required = false) String countryCode) {
        log.debug("Getting CRM states for country: {}", countryCode);
        return ResponseEntity.ok(lookupService.getStates(countryCode));
    }

    @GetMapping("/accounts")
    public ResponseEntity<List<CrmLookupItemDto>> getAccountsForLeadRecordType(
            @RequestParam("leadRecordTypeId") UUID leadRecordTypeId) {
        log.debug("Getting CRM accounts for lead_record_type_id={}", leadRecordTypeId);
        return ResponseEntity.ok(lookupService.getAccountsForLeadRecordType(leadRecordTypeId));
    }

    @GetMapping("/campaigns")
    public ResponseEntity<List<CrmLookupItemDto>> getCampaigns() {
        log.debug("Getting CRM campaigns");
        return ResponseEntity.ok(lookupService.getCampaigns());
    }

    @GetMapping("/contacts")
    public ResponseEntity<List<CrmLookupItemDto>> getContacts() {
        log.debug("Getting CRM contacts");
        return ResponseEntity.ok(lookupService.getContacts());
    }

    // Activity lookups
    @GetMapping("/activity-types")
    public ResponseEntity<List<CrmLookupItemDto>> getActivityTypes() {
        return ResponseEntity.ok(lookupService.getActivityTypes());
    }

    @GetMapping("/activity-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getActivityStatuses() {
        return ResponseEntity.ok(lookupService.getActivityStatuses());
    }

    @GetMapping("/activity-outcomes")
    public ResponseEntity<List<CrmLookupItemDto>> getActivityOutcomes() {
        return ResponseEntity.ok(lookupService.getActivityOutcomes());
    }

    @GetMapping("/activity-visibilities")
    public ResponseEntity<List<CrmLookupItemDto>> getActivityVisibilities() {
        return ResponseEntity.ok(lookupService.getActivityVisibilities());
    }

    @GetMapping("/entity-types")
    public ResponseEntity<List<CrmEntityTypeLookupDto>> getEntityTypes() {
        return ResponseEntity.ok(lookupService.getEntityTypesFull());
    }

    @GetMapping("/call-directions")
    public ResponseEntity<List<CrmLookupItemDto>> getCallDirections() {
        return ResponseEntity.ok(lookupService.getCallDirections());
    }

    @GetMapping("/call-results")
    public ResponseEntity<List<CrmLookupItemDto>> getCallResults() {
        return ResponseEntity.ok(lookupService.getCallResults());
    }

    @GetMapping("/email-delivery-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getEmailDeliveryStatuses() {
        return ResponseEntity.ok(lookupService.getEmailDeliveryStatuses());
    }

    @GetMapping("/sms-delivery-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getSmsDeliveryStatuses() {
        return ResponseEntity.ok(lookupService.getSmsDeliveryStatuses());
    }

    @GetMapping("/whatsapp-delivery-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getWhatsappDeliveryStatuses() {
        return ResponseEntity.ok(lookupService.getWhatsappDeliveryStatuses());
    }

    @GetMapping("/event-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getEventStatuses() {
        return ResponseEntity.ok(lookupService.getEventStatuses());
    }

    @GetMapping("/event-purposes")
    public ResponseEntity<List<CrmLookupItemDto>> getEventPurposes() {
        log.debug("Getting CRM event purposes");
        return ResponseEntity.ok(lookupService.getEventPurposes());
    }

    @GetMapping("/rsvp-statuses")
    public ResponseEntity<List<CrmLookupItemDto>> getRsvpStatuses() {
        return ResponseEntity.ok(lookupService.getRsvpStatuses());
    }

    @GetMapping("/task-types")
    public ResponseEntity<List<CrmTaskTypeLookupDto>> getTaskTypes() {
        return ResponseEntity.ok(lookupService.getTaskTypesFull());
    }

    @GetMapping("/task-statuses")
    public ResponseEntity<List<CrmTaskStatusLookupDto>> getTaskStatuses() {
        return ResponseEntity.ok(lookupService.getTaskStatusesFull());
    }

    @GetMapping("/task-priorities")
    public ResponseEntity<List<CrmTaskPriorityLookupDto>> getTaskPriorities() {
        return ResponseEntity.ok(lookupService.getTaskPrioritiesFull());
    }

    @GetMapping("/email-from-addresses")
    public ResponseEntity<List<CrmLookupItemDto>> getEmailFromAddresses() {
        log.debug("Getting CRM email from addresses");
        return ResponseEntity.ok(lookupService.getEmailFromAddresses());
    }

    @GetMapping("/email-templates")
    public ResponseEntity<List<CrmLookupItemDto>> getEmailTemplates() {
        log.debug("Getting CRM email templates");
        return ResponseEntity.ok(lookupService.getEmailTemplates());
    }

    @GetMapping("/case-types")
    public ResponseEntity<List<CrmCaseTypeLookupDto>> getCaseTypes() {
        log.debug("Getting CRM case types");
        return ResponseEntity.ok(lookupService.getCaseTypes());
    }

    @GetMapping("/case-statuses")
    public ResponseEntity<List<CrmCaseStatusLookupDto>> getCaseStatuses() {
        log.debug("Getting CRM case statuses");
        return ResponseEntity.ok(lookupService.getCaseStatuses());
    }

    @GetMapping("/case-priorities")
    public ResponseEntity<List<CrmCasePriorityLookupDto>> getCasePriorities() {
        log.debug("Getting CRM case priorities");
        return ResponseEntity.ok(lookupService.getCasePriorities());
    }

    @GetMapping("/account-types")
    public ResponseEntity<List<CrmAccountTypeLookupDto>> getAccountTypes() {
        log.debug("Getting CRM account types");
        return ResponseEntity.ok(lookupService.getAccountTypes());
    }

    @GetMapping("/campaign-types")
    public ResponseEntity<List<CrmCampaignTypeLookupDto>> getCampaignTypes() {
        log.debug("Getting CRM campaign types");
        return ResponseEntity.ok(lookupService.getCampaignTypesFull());
    }

    @GetMapping("/campaign-statuses")
    public ResponseEntity<List<CrmCampaignStatusLookupDto>> getCampaignStatuses() {
        log.debug("Getting CRM campaign statuses");
        return ResponseEntity.ok(lookupService.getCampaignStatusesFull());
    }

}

