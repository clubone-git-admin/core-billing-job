package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.CrmLookupItemDto;
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

}

