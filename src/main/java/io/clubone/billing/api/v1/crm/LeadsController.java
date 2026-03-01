package io.clubone.billing.api.v1.crm;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nimbusds.jose.shaded.gson.Gson;

import io.clubone.billing.api.dto.crm.CrmCreateLeadNoteRequest;
import io.clubone.billing.api.dto.crm.CrmLeadDetailDto;
import io.clubone.billing.api.dto.crm.CrmLeadHistoryItemDto;
import io.clubone.billing.api.dto.crm.CrmLeadListResponse;
import io.clubone.billing.api.dto.crm.CrmLeadNoteDto;
import io.clubone.billing.api.dto.crm.CrmLeadRelatedDto;
import io.clubone.billing.api.dto.crm.CrmLeadSummaryDto;
import io.clubone.billing.api.dto.crm.CrmLeadUpsertRequest;
import io.clubone.billing.api.dto.crm.CrmUpdateLeadStatusRequest;
import io.clubone.billing.service.CrmLeadService;

/**
 * REST API for CRM Leads (list, detail, create, update, Kanban, notes, history, related).
 *
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class LeadsController {

    private static final Logger log = LoggerFactory.getLogger(LeadsController.class);

    private final CrmLeadService leadService;

    public LeadsController(CrmLeadService leadService) {
        this.leadService = leadService;
    }

    /**
     * GET /api/crm/leads
     * List leads for table + Kanban.
     */
    @GetMapping("/leads")
    public ResponseEntity<CrmLeadListResponse> listLeads(
            @RequestParam(name = "status_filter", required = false) String statusFilter,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset
    ) {
        log.debug("Listing leads: statusFilter={}, search={}, limit={}, offset={}",
                statusFilter, search, limit, offset);
        CrmLeadListResponse response = leadService.listLeads(statusFilter, search, limit, offset);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/crm/leads/{leadId}
     * Get lead by ID (detail).
     */
    @GetMapping("/leads/{leadId}")
    public ResponseEntity<CrmLeadDetailDto> getLeadById(@PathVariable("leadId") UUID leadId) {
        log.debug("Getting lead detail: leadId={}", leadId);
        CrmLeadDetailDto dto = leadService.getLeadById(leadId);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    /**
     * POST /api/crm/leads
     * Create new lead.
     */
    @PostMapping("/leads")
    public ResponseEntity<CrmLeadDetailDto> createLead(@RequestBody CrmLeadUpsertRequest request) {
        log.info("Creating lead: email={}, phone={}", request.email(), request.phone());
        System.out.println("print ::  " + new Gson().toJson(request));
        CrmLeadDetailDto dto = leadService.createLead(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    /**
     * PATCH /api/crm/leads/{leadId}
     * Delta update existing lead.
     */
    @PatchMapping("/leads/{leadId}")
    public ResponseEntity<CrmLeadDetailDto> updateLead(
            @PathVariable("leadId") UUID leadId,
            @RequestBody CrmLeadUpsertRequest request
    ) {
        log.info("Updating lead: leadId={}", leadId);
        CrmLeadDetailDto dto = leadService.updateLead(leadId, request);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    /**
     * PATCH /api/crm/leads/{leadId}/status
     * Update lead status. Body: { "lead_status_id": "<uuid>", "change_reason"?: "", "notes"?: "" }.
     * lead_status_id must be a UUID from crm.lu_lead_status. NEW/CONTACTED/QUALIFIED/DISQUALIFIED update only leads + lead_status_history (revertible).
     * CONVERTED is one-way: creates client (Prospect), contact (PROSPECT), opportunity (PROSPECTING) and sets converted_contact_id/converted_opportunity_id on the lead.
     */
    @PatchMapping("/leads/{leadId}/status")
    public ResponseEntity<CrmLeadSummaryDto> updateLeadStatus(
            @PathVariable("leadId") UUID leadId,
            @RequestBody CrmUpdateLeadStatusRequest request
    ) {
        log.info("Updating lead status: leadId={}", leadId);
        CrmLeadSummaryDto dto = leadService.updateLeadStatus(leadId, request);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }

    /**
     * GET /api/crm/leads/{leadId}/notes
     * List lead notes.
     */
    @GetMapping("/leads/{leadId}/notes")
    public ResponseEntity<List<CrmLeadNoteDto>> listNotes(@PathVariable("leadId") UUID leadId) {
        log.debug("Listing notes for lead: leadId={}", leadId);
        List<CrmLeadNoteDto> notes = leadService.listNotes(leadId);
        return ResponseEntity.ok(notes);
    }

    /**
     * POST /api/crm/leads/{leadId}/notes
     * Add a new lead note.
     */
    @PostMapping("/leads/{leadId}/notes")
    public ResponseEntity<CrmLeadNoteDto> addNote(
            @PathVariable("leadId") UUID leadId,
            @RequestBody CrmCreateLeadNoteRequest request
    ) {
        log.info("Adding note for lead: leadId={}", leadId);
        CrmLeadNoteDto note = leadService.addNote(leadId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(note);
    }

    /**
     * GET /api/crm/leads/{leadId}/history
     * Lead field/status history.
     */
    @GetMapping("/leads/{leadId}/history")
    public ResponseEntity<List<CrmLeadHistoryItemDto>> listHistory(@PathVariable("leadId") UUID leadId) {
        log.debug("Listing history for lead: leadId={}", leadId);
        List<CrmLeadHistoryItemDto> history = leadService.listHistory(leadId);
        return ResponseEntity.ok(history);
    }

    /**
     * GET /api/crm/leads/{leadId}/related
     * Lead related records (contact, opportunity, cases, account).
     */
    @GetMapping("/leads/{leadId}/related")
    public ResponseEntity<CrmLeadRelatedDto> getRelated(@PathVariable("leadId") UUID leadId) {
        log.debug("Getting related records for lead: leadId={}", leadId);
        CrmLeadRelatedDto dto = leadService.getRelated(leadId);
        return ResponseEntity.ok(dto);
    }
}

