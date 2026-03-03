package io.clubone.billing.api.v1.crm;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.nimbusds.jose.shaded.gson.Gson;

import io.clubone.billing.api.dto.crm.CrmAttachmentDto;
import io.clubone.billing.api.dto.crm.CrmAttachmentDownloadUrlDto;
import io.clubone.billing.api.dto.crm.CrmAttachmentListResponse;
import io.clubone.billing.api.dto.crm.CrmBulkActivitiesRequest;
import io.clubone.billing.api.dto.crm.CrmBulkActivitiesResponse;
import io.clubone.billing.api.dto.crm.CrmCreateLeadNoteRequest;
import io.clubone.billing.api.dto.crm.CrmLeadActivitiesResponse;
import io.clubone.billing.api.dto.crm.CrmLeadActivityDto;
import io.clubone.billing.api.dto.crm.CrmLeadDetailDto;
import io.clubone.billing.api.dto.crm.CrmLeadHistoryItemDto;
import io.clubone.billing.api.dto.crm.CrmLeadListResponse;
import io.clubone.billing.api.dto.crm.CrmLeadNoteDto;
import io.clubone.billing.api.dto.crm.CrmLeadRelatedDto;
import io.clubone.billing.api.dto.crm.CrmLeadSummaryDto;
import io.clubone.billing.api.dto.crm.CrmLeadUpsertRequest;
import io.clubone.billing.api.dto.crm.CrmLogActivityRequest;
import io.clubone.billing.api.dto.crm.CrmUpdateLeadStatusRequest;
import io.clubone.billing.service.CrmActivityService;
import io.clubone.billing.service.CrmAttachmentService;
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
    private final CrmActivityService activityService;
    private final CrmAttachmentService attachmentService;

    public LeadsController(CrmLeadService leadService, CrmActivityService activityService, CrmAttachmentService attachmentService) {
        this.leadService = leadService;
        this.activityService = activityService;
        this.attachmentService = attachmentService;
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
     * POST /api/crm/leads/bulk/activities
     * Bulk log same activity for multiple leads (e.g. Log Call from leads list).
     */
    @PostMapping("/leads/bulk/activities")
    public ResponseEntity<CrmBulkActivitiesResponse> bulkLogActivities(@RequestBody CrmBulkActivitiesRequest request) {
        log.info("Bulk log activities for {} leads", request.leadIds() != null ? request.leadIds().size() : 0);
        CrmBulkActivitiesResponse response = activityService.bulkLogActivities(request);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/crm/leads/{leadId}/activities
     * Lead activities timeline (filter by type, status, outcome, date range, search).
     */
    @GetMapping("/leads/{leadId}/activities")
    public ResponseEntity<CrmLeadActivitiesResponse> getLeadActivities(
            @PathVariable("leadId") UUID leadId,
            @RequestParam(name = "type_code", required = false) String typeCode,
            @RequestParam(name = "status_code", required = false) String statusCode,
            @RequestParam(name = "outcome_code", required = false) String outcomeCode,
            @RequestParam(name = "from", required = false) String from,
            @RequestParam(name = "to", required = false) String to,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset
    ) {
        log.debug("Getting activities for lead: leadId={}", leadId);
        CrmLeadActivitiesResponse response = activityService.getLeadActivities(leadId, typeCode, statusCode, outcomeCode, from, to, search, limit, offset);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/crm/leads/{leadId}/activities
     * Log single activity for a lead (call, email, event, task, sms, whatsapp).
     */
    @PostMapping("/leads/{leadId}/activities")
    public ResponseEntity<CrmLeadActivityDto> logActivity(
            @PathVariable("leadId") UUID leadId,
            @RequestBody CrmLogActivityRequest request
    ) {
        log.info("Logging activity for lead: leadId={}, type={}", leadId, request != null ? request.activityTypeCode() : null);
        CrmLeadActivityDto dto = activityService.logActivity(leadId, request);
        return dto != null ? ResponseEntity.status(HttpStatus.CREATED).body(dto) : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
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

    /**
     * GET /api/crm/leads/{leadId}/linked
     * Linked tab: converted contact, converted opportunity, related cases, related account.
     * Omit top-level keys when there is no linked record. 404 if lead does not exist.
     */
    @GetMapping("/leads/{leadId}/linked")
    public ResponseEntity<CrmLeadRelatedDto> getLinked(@PathVariable("leadId") UUID leadId) {
        log.debug("Getting linked records for lead: leadId={}", leadId);
        if (leadService.getLeadById(leadId) == null) {
            return ResponseEntity.notFound().build();
        }
        CrmLeadRelatedDto dto = leadService.getRelated(leadId);
        return ResponseEntity.ok(dto);
    }

    // --- Lead Attachments (Attachments tab) ---

    /**
     * GET /api/crm/leads/{leadId}/attachments
     * List attachments for the lead (optional: search, category, limit, offset).
     */
    @GetMapping("/leads/{leadId}/attachments")
    public ResponseEntity<CrmAttachmentListResponse> listAttachments(
            @PathVariable("leadId") UUID leadId,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "category", required = false) String category,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset
    ) {
        log.debug("Listing attachments for lead: leadId={}", leadId);
        CrmAttachmentListResponse response = attachmentService.listAttachments(leadId, search, category, limit, offset);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/crm/leads/{leadId}/attachments
     * Upload a new attachment (multipart: file required; description, category optional).
     */
    @PostMapping("/leads/{leadId}/attachments")
    public ResponseEntity<CrmAttachmentDto> uploadAttachment(
            @PathVariable("leadId") UUID leadId,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "category", required = false) String category
    ) {
        log.info("Uploading attachment for lead: leadId={}, fileName={}", leadId, file != null ? file.getOriginalFilename() : null);
        CrmAttachmentDto dto = attachmentService.uploadAttachment(leadId, file, description, category);
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    /**
     * GET /api/crm/leads/{leadId}/attachments/{attachmentId}/download-url
     * Get a short-lived presigned URL for downloading the file (e.g. when list presigned_url is missing or expired).
     */
    @GetMapping("/leads/{leadId}/attachments/{attachmentId}/download-url")
    public ResponseEntity<CrmAttachmentDownloadUrlDto> getAttachmentDownloadUrl(
            @PathVariable("leadId") UUID leadId,
            @PathVariable("attachmentId") UUID attachmentId
    ) {
        log.debug("Getting download URL for attachment: leadId={}, attachmentId={}", leadId, attachmentId);
        CrmAttachmentDownloadUrlDto dto = attachmentService.getDownloadUrl(leadId, attachmentId);
        return ResponseEntity.ok(dto);
    }

    /**
     * DELETE /api/crm/leads/{leadId}/attachments/{attachmentId}
     * Soft-delete the attachment.
     */
    @DeleteMapping("/leads/{leadId}/attachments/{attachmentId}")
    public ResponseEntity<Void> deleteAttachment(
            @PathVariable("leadId") UUID leadId,
            @PathVariable("attachmentId") UUID attachmentId
    ) {
        log.info("Deleting attachment: leadId={}, attachmentId={}", leadId, attachmentId);
        attachmentService.deleteAttachment(leadId, attachmentId);
        return ResponseEntity.noContent().build();
    }
}

