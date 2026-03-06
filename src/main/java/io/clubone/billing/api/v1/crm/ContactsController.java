package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmActivityService;
import io.clubone.billing.service.CrmAttachmentService;
import io.clubone.billing.service.CrmContactService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;

/**
 * REST API for CRM Contacts (list, detail, update, owner, notes, activities, attachments, linked, cases, bulk).
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class ContactsController {

    private static final Logger log = LoggerFactory.getLogger(ContactsController.class);

    private final CrmContactService contactService;
    private final CrmActivityService activityService;
    private final CrmAttachmentService attachmentService;

    public ContactsController(CrmContactService contactService,
                             CrmActivityService activityService,
                             CrmAttachmentService attachmentService) {
        this.contactService = contactService;
        this.activityService = activityService;
        this.attachmentService = attachmentService;
    }

    @GetMapping("/contacts")
    public ResponseEntity<CrmContactListResponse> listContacts(
            @RequestParam(name = "view", required = false) String view,
            @RequestParam(name = "lifecycle_code", required = false) String lifecycleCode,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "owner_id", required = false) UUID ownerId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing contacts: view={}, lifecycle_code={}, search={}", view, lifecycleCode, search);
        CrmContactListResponse response = contactService.listContacts(view, lifecycleCode, search, ownerId, limit, offset);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/contacts/{contactId}")
    public ResponseEntity<CrmContactDetailDto> getContactById(@PathVariable("contactId") UUID contactId) {
        log.debug("Getting contact: contactId={}", contactId);
        CrmContactDetailDto dto = contactService.getContactById(contactId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PatchMapping("/contacts/{contactId}")
    public ResponseEntity<CrmContactDetailDto> updateContact(
            @PathVariable("contactId") UUID contactId,
            @RequestBody CrmContactUpdateRequest request) {
        log.info("Updating contact: contactId={}", contactId);
        CrmContactDetailDto dto = contactService.updateContact(contactId, request);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PatchMapping("/contacts/{contactId}/owner")
    public ResponseEntity<CrmContactDetailDto> changeOwner(
            @PathVariable("contactId") UUID contactId,
            @RequestBody CrmContactOwnerRequest request) {
        log.info("Changing owner for contact: contactId={}", contactId);
        if (request == null || request.ownerId() == null || request.ownerId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        UUID ownerId = UUID.fromString(request.ownerId());
        CrmContactDetailDto dto = contactService.changeOwner(contactId, ownerId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @GetMapping("/contacts/{contactId}/notes")
    public ResponseEntity<List<CrmContactNoteDto>> listNotes(@PathVariable("contactId") UUID contactId) {
        log.debug("Listing notes for contact: contactId={}", contactId);
        List<CrmContactNoteDto> notes = contactService.listNotes(contactId);
        return ResponseEntity.ok(notes);
    }

    @PostMapping("/contacts/{contactId}/notes")
    public ResponseEntity<CrmContactNoteDto> addNote(
            @PathVariable("contactId") UUID contactId,
            @RequestBody CrmCreateLeadNoteRequest request) {
        log.info("Adding note for contact: contactId={}", contactId);
        CrmContactNoteDto note = contactService.addNote(contactId, request);
        if (note == null) return ResponseEntity.notFound().build();
        return ResponseEntity.status(HttpStatus.CREATED).body(note);
    }

    @GetMapping("/contacts/{contactId}/activities")
    public ResponseEntity<CrmLeadActivitiesResponse> getContactActivities(
            @PathVariable("contactId") UUID contactId,
            @RequestParam(name = "type_code", required = false) String typeCode,
            @RequestParam(name = "status_code", required = false) String statusCode,
            @RequestParam(name = "outcome_code", required = false) String outcomeCode,
            @RequestParam(name = "from", required = false) String from,
            @RequestParam(name = "to", required = false) String to,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Getting activities for contact: contactId={}", contactId);
        CrmLeadActivitiesResponse response = activityService.getContactActivities(contactId, typeCode, statusCode, outcomeCode, from, to, search, limit, offset);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/contacts/{contactId}/activities")
    public ResponseEntity<CrmLeadActivityDto> logActivity(
            @PathVariable("contactId") UUID contactId,
            @RequestBody CrmLogActivityRequest request) {
        log.info("Logging activity for contact: contactId={}, type={}", contactId, request != null ? request.activityTypeCode() : null);
        CrmLeadActivityDto dto = activityService.logActivityForContact(contactId, request);
        return dto != null ? ResponseEntity.status(HttpStatus.CREATED).body(dto) : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    @GetMapping("/contacts/{contactId}/attachments")
    public ResponseEntity<CrmAttachmentListResponse> listAttachments(
            @PathVariable("contactId") UUID contactId,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "category", required = false) String category,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing attachments for contact: contactId={}", contactId);
        CrmAttachmentListResponse response = attachmentService.listAttachmentsForContact(contactId, search, category, limit, offset);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/contacts/{contactId}/attachments")
    public ResponseEntity<CrmAttachmentDto> uploadAttachment(
            @PathVariable("contactId") UUID contactId,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "category", required = false) String category) {
        log.info("Uploading attachment for contact: contactId={}", contactId);
        CrmAttachmentDto dto = attachmentService.uploadAttachmentForContact(contactId, file, description, category);
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @GetMapping("/contacts/{contactId}/attachments/{attachmentId}/download-url")
    public ResponseEntity<CrmAttachmentDownloadUrlDto> getAttachmentDownloadUrl(
            @PathVariable("contactId") UUID contactId,
            @PathVariable("attachmentId") UUID attachmentId) {
        log.debug("Getting download URL for contact attachment: contactId={}, attachmentId={}", contactId, attachmentId);
        CrmAttachmentDownloadUrlDto dto = attachmentService.getDownloadUrlForContact(contactId, attachmentId);
        return ResponseEntity.ok(dto);
    }

    @DeleteMapping("/contacts/{contactId}/attachments/{attachmentId}")
    public ResponseEntity<Void> deleteAttachment(
            @PathVariable("contactId") UUID contactId,
            @PathVariable("attachmentId") UUID attachmentId) {
        log.info("Deleting attachment: contactId={}, attachmentId={}", contactId, attachmentId);
        attachmentService.deleteAttachmentForContact(contactId, attachmentId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/contacts/{contactId}/linked")
    public ResponseEntity<CrmContactLinkedDto> getLinked(@PathVariable("contactId") UUID contactId) {
        log.debug("Getting linked records for contact: contactId={}", contactId);
        CrmContactLinkedDto dto = contactService.getLinked(contactId);
        return ResponseEntity.ok(dto);
    }

    @GetMapping("/contacts/{contactId}/cases")
    public ResponseEntity<CrmContactCasesResponse> getCases(
            @PathVariable("contactId") UUID contactId,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "status", required = false) String status,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Getting cases for contact: contactId={}", contactId);
        CrmContactCasesResponse response = contactService.getCases(contactId, search, status, limit, offset);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/contacts/{contactId}/cases")
    public ResponseEntity<CrmContactCaseDto> createCase(
            @PathVariable("contactId") UUID contactId,
            @RequestBody CrmCreateCaseRequest request) {
        log.info("Creating case for contact: contactId={}", contactId);
        CrmContactCaseDto dto = contactService.createCase(contactId, request);
        if (dto == null) return ResponseEntity.badRequest().build();
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @PatchMapping("/contacts/bulk/owner")
    public ResponseEntity<CrmContactBulkOwnerResponse> bulkChangeOwner(@RequestBody CrmContactBulkOwnerRequest request) {
        log.info("Bulk change owner for {} contacts", request.contactIds() != null ? request.contactIds().size() : 0);
        if (request.contactIds() == null || request.contactIds().isEmpty() || request.ownerId() == null || request.ownerId().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        CrmContactBulkOwnerResponse response = contactService.bulkChangeOwner(request.contactIds(), request.ownerId());
        return ResponseEntity.ok(response);
    }
}
