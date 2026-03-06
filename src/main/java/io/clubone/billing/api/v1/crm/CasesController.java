package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.CrmCaseDetailDto;
import io.clubone.billing.api.dto.crm.CrmCaseHistoryItemDto;
import io.clubone.billing.api.dto.crm.CrmContactCaseDto;
import io.clubone.billing.api.dto.crm.CrmContactCasesResponse;
import io.clubone.billing.api.dto.crm.CrmCreateCaseRequest;
import io.clubone.billing.service.CrmCaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for CRM Cases list screen (/crm/cases).
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class CasesController {

    private static final Logger log = LoggerFactory.getLogger(CasesController.class);

    private final CrmCaseService caseService;

    public CasesController(CrmCaseService caseService) {
        this.caseService = caseService;
    }

    @GetMapping("/cases")
    public ResponseEntity<CrmContactCasesResponse> listCases(
            @RequestParam(name = "scope", required = false) String scope,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "case_type_id", required = false) UUID caseTypeId,
            @RequestParam(name = "case_status_id", required = false) UUID caseStatusId,
            @RequestParam(name = "case_priority_id", required = false) UUID casePriorityId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing cases: scope={}, search={}, type={}, status={}, priority={}",
                scope, search, caseTypeId, caseStatusId, casePriorityId);
        CrmContactCasesResponse response = caseService.listCases(scope, search, caseTypeId, caseStatusId, casePriorityId, limit, offset);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/cases")
    public ResponseEntity<CrmContactCaseDto> createCase(@RequestBody CrmCreateCaseRequest request) {
        log.info("Creating case from Cases list: subject={}", request != null ? request.subject() : null);
        CrmContactCaseDto dto = caseService.createCase(request);
        if (dto == null) return ResponseEntity.badRequest().build();
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @GetMapping("/cases/{caseId}")
    public ResponseEntity<CrmCaseDetailDto> getCaseById(@PathVariable("caseId") UUID caseId) {
        log.debug("Getting case: caseId={}", caseId);
        CrmCaseDetailDto dto = caseService.getCaseById(caseId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PatchMapping("/cases/{caseId}")
    public ResponseEntity<CrmCaseDetailDto> updateCase(
            @PathVariable("caseId") UUID caseId,
            @RequestBody Map<String, Object> body) {
        log.info("Updating case: caseId={}", caseId);
        CrmCaseDetailDto dto = caseService.updateCase(caseId, body);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @GetMapping("/cases/{caseId}/history")
    public ResponseEntity<List<CrmCaseHistoryItemDto>> getCaseHistory(@PathVariable("caseId") UUID caseId) {
        log.debug("Getting case status history: caseId={}", caseId);
        List<CrmCaseHistoryItemDto> history = caseService.getCaseHistory(caseId);
        if (history == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(history);
    }
}
