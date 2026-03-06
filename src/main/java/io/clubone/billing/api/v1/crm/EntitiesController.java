package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.CrmEntitySearchResponse;
import io.clubone.billing.service.CrmEntitySearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Entity picker for CRM (e.g. task "For" dropdown: search by entity_type_id).
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class EntitiesController {

    private static final Logger log = LoggerFactory.getLogger(EntitiesController.class);

    private final CrmEntitySearchService entitySearchService;

    public EntitiesController(CrmEntitySearchService entitySearchService) {
        this.entitySearchService = entitySearchService;
    }

    @GetMapping("/entities/search")
    public ResponseEntity<CrmEntitySearchResponse> searchEntities(
            @RequestParam(name = "entity_type_id", required = true) UUID entityTypeId,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Entity search: entity_type_id={}, search={}", entityTypeId, search);
        CrmEntitySearchResponse response = entitySearchService.searchEntities(entityTypeId, search, limit, offset);
        return ResponseEntity.ok(response);
    }
}
