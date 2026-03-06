package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.CrmEntitySearchItemDto;
import io.clubone.billing.api.dto.crm.CrmEntitySearchResponse;
import io.clubone.billing.repo.CrmEntitySearchRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmEntitySearchService {

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final int DEFAULT_ENTITY_SEARCH_LIMIT = 20;

    private final CrmEntitySearchRepository entitySearchRepository;

    public CrmEntitySearchService(CrmEntitySearchRepository entitySearchRepository) {
        this.entitySearchRepository = entitySearchRepository;
    }

    public CrmEntitySearchResponse searchEntities(UUID entityTypeId, String search, Integer limit, Integer offset) {
        if (entityTypeId == null) return new CrmEntitySearchResponse(List.of(), 0L);
        UUID orgId = getOrgClientId();
        int limitVal = limit != null && limit > 0 ? Math.min(limit, 100) : DEFAULT_ENTITY_SEARCH_LIMIT;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = entitySearchRepository.searchEntities(orgId, entityTypeId, search, limitVal, offsetVal);
        long total = entitySearchRepository.countEntities(orgId, entityTypeId, search);
        List<CrmEntitySearchItemDto> items = rows.stream()
                .map(r -> new CrmEntitySearchItemDto(
                        asString(r.get("entity_id")),
                        asString(r.get("display_name")),
                        asString(r.get("secondary_text"))
                ))
                .toList();
        return new CrmEntitySearchResponse(items, total);
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
}
