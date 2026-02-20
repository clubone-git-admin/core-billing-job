package io.clubone.billing.service;

import io.clubone.billing.repo.LookupRepository;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for lookup data operations.
 */
@Service
public class LookupService {

    private final LookupRepository lookupRepository;

    public LookupService(LookupRepository lookupRepository) {
        this.lookupRepository = lookupRepository;
    }

    public Map<String, Object> getStatuses(String lookupType) {
        List<Map<String, Object>> statuses = lookupRepository.getStatuses(lookupType);

        List<Map<String, Object>> statusList = statuses.stream()
                .map(s -> Map.of(
                        "id", s.get("id"),
                        "code", s.get("code"),
                        "display_name", s.get("display_name"),
                        "description", s.getOrDefault("description", ""),
                        "is_active", s.getOrDefault("is_active", true),
                        "sort_order", s.getOrDefault("sort_order", 0)
                ))
                .collect(Collectors.toList());

        return Map.of("data", statusList);
    }

    public Map<String, Object> getStages() {
        List<Map<String, Object>> stages = lookupRepository.getStages();

        List<Map<String, Object>> stageList = stages.stream()
                .map(s -> Map.of(
                        "id", s.get("id"),
                        "code", s.get("code"),
                        "display_name", s.get("display_name"),
                        "description", s.getOrDefault("description", ""),
                        "stage_sequence", s.get("stage_sequence"),
                        "is_optional", s.getOrDefault("is_optional", false),
                        "is_active", s.getOrDefault("is_active", true)
                ))
                .collect(Collectors.toList());

        return Map.of("data", stageList);
    }
}

