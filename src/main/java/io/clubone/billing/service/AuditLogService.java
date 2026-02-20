package io.clubone.billing.service;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import io.clubone.billing.repo.AuditLogRepository;

/**
 * Service for audit log operations.
 */
@Service
public class AuditLogService {

    private final AuditLogRepository auditLogRepository;

    public AuditLogService(AuditLogRepository auditLogRepository) {
        this.auditLogRepository = auditLogRepository;
    }

    public Map<String, Object> listAuditLogs(
            String entityType, UUID entityId, OffsetDateTime fromTs,
            OffsetDateTime toTs, Integer limit, Integer offset) {
        
        List<Map<String, Object>> logs = auditLogRepository.findAuditLogs(
                entityType, entityId, fromTs, toTs, limit, offset);
        
        Integer total = auditLogRepository.countAuditLogs(
                entityType, entityId, fromTs, toTs);

        List<Map<String, Object>> logList = logs.stream()
                .map(this::formatAuditLog)
                .collect(Collectors.toList());

        return Map.of(
                "data", logList,
                "total", total,
                "limit", limit,
                "offset", offset
        );
    }

    public byte[] exportAuditLogs(String entityType, OffsetDateTime fromTs, OffsetDateTime toTs, String format) {
        if ("csv".equalsIgnoreCase(format)) {
            String csv = auditLogRepository.exportAuditLogsCSV(entityType, fromTs, toTs);
            return csv.getBytes(StandardCharsets.UTF_8);
        } else {
            // For JSON format, return JSON bytes
            Map<String, Object> data = listAuditLogs(entityType, null, fromTs, toTs, 10000, 0);
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                return null;
            }
        }
    }

    private Map<String, Object> formatAuditLog(Map<String, Object> log) {
        Map<String, Object> details = new HashMap<>();
        String detailsStr = (String) log.get("details");
        if (detailsStr != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> parsedDetails = mapper.readValue(detailsStr, Map.class);
                details = parsedDetails;
            } catch (Exception e) {
                // Keep empty map if parsing fails
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("audit_log_id", log.get("audit_log_id"));
        result.put("event_type", log.get("event_type"));
        result.put("entity_type", log.get("entity_type"));
        result.put("entity_id", log.get("entity_id"));
        result.put("action", log.get("action"));
        result.put("user_id", log.getOrDefault("user_id", ""));
        result.put("user_email", log.getOrDefault("user_email", ""));
        result.put("details", details);
        result.put("created_on", log.get("created_on"));
        result.put("ip_address", log.getOrDefault("ip_address", ""));
        result.put("user_agent", log.getOrDefault("user_agent", ""));
        return result;
    }
}

