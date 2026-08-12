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
import io.clubone.billing.repo.LocationLevelRepository;

/**
 * Service for audit log operations.
 */
@Service
public class AuditLogService {

    private final AuditLogRepository auditLogRepository;
    private final LocationLevelRepository locationLevelRepository;

    public AuditLogService(
            AuditLogRepository auditLogRepository,
            LocationLevelRepository locationLevelRepository) {
        this.auditLogRepository = auditLogRepository;
        this.locationLevelRepository = locationLevelRepository;
    }

    public Map<String, Object> listAuditLogs(
            String entityType,
            UUID entityId,
            UUID billingRunId,
            String eventType,
            UUID locationLevelId,
            Boolean includeChildLocations,
            OffsetDateTime fromTs,
            OffsetDateTime toTs,
            Integer limit,
            Integer offset) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);

        int safeLimit = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        int safeOffset = offset != null && offset >= 0 ? offset : 0;

        List<Map<String, Object>> logs = auditLogRepository.findAuditLogs(
                entityType,
                entityId,
                billingRunId,
                eventType,
                locationIds,
                fromTs,
                toTs,
                safeLimit,
                safeOffset);

        Integer total = auditLogRepository.countAuditLogs(
                entityType, entityId, billingRunId, eventType, locationIds, fromTs, toTs);

        List<Map<String, Object>> logList = logs.stream()
                .map(this::formatAuditLog)
                .collect(Collectors.toList());

        return Map.of(
                "data", logList,
                "total", total,
                "limit", safeLimit,
                "offset", safeOffset
        );
    }

    public byte[] exportAuditLogs(
            String entityType,
            UUID billingRunId,
            String eventType,
            UUID locationLevelId,
            Boolean includeChildLocations,
            OffsetDateTime fromTs,
            OffsetDateTime toTs,
            String format) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);
        if ("csv".equalsIgnoreCase(format)) {
            String csv = auditLogRepository.exportAuditLogsCSV(
                    entityType, billingRunId, eventType, locationIds, fromTs, toTs);
            return csv.getBytes(StandardCharsets.UTF_8);
        } else {
            Map<String, Object> data =
                    listAuditLogs(
                            entityType,
                            null,
                            billingRunId,
                            eventType,
                            locationLevelId,
                            includeChildLocations,
                            fromTs,
                            toTs,
                            10000,
                            0);
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                return null;
            }
        }
    }

    /**
     * PostgreSQL JDBC returns {@code json}/{@code jsonb} as {@code org.postgresql.util.PGobject}, not {@link String}.
     * Avoid importing driver types here; use reflection when needed.
     */
    private static String jdbcJsonToString(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof String s) {
            return s;
        }
        if ("org.postgresql.util.PGobject".equals(raw.getClass().getName())) {
            try {
                Object v = raw.getClass().getMethod("getValue").invoke(raw);
                return v != null ? v.toString() : null;
            } catch (ReflectiveOperationException e) {
                return raw.toString();
            }
        }
        return raw.toString();
    }

    private Map<String, Object> formatAuditLog(Map<String, Object> log) {
        Map<String, Object> details = new HashMap<>();
        String detailsStr = jdbcJsonToString(log.get("details"));
        if (detailsStr != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> parsedDetails = mapper.readValue(detailsStr, Map.class);
                details = new HashMap<>(parsedDetails);
            } catch (Exception e) {
                // Keep empty map if parsing fails
            }
        }

        String billingRunCode = stringOrEmpty(log.get("billing_run_code"));
        String stageRunCode = stringOrEmpty(log.get("stage_run_code"));
        String stageCode = stringOrEmpty(log.get("stage_code"));
        if (!billingRunCode.isEmpty()) {
            details.putIfAbsent("billing_run_code", billingRunCode);
        }
        if (!stageRunCode.isEmpty()) {
            details.putIfAbsent("stage_run_code", stageRunCode);
        }
        if (!stageCode.isEmpty()) {
            details.putIfAbsent("stage_code", stageCode);
        }

        String resolvedLabel = stringOrEmpty(log.get("resolved_user_label"));
        String storedEmail = stringOrEmpty(log.get("user_email"));
        String userEmail = !resolvedLabel.isEmpty() ? resolvedLabel : storedEmail;

        Map<String, Object> result = new HashMap<>();
        result.put("audit_log_id", log.get("audit_log_id"));
        result.put("event_type", log.get("event_type"));
        result.put("entity_type", log.get("entity_type"));
        result.put("entity_id", log.get("entity_id"));
        result.put("action", log.get("action"));
        result.put("user_id", log.getOrDefault("user_id", ""));
        result.put("user_email", userEmail);
        result.put("user_name", resolvedLabel);
        result.put("billing_run_code", billingRunCode);
        result.put("stage_run_code", stageRunCode);
        result.put("stage_code", stageCode);
        result.put("details", details);
        result.put("created_on", log.get("created_on"));
        result.put("ip_address", log.getOrDefault("ip_address", ""));
        result.put("user_agent", log.getOrDefault("user_agent", ""));
        return result;
    }

    private static String stringOrEmpty(Object v) {
        if (v == null) {
            return "";
        }
        String s = v.toString().trim();
        return "null".equalsIgnoreCase(s) ? "" : s;
    }

    private List<UUID> resolveLocationIds(UUID locationLevelId, Boolean includeChildLocations) {
        if (locationLevelId == null) {
            return List.of();
        }
        boolean includeChildren = includeChildLocations == null || includeChildLocations;
        return locationLevelRepository
                .resolveLocationsForLevel(locationLevelId, includeChildren)
                .stream()
                .map(LocationLevelRepository.LocationRow::locationId)
                .toList();
    }
}
