package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.acquisition.AcqSessionDetailDto;
import io.clubone.billing.api.dto.acquisition.AcqSessionListResponse;
import io.clubone.billing.repo.AcqSessionRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for acquisition session APIs. Uses same context (X-Application-Id, X-Location-Id, X-Actor-Id).
 */
@Service
public class AcquisitionService {

    private final AcqSessionRepository acqSessionRepository;
    private final CrmRequestContext context;

    public AcquisitionService(AcqSessionRepository acqSessionRepository, CrmRequestContext context) {
        this.acqSessionRepository = acqSessionRepository;
        this.context = context;
    }

    /**
     * Get acquisition session details by client_id. Scoped by application_id from X-Application-Id header.
     * Dates returned as stored (no conversion).
     */
    public AcqSessionListResponse getSessionsByClientId(UUID clientId) {
        UUID applicationId = context.getApplicationId();
        List<Map<String, Object>> rows = acqSessionRepository.findSessionsByClientId(applicationId, clientId);
        List<AcqSessionDetailDto> sessions = rows.stream().map(this::mapToDetail).toList();
        return new AcqSessionListResponse(sessions, sessions.size());
    }

    private AcqSessionDetailDto mapToDetail(Map<String, Object> r) {
        Integer lastStep = null;
        Object step = r.get("last_completed_step");
        if (step instanceof Number n) lastStep = n.intValue();
        return new AcqSessionDetailDto(
                asString(r.get("session_id")),
                asString(r.get("application_id")),
                asString(r.get("client_id")),
                asString(r.get("status_code")),
                AcqSessionRepository.toTimestampStringAsStored(r.get("created_on")),
                asString(r.get("created_by")),
                AcqSessionRepository.toTimestampStringAsStored(r.get("modified_on")),
                asString(r.get("modified_by")),
                AcqSessionRepository.toTimestampStringAsStored(r.get("expires_at")),
                AcqSessionRepository.toTimestampStringAsStored(r.get("opened_at")),
                AcqSessionRepository.toTimestampStringAsStored(r.get("submitted_at")),
                lastStep,
                asString(r.get("first_name")),
                asString(r.get("last_name")),
                asString(r.get("email")),
                asString(r.get("phone")),
                asString(r.get("timezone")),
                r.get("step_payload_json"),
                r.get("quote_json"),
                r.get("utm_json")
        );
    }

    private static String asString(Object v) {
        return v == null ? null : v.toString();
    }
}
