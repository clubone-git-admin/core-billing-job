package io.clubone.billing.api.context;

import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.UUID;

/**
 * Request-scoped context for CRM API calls. Populated by CrmContextInterceptor from headers:
 * X-Application-Id → orgClientId (resolved via access.access_application), X-Actor-Id → actorId.
 * No hardcoded defaults; CRM endpoints require these headers.
 */
@Component
@RequestScope
public class CrmRequestContext {

    private UUID orgClientId;
    private UUID actorId;

    public UUID getOrgClientId() {
        if (orgClientId == null) {
            throw new IllegalStateException("CRM context not set: X-Application-Id missing or invalid (org_client_id not resolved)");
        }
        return orgClientId;
    }

    public UUID getActorId() {
        if (actorId == null) {
            throw new IllegalStateException("CRM context not set: X-Actor-Id missing or invalid");
        }
        return actorId;
    }

    public void setOrgClientId(UUID orgClientId) {
        this.orgClientId = orgClientId;
    }

    public void setActorId(UUID actorId) {
        this.actorId = actorId;
    }

    /** True if context was populated (e.g. by interceptor for /api/crm/**). */
    public boolean isSet() {
        return orgClientId != null && actorId != null;
    }
}
