package io.clubone.billing.api.context;

import io.clubone.billing.repo.AccessApplicationRepository;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.UUID;

/**
 * Interceptor for /api/crm/**. Sets CrmRequestContext from headers:
 * X-Application-Id → resolve org_client_id from access.access_application; X-Actor-Id → actorId.
 * Returns 400 if headers are missing or application is not found.
 */
@Component
public class CrmContextInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(CrmContextInterceptor.class);
    public static final String HEADER_APPLICATION_ID = "X-Application-Id";
    /** Alternate header name some clients send (case-insensitive). */
    private static final String HEADER_APPLICATION_ID_ALT = "Application-Id";
    public static final String HEADER_ACTOR_ID = "X-Actor-Id";
    private static final String HEADER_ACTOR_ID_ALT = "Actor-Id";

    private final CrmRequestContext context;
    private final AccessApplicationRepository accessApplicationRepository;

    public CrmContextInterceptor(CrmRequestContext context, AccessApplicationRepository accessApplicationRepository) {
        this.context = context;
        this.accessApplicationRepository = accessApplicationRepository;
    }

    private static String getHeaderOrAlternate(HttpServletRequest request, String primary, String alternate) {
        String v = request.getHeader(primary);
        if (v != null && !v.isBlank()) return v;
        return request.getHeader(alternate);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // CORS preflight (OPTIONS) does not send custom headers; let it through so the browser gets CORS response
        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
            return true;
        }

        String applicationIdStr = getHeaderOrAlternate(request, HEADER_APPLICATION_ID, HEADER_APPLICATION_ID_ALT);
        String actorIdStr = getHeaderOrAlternate(request, HEADER_ACTOR_ID, HEADER_ACTOR_ID_ALT);

        if (applicationIdStr == null || applicationIdStr.isBlank()) {
            log.warn("CRM request missing {}", HEADER_APPLICATION_ID);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Missing or empty X-Application-Id header\"}");
            return false;
        }
        if (actorIdStr == null || actorIdStr.isBlank()) {
            log.warn("CRM request missing {}", HEADER_ACTOR_ID);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Missing or empty X-Actor-Id header\"}");
            return false;
        }

        UUID applicationId;
        UUID actorId;
        try {
            applicationId = UUID.fromString(applicationIdStr.trim());
            actorId = UUID.fromString(actorIdStr.trim());
        } catch (IllegalArgumentException e) {
            log.warn("CRM request invalid UUID in header: app={}, actor={}", applicationIdStr, actorIdStr);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"X-Application-Id and X-Actor-Id must be valid UUIDs\"}");
            return false;
        }

        UUID orgClientId = accessApplicationRepository.findOrgClientIdByApplicationId(applicationId);
        if (orgClientId == null) {
            log.warn("CRM request application not found or inactive: applicationId={}", applicationId);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"Application not found or inactive for X-Application-Id\"}");
            return false;
        }

        context.setOrgClientId(orgClientId);
        context.setActorId(actorId);
        return true;
    }
}
