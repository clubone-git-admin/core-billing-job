package io.clubone.billing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.notification.NotificationJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * Client for the notification service: POST /notification/api/notification/job/send.
 * Used when logging EMAIL activities for CRM leads.
 */
@Component
public class NotificationClient {

    private static final Logger log = LoggerFactory.getLogger(NotificationClient.class);
    private static final String SEND_PATH = "/notification/api/notification/job/send";
    private static final String HEADER_ACTOR_USER_ID = "X-Actor-UserId";

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final CrmRequestContext context;

    public NotificationClient(
            RestTemplate restTemplate,
            ObjectMapper objectMapper,
            @Value("${clubone.notification.base-url:http://localhost:8000}") String baseUrl,
            CrmRequestContext context
    ) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.context = context;
    }

    /**
     * Sends the notification job to the notification service.
     * Logs request body and response. Swallows errors so that CRM activity logging is not failed by notification failures.
     */
    public void sendJob(NotificationJobRequest request) {
        if (request == null || request.recipients() == null || request.recipients().isEmpty()) {
            log.warn("Notification send skipped: no request or no recipients");
            return;
        }
        String url = baseUrl + SEND_PATH;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(HEADER_ACTOR_USER_ID, context.getActorId().toString());
        HttpEntity<NotificationJobRequest> entity = new HttpEntity<>(request, headers);

        try {
            String requestBody = toJson(request);
            log.info("Notification /send request -> {} | body: {}", url, requestBody);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialize notification request for logging: {}", e.getMessage());
        }

        try {
            String response = restTemplate.postForObject(url, entity, String.class);
            log.info("Notification /send response <- {} | body: {}", url, response != null ? response : "(empty)");
        } catch (Exception e) {
            log.error("Failed to send notification job to {}: {}", url, e.getMessage(), e);
            // Do not rethrow: activity is already persisted; notification can be retried or handled separately
        }
    }

    private String toJson(NotificationJobRequest request) throws JsonProcessingException {
        return objectMapper.writeValueAsString(request);
    }
}
