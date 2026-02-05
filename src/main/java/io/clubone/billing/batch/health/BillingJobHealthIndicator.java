package io.clubone.billing.batch.health;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom health indicator for billing job.
 * Checks database connectivity, payment service availability, and DLQ status.
 */
@Component
public class BillingJobHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(BillingJobHealthIndicator.class);

    private final JdbcTemplate jdbc;
    private final RestTemplate restTemplate;
    private final BillingJobProperties props;
    private final DeadLetterQueueService dlqService;

    public BillingJobHealthIndicator(JdbcTemplate jdbc, RestTemplate restTemplate,
                                     BillingJobProperties props, DeadLetterQueueService dlqService) {
        this.jdbc = jdbc;
        this.restTemplate = restTemplate;
        this.props = props;
        this.dlqService = dlqService;
    }

    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        Map<String, Object> details = new HashMap<>();
        boolean allHealthy = true;

        // Check database connectivity
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            details.put("database", Map.of("status", "UP", "message", "Connected"));
        } catch (Exception e) {
            allHealthy = false;
            details.put("database", Map.of("status", "DOWN", "error", e.getMessage()));
            builder.withDetail("database", Map.of("status", "DOWN", "error", e.getMessage()));
            return builder.down().withDetails(details).build();
        }

        // Check payment service availability (only if HTTP strategy)
        if ("HTTP".equalsIgnoreCase(props.getPayment().getStrategy())) {
            try {
                String healthUrl = props.getPayment().getHttp().getBaseUrl() + "/actuator/health";
                var response = restTemplate.getForEntity(healthUrl, Map.class);
                if (response.getStatusCode().is2xxSuccessful()) {
                    details.put("paymentService", Map.of("status", "UP", "url", healthUrl));
                } else {
                    details.put("paymentService", Map.of("status", "WARNING", "statusCode", response.getStatusCode().value()));
                }
            } catch (Exception e) {
                details.put("paymentService", Map.of("status", "DOWN", "error", e.getMessage()));
                // Payment service down doesn't make entire system down, just degraded
            }
        } else {
            details.put("paymentService", Map.of("status", "N/A", "strategy", props.getPayment().getStrategy()));
        }

        // Check DLQ size
        try {
            int dlqSize = dlqService.getUnresolvedCount();
            details.put("deadLetterQueue", Map.of("size", dlqSize, "status", dlqSize > 100 ? "WARNING" : "OK"));
            if (dlqSize > 100) {
                // Use DOWN status when DLQ exceeds threshold, but add warning detail
                builder.status(Status.DOWN);
                builder.withDetail("dlqWarning", "DLQ size exceeds threshold: " + dlqSize);
                allHealthy = false;
            }
        } catch (Exception e) {
            details.put("deadLetterQueue", Map.of("status", "CHECK_FAILED", "error", e.getMessage()));
        }

        // Check active jobs
        try {
            Integer activeJobs = jdbc.queryForObject(
                "SELECT COUNT(1) FROM client_subscription_billing.billing_run WHERE status = 'RUNNING'",
                Integer.class
            );
            details.put("activeJobs", activeJobs != null ? activeJobs : 0);
        } catch (Exception e) {
            details.put("activeJobs", "CHECK_FAILED");
        }

        builder.withDetails(details);

        if (allHealthy) {
            return builder.up().build();
        } else {
            return builder.down().build();
        }
    }
}
