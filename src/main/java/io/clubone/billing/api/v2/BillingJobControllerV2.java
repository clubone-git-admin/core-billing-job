package io.clubone.billing.api.v2;

import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.ratelimit.BillingRateLimiter;
import io.clubone.billing.batch.util.LoggingUtils;
import io.clubone.billing.repo.BillingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Version 2 of Billing Job Controller.
 * Provides improved API with better error handling and response structure.
 */
@RestController
@RequestMapping("/api/v2/billing")
public class BillingJobControllerV2 {

    private static final Logger log = LoggerFactory.getLogger(BillingJobControllerV2.class);

    private final JobLauncher jobLauncher;
    private final Job billingJob;
    private final BillingRepository repo;
    private final BillingRateLimiter rateLimiter;
    private final BillingMetrics metrics;

    public BillingJobControllerV2(JobLauncher jobLauncher,
                                  @Qualifier("billingJob") Job billingJob,
                                  BillingRepository repo,
                                  BillingRateLimiter rateLimiter,
                                  BillingMetrics metrics) {
        this.jobLauncher = jobLauncher;
        this.billingJob = billingJob;
        this.repo = repo;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    /**
     * Launch billing job (V2 API).
     * POST /api/v2/billing/run
     */
    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> run(
            @RequestParam("asOfDate") String asOfDateStr,
            @RequestParam(name = "mode", defaultValue = "MOCK") String modeStr) {

        // Validation
        if (asOfDateStr == null || asOfDateStr.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "asOfDate parameter is required",
                "apiVersion", "v2"
            ));
        }

        LocalDate asOfDate;
        try {
            asOfDate = LocalDate.parse(asOfDateStr.trim());
        } catch (DateTimeParseException e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Invalid asOfDate format. Use ISO date format (yyyy-MM-dd)",
                "apiVersion", "v2",
                "asOfDate", asOfDateStr
            ));
        }

        RunMode mode;
        try {
            mode = RunMode.valueOf(modeStr.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Invalid mode. Use MOCK or LIVE",
                "apiVersion", "v2",
                "mode", modeStr
            ));
        }

        // Rate limiting
        if (!rateLimiter.tryConsumeJobExecution()) {
            return ResponseEntity.status(429).body(Map.of(
                "error", "Rate limit exceeded",
                "apiVersion", "v2",
                "retryAfter", "1 hour"
            ));
        }

        String correlationId = LoggingUtils.generateCorrelationId();
        LoggingUtils.setCorrelationId(correlationId);

        var timer = metrics.startJobTimer();
        try {
            JobParameters params = new JobParametersBuilder()
                .addString("asOfDate", asOfDate.toString())
                .addString("runMode", mode.name())
                .addString("correlationId", correlationId)
                .addString("apiVersion", "v2")
                .addLong("ts", System.currentTimeMillis())
                .toJobParameters();

            JobExecution exec = jobLauncher.run(billingJob, params);
            String billingRunId = exec.getExecutionContext().containsKey("billingRunId")
                ? exec.getExecutionContext().getString("billingRunId")
                : null;

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("apiVersion", "v2");
            resp.put("correlationId", correlationId);
            resp.put("jobExecutionId", exec.getId());
            resp.put("jobStatus", exec.getStatus().toString());
            resp.put("billingRunId", billingRunId);
            resp.put("mode", mode.name());
            resp.put("asOfDate", asOfDate.toString());

            if (exec.getStatus() == BatchStatus.FAILED) {
                if (exec.getFailureExceptions() != null && !exec.getFailureExceptions().isEmpty()) {
                    log.error("Billing job failed: correlationId={} jobExecutionId={} billingRunId={}",
                        correlationId, exec.getId(), billingRunId, exec.getFailureExceptions().get(0));
                }
                resp.put("error", "Job execution failed. Please contact support with correlationId: " + correlationId);
                return ResponseEntity.ok(resp);
            }

            log.info("Billing job completed: correlationId={} jobExecutionId={} billingRunId={} status={}",
                correlationId, exec.getId(), billingRunId, exec.getStatus());
            metrics.recordJobExecutionTime(timer);
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            log.error("Failed to launch billing job: correlationId={} asOfDate={} mode={}",
                correlationId, asOfDate, mode, e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Job launch failed. Please contact support with correlationId: " + correlationId,
                "apiVersion", "v2",
                "correlationId", correlationId
            ));
        } finally {
            LoggingUtils.clearContext();
        }
    }
}
