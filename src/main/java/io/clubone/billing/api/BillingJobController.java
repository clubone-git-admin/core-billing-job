package io.clubone.billing.api;

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

@RestController
@RequestMapping("/api/billing")
public class BillingJobController {

	private static final Logger log = LoggerFactory.getLogger(BillingJobController.class);

	private final JobLauncher jobLauncher;
	private final Job billingJob;
	private final BillingRepository repo;
	private final BillingRateLimiter rateLimiter;
	private final BillingMetrics metrics;

	public BillingJobController(JobLauncher jobLauncher,
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

	// POST /api/billing/run?mode=MOCK&asOfDate=2026-02-01
	// POST /api/billing/run?mode=LIVE&asOfDate=2026-02-01
	@PostMapping("/run")
	public ResponseEntity<Map<String, Object>> run(
			@RequestParam("asOfDate") String asOfDateStr,
			@RequestParam(name = "mode", defaultValue = "MOCK") String modeStr) {

		// Validate asOfDate parameter
		if (asOfDateStr == null || asOfDateStr.trim().isEmpty()) {
			log.warn("Missing asOfDate parameter");
			return ResponseEntity.badRequest().body(Map.of(
					"error", "asOfDate parameter is required. Use ISO date format (yyyy-MM-dd)."
			));
		}

		LocalDate asOfDate;
		try {
			asOfDate = LocalDate.parse(asOfDateStr.trim());
		} catch (DateTimeParseException e) {
			log.warn("Invalid asOfDate parameter: asOfDate={}", asOfDateStr, e);
			return ResponseEntity.badRequest().body(Map.of(
					"error", "Invalid asOfDate. Use ISO date format (yyyy-MM-dd).",
					"asOfDate", asOfDateStr
			));
		}

		// Validate asOfDate is not too far in the future (optional business rule)
		LocalDate maxFutureDate = LocalDate.now().plusYears(1);
		if (asOfDate.isAfter(maxFutureDate)) {
			log.warn("asOfDate is too far in the future: asOfDate={}", asOfDate);
			return ResponseEntity.badRequest().body(Map.of(
					"error", "asOfDate cannot be more than 1 year in the future.",
					"asOfDate", asOfDateStr
			));
		}

		// Validate mode parameter
		if (modeStr == null || modeStr.trim().isEmpty()) {
			modeStr = "MOCK"; // Default value
		}

		RunMode mode;
		try {
			mode = RunMode.valueOf(modeStr.trim().toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			log.warn("Invalid mode parameter: mode={}", modeStr, e);
			return ResponseEntity.badRequest().body(Map.of(
					"error", "Invalid mode. Use MOCK or LIVE.",
					"mode", modeStr
			));
		}

		// Rate limiting check
		if (!rateLimiter.tryConsumeJobExecution()) {
			log.warn("Job execution rate limit exceeded: asOfDate={} mode={}", asOfDate, mode);
			return ResponseEntity.status(429).body(Map.of(
				"error", "Rate limit exceeded. Maximum 10 jobs per hour allowed.",
				"retryAfter", "1 hour"
			));
		}

		// Generate correlation ID for this request
		String correlationId = LoggingUtils.generateCorrelationId();
		LoggingUtils.setCorrelationId(correlationId);
		
		log.info("Launching billing job: correlationId={} asOfDate={} mode={}", correlationId, asOfDate, mode);

		var timer = metrics.startJobTimer();
		try {
			JobParameters params = new JobParametersBuilder()
					.addString("asOfDate", asOfDate.toString())
					.addString("runMode", mode.name())
					.addString("correlationId", correlationId)
					.addLong("ts", System.currentTimeMillis())
					.toJobParameters();

			JobExecution exec = jobLauncher.run(billingJob, params);
			String billingRunId = exec.getExecutionContext().containsKey("billingRunId")
					? exec.getExecutionContext().getString("billingRunId")
					: null;

			Map<String, Object> resp = new LinkedHashMap<>();
			resp.put("correlationId", correlationId);
			resp.put("jobExecutionId", exec.getId());
			resp.put("jobStatus", exec.getStatus().toString());
			resp.put("billingRunId", billingRunId);
			resp.put("mode", mode.name());
			resp.put("asOfDate", asOfDate.toString());

			if (exec.getStatus() == BatchStatus.FAILED) {
				// Log detailed error server-side
				if (exec.getFailureExceptions() != null && !exec.getFailureExceptions().isEmpty()) {
					Throwable first = exec.getFailureExceptions().get(0);
					log.error("Billing job failed: correlationId={} jobExecutionId={} billingRunId={}", 
						correlationId, exec.getId(), billingRunId, first);
				}
				// Return generic error message to client
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
			// Return generic error message, detailed error logged server-side
			return ResponseEntity.internalServerError().body(Map.of(
					"error", "Job launch failed. Please contact support with correlationId: " + correlationId,
					"correlationId", correlationId,
					"asOfDate", asOfDate.toString(),
					"mode", mode.name()
			));
		} finally {
			LoggingUtils.clearContext();
		}
	}

	// GET /api/billing/run/{billingRunId}/summary
	@GetMapping("/run/{billingRunId}/summary")
	public ResponseEntity<Map<String, Object>> summary(@PathVariable String billingRunId) {
		try {
			UUID runId = UUID.fromString(billingRunId);
			Map<String, Object> resp = new LinkedHashMap<>();
			resp.put("billingRunId", billingRunId);
			resp.put("countsByStatus", repo.countsByStatus(runId));
			return ResponseEntity.ok(resp);
		} catch (IllegalArgumentException e) {
			log.warn("Invalid billingRunId: {}", billingRunId, e);
			return ResponseEntity.badRequest().body(Map.of("error", "Invalid billingRunId format."));
		} catch (Exception e) {
			log.error("Failed to get summary for billingRunId={}", billingRunId, e);
			throw e;
		}
	}

	// GET /api/billing/run/{billingRunId}/history?limit=200
	@GetMapping("/run/{billingRunId}/history")
	public ResponseEntity<List<Map<String, Object>>> history(@PathVariable String billingRunId,
	                                                          @RequestParam(name = "limit", defaultValue = "200") int limit) {
		// Validate limit parameter
		if (limit <= 0) {
			log.warn("Invalid limit parameter: limit={}", limit);
			return ResponseEntity.badRequest().build();
		}
		// Cap limit at reasonable maximum to prevent excessive memory usage
		int maxLimit = 10000;
		if (limit > maxLimit) {
			log.warn("Limit exceeds maximum, capping at {}: requested={}", maxLimit, limit);
			limit = maxLimit;
		}

		try {
			UUID runId = UUID.fromString(billingRunId);
			return ResponseEntity.ok(repo.history(runId, limit));
		} catch (IllegalArgumentException e) {
			log.warn("Invalid billingRunId: {}", billingRunId, e);
			return ResponseEntity.badRequest().build();
		}
	}
}
