package io.clubone.billing.api;

import io.clubone.billing.batch.RunMode;
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

	public BillingJobController(JobLauncher jobLauncher,
	                            @Qualifier("billingJob") Job billingJob,
	                            BillingRepository repo) {
		this.jobLauncher = jobLauncher;
		this.billingJob = billingJob;
		this.repo = repo;
	}

	// POST /api/billing/run?mode=MOCK&asOfDate=2026-02-01
	// POST /api/billing/run?mode=LIVE&asOfDate=2026-02-01
	@PostMapping("/run")
	public ResponseEntity<Map<String, Object>> run(
			@RequestParam("asOfDate") String asOfDateStr,
			@RequestParam(name = "mode", defaultValue = "MOCK") String modeStr) {

		LocalDate asOfDate;
		try {
			asOfDate = LocalDate.parse(asOfDateStr);
		} catch (DateTimeParseException e) {
			log.warn("Invalid asOfDate parameter: asOfDate={}", asOfDateStr, e);
			return ResponseEntity.badRequest().body(Map.of(
					"error", "Invalid asOfDate. Use ISO date format (yyyy-MM-dd).",
					"asOfDate", asOfDateStr
			));
		}

		RunMode mode;
		try {
			mode = RunMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			log.warn("Invalid mode parameter: mode={}", modeStr, e);
			return ResponseEntity.badRequest().body(Map.of(
					"error", "Invalid mode. Use MOCK or LIVE.",
					"mode", modeStr
			));
		}

		log.info("Launching billing job: asOfDate={} mode={}", asOfDate, mode);

		try {
			JobParameters params = new JobParametersBuilder()
					.addString("asOfDate", asOfDate.toString())
					.addString("runMode", mode.name())
					.addLong("ts", System.currentTimeMillis())
					.toJobParameters();

			JobExecution exec = jobLauncher.run(billingJob, params);
			String billingRunId = exec.getExecutionContext().containsKey("billingRunId")
					? exec.getExecutionContext().getString("billingRunId")
					: null;

			Map<String, Object> resp = new LinkedHashMap<>();
			resp.put("jobExecutionId", exec.getId());
			resp.put("jobStatus", exec.getStatus().toString());
			resp.put("billingRunId", billingRunId);
			resp.put("mode", mode.name());
			resp.put("asOfDate", asOfDate.toString());

			if (exec.getStatus() == BatchStatus.FAILED) {
				String errorMsg = "Job failed";
				if (exec.getFailureExceptions() != null && !exec.getFailureExceptions().isEmpty()) {
					Throwable first = exec.getFailureExceptions().get(0);
					errorMsg = first.getMessage();
					log.error("Billing job failed: jobExecutionId={} billingRunId={}", exec.getId(), billingRunId, first);
				}
				resp.put("error", errorMsg);
				return ResponseEntity.ok(resp);
			}

			log.info("Billing job completed: jobExecutionId={} billingRunId={} status={}", exec.getId(), billingRunId, exec.getStatus());
			return ResponseEntity.ok(resp);

		} catch (Exception e) {
			log.error("Failed to launch billing job: asOfDate={} mode={}", asOfDate, mode, e);
			return ResponseEntity.internalServerError().body(Map.of(
					"error", "Job launch failed: " + e.getMessage(),
					"asOfDate", asOfDate.toString(),
					"mode", mode.name()
			));
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
		try {
			UUID runId = UUID.fromString(billingRunId);
			return ResponseEntity.ok(repo.history(runId, limit));
		} catch (IllegalArgumentException e) {
			log.warn("Invalid billingRunId: {}", billingRunId, e);
			return ResponseEntity.badRequest().build();
		}
	}
}
