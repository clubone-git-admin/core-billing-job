package io.clubone.billing.service.duepreview;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.SnapshotRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.S3Service;
import io.clubone.billing.service.schedule.StageRunLeaseHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Background due-preview generation for large volumes (50k+ rows).
 * <p>
 * Not {@code @Transactional} for the whole job — avoids holding one DB connection across SQL + CSV + S3.
 * Candidates are paged via {@link DuePreviewRepository#getDueInvoicesForPreviewPage} (never the unpaged
 * {@link DuePreviewRepository#getDueInvoicesForPreview}) so memory stays bounded regardless of volume.
 * Due preview is read-only (no invoice/schedule mutation), so a stale-reclaim / restart simply rebuilds
 * the CSV from scratch by paging again — there is no resumable checkpoint to protect.
 */
@Service
public class DuePreviewJobRunner {

    private static final Logger log = LoggerFactory.getLogger(DuePreviewJobRunner.class);
    private static final String STAGE = "DUE_PREVIEW";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final DuePreviewRepository duePreviewRepository;
    private final S3Service s3Service;
    private final AuditLogRepository auditLogRepository;
    private final SnapshotRepository snapshotRepository;
    private final long leaseHeartbeatSeconds;
    private final int candidatePageSize;

    public DuePreviewJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DuePreviewRepository duePreviewRepository,
            S3Service s3Service,
            AuditLogRepository auditLogRepository,
            SnapshotRepository snapshotRepository,
            @Value("${clubone.billing.scheduled-stage.lease-heartbeat-seconds:30}") long leaseHeartbeatSeconds,
            @Value("${clubone.billing.due-preview.candidate-page-size:1000}") int candidatePageSize) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.duePreviewRepository = duePreviewRepository;
        this.s3Service = s3Service;
        this.auditLogRepository = auditLogRepository;
        this.snapshotRepository = snapshotRepository;
        this.leaseHeartbeatSeconds = Math.max(5, leaseHeartbeatSeconds);
        this.candidatePageSize = Math.max(100, Math.min(candidatePageSize, 5_000));
    }

    /**
     * Not {@code @Transactional}: long CSV generation must not hold one connection for the whole run.
     */
    public void process(UUID stageRunId) {
        StageRunDto stage = stageRunRepository.findById(stageRunId);
        if (stage == null || !STAGE.equals(stage.stageCode())) {
            log.warn("Due preview job skipped: stage not found or wrong code stageRunId={}", stageRunId);
            return;
        }
        String status = stage.statusCode();
        if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
            log.info("Due preview job skipped: already terminal status={} stageRunId={}", status, stageRunId);
            return;
        }
        if ("RUNNING".equals(status)) {
            // Another worker already claimed RUNNING — do not double-execute.
            log.info("Due preview job skipped (already RUNNING; another worker owns it): stageRunId={}", stageRunId);
            return;
        }
        if ("QUEUED".equals(status) || "PENDING".equals(status) || "SCHEDULED".equals(status) || "IDLE".equals(status)) {
            boolean claimed = stageRunRepository.tryClaimStageRunToRunning(
                    stageRunId, "QUEUED", "PENDING", "SCHEDULED", "IDLE");
            if (!claimed) {
                log.info("Due preview job skipped (lost RUNNING claim): stageRunId={} priorStatus={}", stageRunId, status);
                return;
            }
            log.info("Due preview job claimed RUNNING: stageRunId={} priorStatus={}", stageRunId, status);
            stage = stageRunRepository.findById(stageRunId);
        } else {
            log.warn("Due preview job skipped: unexpected status stageRunId={} status={}", stageRunId, status);
            return;
        }

        Map<String, Object> requestSummary =
                stage.summaryJson() != null ? new HashMap<>(stage.summaryJson()) : new HashMap<>();
        LocalDate dueDate = parseDueDate(requestSummary.get("due_date"));
        UUID requestLocationId = parseUuid(requestSummary.get("location_id"));
        String createdBy = requestSummary.get("created_by") != null
                ? requestSummary.get("created_by").toString()
                : "system";
        UUID billingRunId = stage.billingRunId();

        if (dueDate == null) {
            fail(stageRunId, "Missing due_date in stage summary_json", createdBy);
            return;
        }

        Path tempCsv = null;
        try {
            // Read-only job: no checkpoint to resume — reclaim/restart always rebuilds the CSV from
            // the first page. Paging (not the full unpaged query) is what keeps memory bounded.
            mergeProgress(stageRunId, Map.of(
                    "phase", "STARTED",
                    "processed", 0));

            List<UUID> locationFilter =
                    billingRunRepository.resolveLocationFilterForDuePreviewOrInvoice(billingRunId, requestLocationId);
            log.info(
                    "Due preview job: stageRunId={} billingRunId={} dueDate={} locationFilterSize={} candidatePageSize={}",
                    stageRunId,
                    billingRunId,
                    dueDate,
                    locationFilter != null ? locationFilter.size() : 0,
                    candidatePageSize);

            mergeProgress(stageRunId, Map.of("phase", "BUILDING_CSV", "processed", 0));

            tempCsv = Files.createTempFile("due-preview-", ".csv");
            StageRunLeaseHeartbeat heartbeat =
                    new StageRunLeaseHeartbeat(stageRunRepository, stageRunId, leaseHeartbeatSeconds);

            int processed = 0;
            int eligibleCount = 0;
            int notEligibleCount = 0;
            int pageIndex = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            BigDecimal eligibleTotalAmount = BigDecimal.ZERO;
            UUID afterBillingScheduleId = null;

            try (BufferedWriter writer = Files.newBufferedWriter(tempCsv, StandardCharsets.UTF_8)) {
                writeCsvHeader(writer);
                while (true) {
                    List<Map<String, Object>> page = duePreviewRepository.getDueInvoicesForPreviewPage(
                            dueDate, locationFilter, afterBillingScheduleId, candidatePageSize);
                    if (page.isEmpty()) {
                        break;
                    }
                    pageIndex++;
                    log.info(
                            "Due preview job: page={} size={} stageRunId={} dueDate={} afterScheduleId={}",
                            pageIndex,
                            page.size(),
                            stageRunId,
                            dueDate,
                            afterBillingScheduleId);

                    // Eligibility resolved per page only — never for the whole candidate set at once.
                    List<UUID> pageIds = new ArrayList<>(page.size());
                    for (Map<String, Object> row : page) {
                        if (row.get("subscription_instance_id") instanceof UUID u) {
                            pageIds.add(u);
                        }
                    }
                    Set<UUID> pageEligibleIds = duePreviewRepository.findEligibleSubscriptionInstanceIds(pageIds, dueDate);

                    for (Map<String, Object> row : page) {
                        heartbeat.maybeTouch();

                        UUID subscriptionInstanceId = row.get("subscription_instance_id") instanceof UUID u ? u : null;
                        BigDecimal rowTotal = row.get("total_amount") instanceof BigDecimal bd ? bd : BigDecimal.ZERO;
                        totalAmount = totalAmount.add(rowTotal);

                        boolean eligible = subscriptionInstanceId != null && pageEligibleIds.contains(subscriptionInstanceId);
                        if (eligible) {
                            eligibleCount++;
                            eligibleTotalAmount = eligibleTotalAmount.add(rowTotal);
                        } else {
                            notEligibleCount++;
                        }

                        writeCsvRow(writer, row, eligible);
                        processed++;

                        Object scheduleRaw = row.get("billing_schedule_id");
                        if (scheduleRaw instanceof UUID scheduleId) {
                            afterBillingScheduleId = scheduleId;
                        }
                    }

                    mergeProgress(stageRunId, Map.of(
                            "phase", "BUILDING_CSV",
                            "processed", processed,
                            "eligible_count", eligibleCount,
                            "not_eligible_count", notEligibleCount,
                            "page_index", pageIndex));

                    if (page.size() < candidatePageSize) {
                        break;
                    }
                }
            }

            log.info(
                    "Due preview job: paged candidates done stageRunId={} pages={} processed={}",
                    stageRunId,
                    pageIndex,
                    processed);

            LocalDateTime now = LocalDateTime.now();
            String dateTimeStr = now.format(DateTimeFormatter.ofPattern("dd-MM_HHmm"));
            String fileName = String.format(
                    "due-preview-%s-%s-%s.csv",
                    billingRunId.toString().substring(0, 8),
                    dueDate.toString().replace("-", ""),
                    dateTimeStr);

            mergeProgress(stageRunId, Map.of("phase", "UPLOADING_S3", "processed", processed));
            String s3Path = s3Service.uploadFileToS3(tempCsv, fileName, "text/csv");

            Map<String, Object> summaryJson = new LinkedHashMap<>(requestSummary);
            summaryJson.put("stage_run_id", stageRunId.toString());
            summaryJson.put("s3_path", s3Path);
            summaryJson.put("file_name", fileName);
            summaryJson.put("total_instances", processed);
            summaryJson.put("eligible_count", eligibleCount);
            summaryJson.put("not_eligible_count", notEligibleCount);
            summaryJson.put("total_amount", totalAmount);
            summaryJson.put("eligible_total_amount", eligibleTotalAmount);
            summaryJson.put("generated_at", OffsetDateTime.now(ZoneOffset.UTC).toString());
            summaryJson.put("phase", "COMPLETED");
            summaryJson.put("progress_percent", 100);
            summaryJson.put("processed", processed);
            summaryJson.put("candidate_page_size", candidatePageSize);
            summaryJson.put("pages_processed", pageIndex);

            stageRunRepository.completeStageRun(stageRunId, summaryJson);
            auditLogRepository.insertAuditLog(
                    "DUE_PREVIEW", "STAGE_RUN", stageRunId, "GENERATED", createdBy, summaryJson);

            try {
                UUID createdByUuid = parseUuid(createdBy);
                snapshotRepository.createSnapshot(
                        billingRunId, "DUE_PREVIEW", "DUE_PREVIEW", summaryJson, s3Path, createdByUuid);
            } catch (Exception e) {
                log.warn(
                        "Could not create due-preview snapshot (non-blocking): {}. Ensure lu_snapshot_type has DUE_PREVIEW.",
                        e.getMessage());
            }

            log.info(
                    "Due preview job completed: stageRunId={} total={} eligible={} s3Path={}",
                    stageRunId,
                    processed,
                    eligibleCount,
                    s3Path);
        } catch (Exception e) {
            log.error("Due preview job failed: stageRunId={}", stageRunId, e);
            fail(stageRunId, e.getMessage() != null ? e.getMessage() : e.getClass().getName(), createdBy);
        } finally {
            if (tempCsv != null) {
                try {
                    Files.deleteIfExists(tempCsv);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    private void fail(UUID stageRunId, String message, String createdBy) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("error", message);
        details.put("phase", "FAILED");
        details.put("failed_at", OffsetDateTime.now(ZoneOffset.UTC).toString());
        try {
            stageRunRepository.failStageRun(stageRunId, message, details);
            auditLogRepository.insertAuditLog(
                    "DUE_PREVIEW",
                    "STAGE_RUN",
                    stageRunId,
                    "FAILED",
                    createdBy != null ? createdBy : "system",
                    details);
        } catch (Exception ex) {
            log.error("Failed to mark due preview stage FAILED: stageRunId={}", stageRunId, ex);
        }
    }

    private void mergeProgress(UUID stageRunId, Map<String, Object> patch) {
        try {
            stageRunRepository.mergeStageRunSummaryJson(stageRunId, patch);
        } catch (Exception e) {
            log.warn("Due preview progress merge failed stageRunId={}: {}", stageRunId, e.getMessage());
        }
    }

    private static void writeCsvHeader(BufferedWriter writer) throws IOException {
        writer.write(
                "billing_schedule_id,subscription_instance_id,subscription_plan_id,subscription_plan_code,subscription_id,cycle_number,");
        writer.write("payment_due_date,start_date,last_billed_on,");
        writer.write("client_role_id,role_id,client_first_name,client_last_name,client_email,");
        writer.write("client_agreement_id,client_agreement_status,agreement_name,");
        writer.write("location_name,");
        writer.write("client_payment_method_id,payment_method_name,payment_type_name,card_last4,");
        writer.write("contract_start_date,contract_end_date,");
        writer.write("unit_price,effective_unit_price,price_cycle_start,price_cycle_end,");
        writer.write("sub_total,tax_amount,discount_amount,base_total_amount,adjustment_amount,total_amount,");
        writer.write("subscription_instance_status_name,eligible,eligibility_reason\n");
    }

    private static void writeCsvRow(BufferedWriter writer, Map<String, Object> instance, boolean eligible)
            throws IOException {
        writer.write(csv(instance.get("billing_schedule_id")));
        writer.write(',');
        writer.write(csv(instance.get("subscription_instance_id")));
        writer.write(',');
        writer.write(csv(instance.get("subscription_plan_id")));
        writer.write(',');
        writer.write(csv(instance.get("subscription_plan_code")));
        writer.write(',');
        writer.write(csv(instance.get("subscription_id")));
        writer.write(',');
        writer.write(csv(instance.get("cycle_number")));
        writer.write(',');
        writer.write(csv(instance.get("payment_due_date")));
        writer.write(',');
        writer.write(csv(instance.get("start_date")));
        writer.write(',');
        writer.write(csv(instance.get("last_billed_on")));
        writer.write(',');
        writer.write(csv(instance.get("client_role_id")));
        writer.write(',');
        writer.write(csv(instance.get("role_id")));
        writer.write(',');
        writer.write(csv(instance.get("client_first_name")));
        writer.write(',');
        writer.write(csv(instance.get("client_last_name")));
        writer.write(',');
        writer.write(csv(instance.get("client_email")));
        writer.write(',');
        writer.write(csv(instance.get("client_agreement_id")));
        writer.write(',');
        writer.write(csv(instance.get("client_agreement_status")));
        writer.write(',');
        writer.write(csv(instance.get("agreement_name")));
        writer.write(',');
        writer.write(csv(instance.get("location_name")));
        writer.write(',');
        writer.write(csv(instance.get("client_payment_method_id")));
        writer.write(',');
        writer.write(csv(instance.get("payment_method_name")));
        writer.write(',');
        writer.write(csv(instance.get("payment_type_name")));
        writer.write(',');
        writer.write(csv(instance.get("card_last4")));
        writer.write(',');
        writer.write(csv(instance.get("contract_start_date")));
        writer.write(',');
        writer.write(csv(instance.get("contract_end_date")));
        writer.write(',');
        writer.write(csv(instance.get("unit_price")));
        writer.write(',');
        writer.write(csv(instance.get("effective_unit_price")));
        writer.write(',');
        writer.write(csv(instance.get("price_cycle_start")));
        writer.write(',');
        writer.write(csv(instance.get("price_cycle_end")));
        writer.write(',');
        writer.write(csv(instance.get("sub_total")));
        writer.write(',');
        writer.write(csv(instance.get("tax_amount")));
        writer.write(',');
        writer.write(csv(instance.get("discount_amount")));
        writer.write(',');
        writer.write(csv(instance.get("base_total_amount")));
        writer.write(',');
        writer.write(csv(instance.get("adjustment_amount")));
        writer.write(',');
        writer.write(csv(instance.get("total_amount")));
        writer.write(',');
        writer.write(csv(instance.get("subscription_instance_status_name")));
        writer.write(',');
        writer.write(csv(eligible));
        writer.write(',');
        writer.write(csv(eligible ? "ELIGIBLE" : "NOT_ELIGIBLE"));
        writer.write('\n');
    }

    private static String csv(Object value) {
        if (value == null) {
            return "";
        }
        String str = value.toString();
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    private static LocalDate parseDueDate(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof LocalDate d) {
            return d;
        }
        try {
            return LocalDate.parse(raw.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private static UUID parseUuid(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof UUID u) {
            return u;
        }
        try {
            return UUID.fromString(raw.toString());
        } catch (Exception e) {
            return null;
        }
    }
}
