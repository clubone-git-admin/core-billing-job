package io.clubone.billing.service;

import io.clubone.billing.api.dto.ApproveDuePreviewRequest;
import io.clubone.billing.api.dto.DuePreviewRequest;
import io.clubone.billing.api.dto.DuePreviewRunHistoryDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.repo.ApprovalRepository;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.SnapshotRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.service.duepreview.DuePreviewQueuedEvent;
import io.clubone.billing.util.BillingReadExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Service for due preview operations. Uses an existing billing_run (request.billRunId), creates only a billing_stage_run
 * and optionally audit log and snapshot; stores the output in S3.
 */
@Service
public class DuePreviewService {

    private static final Logger log = LoggerFactory.getLogger(DuePreviewService.class);
    private static final DateTimeFormatter LINE_CYCLE_DATE_FORMAT = DateTimeFormatter.ofPattern("MMM d", Locale.US);

    private final DuePreviewRepository duePreviewRepository;
    private final BillingRunRepository billingRunRepository;
    private final StageRunRepository stageRunRepository;
    private final S3Service s3Service;
    private final AuditLogRepository auditLogRepository;
    private final SnapshotRepository snapshotRepository;
    private final ApprovalRepository approvalRepository;
    private final BillingReadExecutors readExecutors;
    private final ApplicationEventPublisher applicationEventPublisher;

    public DuePreviewService(
            DuePreviewRepository duePreviewRepository,
            BillingRunRepository billingRunRepository,
            StageRunRepository stageRunRepository,
            S3Service s3Service,
            AuditLogRepository auditLogRepository,
            SnapshotRepository snapshotRepository,
            ApprovalRepository approvalRepository,
            BillingReadExecutors readExecutors,
            ApplicationEventPublisher applicationEventPublisher) {
        this.duePreviewRepository = duePreviewRepository;
        this.billingRunRepository = billingRunRepository;
        this.stageRunRepository = stageRunRepository;
        this.s3Service = s3Service;
        this.auditLogRepository = auditLogRepository;
        this.snapshotRepository = snapshotRepository;
        this.approvalRepository = approvalRepository;
        this.readExecutors = readExecutors;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Null = no location filter. If {@code billing_run_location} has rows, only those ids are used
     * (not {@code br.location_id} or request body). Otherwise legacy scope, then request id.
     */
    private List<UUID> resolveLocationFilterForDuePreview(UUID billingRunId, UUID requestLocationId) {
        return billingRunRepository.resolveLocationFilterForDuePreviewOrInvoice(
                billingRunId, requestLocationId);
    }

    /**
     * List due preview run history with pagination.
     *
     * @param billingRunId if non-null, restrict to due preview stage runs for this parent billing run
     * @param limit    page size
     * @param offset   offset for pagination
     * @param sortBy   optional sort field (e.g. generated_at, status, run_id)
     * @param sortOrder asc or desc
     * @return paginated list of due preview run history records
     */
    public PageResponse<DuePreviewRunHistoryDto> listDuePreviewRunHistory(
            UUID billingRunId, int limit, int offset, String sortBy, String sortOrder) {
        CompletableFuture<Integer> totalF =
                readExecutors.supplyAsync(() -> duePreviewRepository.countDuePreviewRunHistory(billingRunId));
        CompletableFuture<List<DuePreviewRunHistoryDto>> dataF = readExecutors.supplyAsync(
                () -> duePreviewRepository.findDuePreviewRunHistory(
                        billingRunId, limit, offset, sortBy, sortOrder));
        CompletableFuture.allOf(totalF, dataF).join();
        return PageResponse.of(dataF.join(), totalF.join(), limit, offset);
    }

    /**
     * Reads S3 URI from stage summary_json ({@code s3_path} or legacy {@code s3Path}).
     */
    private static String s3PathFromSummary(Map<String, Object> summary) {
        if (summary == null || summary.isEmpty()) {
            return null;
        }
        Object v = summary.get("s3_path");
        if (v == null) {
            v = summary.get("s3Path");
        }
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    /**
     * Resolves the CSV file location: summary_json first, then snapshot matched by {@code stage_run_id},
     * then latest DUE_PREVIEW snapshot for the billing run (legacy).
     */
    private String resolveDuePreviewS3Path(UUID billingRunId, UUID stageRunId, Map<String, Object> summary) {
        String path = s3PathFromSummary(summary);
        if (path != null && !path.isBlank()) {
            return path;
        }
        if (stageRunId != null) {
            try {
                String byStage = snapshotRepository.findDuePreviewSnapshotS3PathByStageRunId(stageRunId);
                if (byStage != null && !byStage.isBlank()) {
                    return byStage.trim();
                }
            } catch (Exception e) {
                log.warn("Could not load due-preview snapshot for stageRunId={}: {}", stageRunId, e.getMessage());
            }
        }
        if (billingRunId == null) {
            return null;
        }
        try {
            List<Map<String, Object>> snaps = snapshotRepository.findByBillingRunId(billingRunId, "DUE_PREVIEW", "DUE_PREVIEW");
            if (snaps.isEmpty()) {
                return null;
            }
            Object sp = snaps.get(0).get("s3_path");
            if (sp != null) {
                String t = sp.toString().trim();
                if (!t.isEmpty()) {
                    return t;
                }
            }
        } catch (Exception e) {
            log.warn("Could not load due-preview snapshot for billingRunId={}: {}", billingRunId, e.getMessage());
        }
        return null;
    }

    /**
     * Restores {@code s3_path} / {@code file_name} on DUE_PREVIEW stage rows when approve overwrote {@code summary_json}
     * but a matching snapshot row exists (see {@link #resolveDuePreviewS3Path}).
     */
    public List<StageRunDto> enrichDuePreviewStageSummaries(UUID billingRunId, List<StageRunDto> stages) {
        if (stages == null || billingRunId == null) {
            return stages;
        }
        List<StageRunDto> out = new ArrayList<>(stages.size());
        for (StageRunDto s : stages) {
            if (!"DUE_PREVIEW".equals(s.stageCode())) {
                out.add(s);
                continue;
            }
            Map<String, Object> sj = s.summaryJson();
            HashMap<String, Object> merged = sj != null ? new HashMap<>(sj) : new HashMap<>();
            String path = resolveDuePreviewS3Path(billingRunId, s.stageRunId(), merged);
            if (path != null && !path.isBlank()) {
                if (merged.get("s3_path") == null) {
                    merged.put("s3_path", path);
                }
                if (merged.get("file_name") == null) {
                    int slash = path.lastIndexOf('/');
                    if (slash >= 0 && slash < path.length() - 1) {
                        merged.put("file_name", path.substring(slash + 1));
                    }
                }
            }
            out.add(new StageRunDto(
                    s.stageRunId(), s.stageRunCode(), s.billingRunId(),
                    s.stageCode(), s.stageDisplayName(), s.stageSequence(),
                    s.statusCode(), s.statusDisplayName(), s.scheduledFor(),
                    s.startedOn(), s.endedOn(), merged.isEmpty() ? null : merged,
                    s.errorMessage(), s.errorDetails(), s.attemptNumber(), s.maxAttempts(),
                    s.isLocked()));
        }
        return out;
    }

    /**
     * Get due preview run details by stage_run_id. Loads run metadata from DB and invoice rows from S3 CSV.
     *
     * @param stageRunId billing_stage_run.stage_run_id (DUE_PREVIEW stage)
     * @return Map with "run" (run_id, run_code, generated_at, status, filename, invoices, totalAmount, totalTax,
     *         totalDiscount, summary_json) and "invoices" (list of row maps from CSV, each with {@code line_items})
     */
    public Map<String, Object> getDuePreviewRunDetails(UUID stageRunId) {
        return getDuePreviewRunDetails(stageRunId, true, null);
    }

    /**
     * @param includeInvoices when false, skip S3 CSV download (fast poll for QUEUED/RUNNING/COMPLETED)
     * @param invoiceLimit    when includeInvoices, optional max CSV rows returned (null = all; use for UI caps)
     */
    public Map<String, Object> getDuePreviewRunDetails(
            UUID stageRunId, boolean includeInvoices, Integer invoiceLimit) {
        StageRunDto stageRun = stageRunRepository.findById(stageRunId);
        if (stageRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
        }
        if (!"DUE_PREVIEW".equals(stageRun.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Not a due preview run: " + stageRunId);
        }

        Map<String, Object> summaryJson = stageRun.summaryJson() != null ? new HashMap<>(stageRun.summaryJson()) : new HashMap<>();
        String s3Path = resolveDuePreviewS3Path(stageRun.billingRunId(), stageRun.stageRunId(), summaryJson);
        String status = stageRun.statusCode();
        // Poll while async job runs — return progress without requiring S3 yet
        if (s3Path == null || s3Path.isBlank()) {
            if (isDuePreviewInFlight(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                Map<String, Object> inFlight = buildInFlightDuePreviewDetails(stageRun, summaryJson);
                enrichRunCreator(inFlight, stageRun.stageRunId(), summaryJson);
                return inFlight;
            }
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Due preview file not found in S3 for run: " + stageRunId);
        }
        // Status-only / progress poll after COMPLETED — avoid loading 30k+ CSV rows into HTTP
        if (!includeInvoices) {
            Map<String, Object> summaryOnly =
                    buildSummaryOnlyDuePreviewDetails(stageRun, summaryJson, s3Path);
            enrichRunCreator(summaryOnly, stageRun.stageRunId(), summaryJson);
            return summaryOnly;
        }
        if (summaryJson.get("s3_path") == null) {
            summaryJson.put("s3_path", s3Path);
        }
        if (summaryJson.get("file_name") == null) {
            int slash = s3Path.lastIndexOf('/');
            if (slash >= 0 && slash < s3Path.length() - 1) {
                summaryJson.put("file_name", s3Path.substring(slash + 1));
            }
        }

        String csvContent = s3Service.downloadFromS3(s3Path);
        List<Map<String, Object>> invoices = parseDuePreviewCsv(csvContent);
        if (invoiceLimit != null && invoiceLimit >= 0 && invoices.size() > invoiceLimit) {
            invoices = new ArrayList<>(invoices.subList(0, invoiceLimit));
            summaryJson.put("invoices_truncated", true);
            summaryJson.put("invoice_limit", invoiceLimit);
        }
        ensureInvoiceAttributes(invoices);
        enrichMissingSubscriptionPlanCodes(invoices);

        Set<UUID> scheduleIds = invoices.stream()
                .map(row -> parseUuid(row.get("billing_schedule_id")))
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<UUID, String> lineDescriptions = duePreviewRepository.findDuePreviewLineDescriptionsByBillingScheduleIds(scheduleIds);
        attachDuePreviewLineItems(invoices, lineDescriptions);

        BigDecimal totalTaxFromRows = sumNumericColumn(invoices, "tax_amount");
        BigDecimal totalDiscountFromRows = sumNumericColumn(invoices, "discount_amount");
        boolean mixedCurrency = Boolean.TRUE.equals(summaryJson.get("mixed_currency"));

        // Build run object for response
        Object totalAmountObj = summaryJson.get("total_amount");
        BigDecimal totalAmount = totalAmountObj instanceof Number n ? BigDecimal.valueOf(n.doubleValue()) : null;
        Integer invoicesCount = summaryJson.get("total_instances") instanceof Number n ? n.intValue() : (invoices != null ? invoices.size() : 0);
        if (summaryJson.get("invoices_count") == null && invoicesCount != null) {
            summaryJson.put("invoices_count", invoicesCount);
        }
        if (summaryJson.get("failure_count") == null) {
            summaryJson.put("failure_count", 0);
        }
        if (mixedCurrency) {
            summaryJson.put("total_tax", null);
            summaryJson.put("total_discount", null);
            summaryJson.put("total_tax_note", "Use by_currency — mixed currencies cannot be summed");
        } else {
            summaryJson.put("total_tax", totalTaxFromRows);
            summaryJson.put("total_discount", totalDiscountFromRows);
        }

        Map<String, Object> run = new LinkedHashMap<>();
        run.put("run_id", stageRun.stageRunId());
        run.put("run_code", stageRun.stageRunCode());
        run.put("generated_at", stageRun.endedOn() != null ? stageRun.endedOn() : stageRun.startedOn());
        run.put("status", stageRun.statusCode());
        run.put("status_display_name", stageRun.statusDisplayName());
        run.put("filename", summaryJson.get("file_name"));
        run.put("invoices", invoicesCount);
        run.put("totalAmount", mixedCurrency ? null : (totalAmount != null ? totalAmount : BigDecimal.ZERO));
        run.put("totalTax", mixedCurrency ? null : totalTaxFromRows);
        run.put("totalDiscount", mixedCurrency ? null : totalDiscountFromRows);
        run.put("summary_json", summaryJson);
        putStageRunCreator(run, stageRun.stageRunId(), summaryJson);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("run", run);
        response.put("invoices", invoices != null ? invoices : List.of());
        return response;
    }

    /**
     * Get due preview CSV content from S3 by stage run ID.
     *
     * @param stageRunId billing_stage_run.stage_run_id (DUE_PREVIEW stage)
     * @return CSV content as string
     */
    public String getDuePreviewCsvContent(UUID stageRunId) {
        StageRunDto stageRun = stageRunRepository.findById(stageRunId);
        if (stageRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
        }
        if (!"DUE_PREVIEW".equals(stageRun.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Not a due preview run: " + stageRunId);
        }

        Map<String, Object> summaryJson = stageRun.summaryJson() != null ? new HashMap<>(stageRun.summaryJson()) : new HashMap<>();
        String s3Path = resolveDuePreviewS3Path(stageRun.billingRunId(), stageRun.stageRunId(), summaryJson);
        if (s3Path == null || s3Path.isBlank()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Due preview file not found in S3 for run: " + stageRunId);
        }

        return s3Service.downloadFromS3(s3Path);
    }

    /**
     * Get due preview filename from stage run summary.
     *
     * @param stageRunId billing_stage_run.stage_run_id (DUE_PREVIEW stage)
     * @return filename (e.g. "due-preview-abc12345-20260219.csv")
     */
    public String getDuePreviewFilename(UUID stageRunId) {
        StageRunDto stageRun = stageRunRepository.findById(stageRunId);
        if (stageRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
        }
        if (!"DUE_PREVIEW".equals(stageRun.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Not a due preview run: " + stageRunId);
        }

        Map<String, Object> summaryJson = stageRun.summaryJson();
        if (summaryJson != null) {
            String fileName = (String) summaryJson.get("file_name");
            if (fileName != null && !fileName.isBlank()) {
                return fileName;
            }
        }
        // Fallback: generate filename from stage run ID and date
        return String.format("due-preview-%s.csv", stageRunId.toString().substring(0, 8));
    }

    /**
     * Approve or deny a due preview stage run.
     * - Creates approval record in billing_run_approval
     * - Updates billing_run.approval_status_id
     * - Marks DUE_PREVIEW stage run as COMPLETED
     * - If approved: creates INVOICE_GENERATION stage run (IDLE until user runs generation) and updates billing_run.current_stage_code_id
     * - If denied: only completes DUE_PREVIEW, no next stage transition
     *
     * @param stageRunId The DUE_PREVIEW stage run ID
     * @param request Approval/denial request with approver info
     * @param approved true to approve, false to deny
     * @return Updated stage run DTO
     */
    @Transactional
    public StageRunDto approveOrDenyDuePreview(UUID stageRunId, ApproveDuePreviewRequest request, boolean approved) {
        // Validate stage run exists and is DUE_PREVIEW
        StageRunDto duePreviewStage = stageRunRepository.findById(stageRunId);
        if (duePreviewStage == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
        }
        if (!"DUE_PREVIEW".equals(duePreviewStage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Not a DUE_PREVIEW stage run: " + stageRunId);
        }

        UUID billingRunId = duePreviewStage.billingRunId();
        var billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }

        // Apply defaults for optional fields
        String approverRole = request.approverRole() != null && !request.approverRole().isBlank() 
                ? request.approverRole() : "APPROVER";
        Integer approvalLevel = request.approvalLevel() != null ? request.approvalLevel() : 1;

        // Create or update approval record in billing_run_approval
        try {
            // Check if approval record already exists for this level
            var existingApprovals = approvalRepository.findByBillingRunId(billingRunId);
            var existingApproval = existingApprovals.stream()
                    .filter(a -> a.approvalLevel().equals(approvalLevel))
                    .findFirst();

            if (existingApproval.isPresent()) {
                // Update existing approval
                if (approved) {
                    approvalRepository.approve(billingRunId, approvalLevel, request.approverId(), request.notes());
                } else {
                    approvalRepository.reject(billingRunId, approvalLevel, request.approverId(), 
                            request.notes() != null ? request.notes() : request.rejectionReason());
                }
            } else {
                // Create new approval record
                approvalRepository.createApproval(billingRunId, approvalLevel, approverRole);
                if (approved) {
                    approvalRepository.approve(billingRunId, approvalLevel, request.approverId(), request.notes());
                } else {
                    approvalRepository.reject(billingRunId, approvalLevel, request.approverId(), 
                            request.notes() != null ? request.notes() : request.rejectionReason());
                }
            }
        } catch (Exception e) {
            log.warn("Could not create/update approval record (non-blocking): {}", e.getMessage());
        }

        // Update billing_run.approval_status_id
        String approvalStatus = approved ? "APPROVED" : "REJECTED";
		/*
		 * approvalRepository.updateBillingRunApprovalStatus(billingRunId,
		 * approvalStatus, request.approverId(), request.notes() != null ?
		 * request.notes() : request.rejectionReason());
		 */
        // Mark DUE_PREVIEW stage run as COMPLETED (merge into existing summary so s3_path / file_name are kept)
        Map<String, Object> summaryJson = new HashMap<>();
        if (duePreviewStage.summaryJson() != null) {
            summaryJson.putAll(duePreviewStage.summaryJson());
        }
        summaryJson.put("approved", approved);
        summaryJson.put("approver_id", request.approverId().toString());
        summaryJson.put("approver_role", approverRole);
        summaryJson.put("approval_level", approvalLevel);
        summaryJson.put("notes", request.notes());
        if (!approved && request.rejectionReason() != null) {
            summaryJson.put("rejection_reason", request.rejectionReason());
        }
        stageRunRepository.completeStageRun(stageRunId, summaryJson);

        // If approved: transition to INVOICE_GENERATION stage (IDLE until POST invoice-generation/runs "Run")
        if (approved) {
            StageRunDto existingInvoiceGen = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, "INVOICE_GENERATION");
            UUID invoiceGenStageRunId;
            if (existingInvoiceGen == null) {
                invoiceGenStageRunId = stageRunRepository.createStageRun(
                        billingRunId, "INVOICE_GENERATION", OffsetDateTime.now(), null, request.approverId(), false);
            } else {
                invoiceGenStageRunId = existingInvoiceGen.stageRunId();
            }

            boolean idleApplied = stageRunRepository.trySetStageRunStatusByCode(invoiceGenStageRunId, "IDLE");
            if (!idleApplied) {
                log.warn(
                        "INVOICE_GENERATION stage could not be set to IDLE (add billing_config.stage_run_status IDLE). stageRunId={}",
                        invoiceGenStageRunId);
            }

            billingRunRepository.updateCurrentStage(billingRunId, "INVOICE_GENERATION");

            log.info("Transitioned to INVOICE_GENERATION stage (IDLE): billingRunId={}, stageRunId={}", billingRunId, invoiceGenStageRunId);
        }

        // Audit log
        auditLogRepository.insertAuditLog(
                "DUE_PREVIEW",
                "STAGE_RUN",
                stageRunId,
                approved ? "APPROVED" : "DENIED",
                request.approverId().toString(),
                summaryJson);

        return stageRunRepository.findById(stageRunId);
    }

    /**
     * Parse due preview CSV (header + rows) into list of maps with snake_case keys matching CSV columns.
     */
    private List<Map<String, Object>> parseDuePreviewCsv(String csvContent) {
        if (csvContent == null || csvContent.isBlank()) {
            return List.of();
        }
        String[] lines = csvContent.split("\n");
        if (lines.length < 2) {
            return List.of();
        }
        String[] headers = parseCsvLine(lines[0]);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String[] values = parseCsvLine(lines[i]);
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < headers.length; j++) {
                String key = headers[j].trim();
                String value = j < values.length ? values[j].trim() : "";
                row.put(key, value.isEmpty() ? null : value);
            }
            rows.add(row);
        }
        return rows;
    }

    /** Keys for invoice attributes that should always be present in get run details response. */
    private static final String[] INVOICE_ATTRIBUTE_KEYS = {
            "role_id", "client_agreement_status", "agreement_name", "location_name", "currency_code",
            "payment_method_name", "payment_type_name", "card_last4", "subscription_id",
            "subscription_plan_id", "subscription_plan_code"
    };

    /**
     * Ensure each invoice map has the standard attributes (role_id, client_agreement_status, agreement_name,
     * location_name, payment_method_name, payment_type_name, card_last4, subscription_id,
     * subscription_plan_id, subscription_plan_code) so the API response is consistent; missing keys get null.
     */
    private void ensureInvoiceAttributes(List<Map<String, Object>> invoices) {
        if (invoices == null) return;
        for (Map<String, Object> row : invoices) {
            for (String key : INVOICE_ATTRIBUTE_KEYS) {
                if (!row.containsKey(key)) {
                    row.put(key, null);
                }
            }
        }
    }

    /**
     * Backfill {@code subscription_plan_code} for older CSV snapshots that predate the column,
     * or rows where the code was blank at generation time.
     */
    private void enrichMissingSubscriptionPlanCodes(List<Map<String, Object>> invoices) {
        if (invoices == null || invoices.isEmpty()) {
            return;
        }
        Set<UUID> missingPlanIds = new LinkedHashSet<>();
        for (Map<String, Object> row : invoices) {
            if (row == null) continue;
            Object codeObj = row.get("subscription_plan_code");
            String code = codeObj != null ? codeObj.toString().trim() : "";
            if (!code.isEmpty()) continue;
            UUID planId = parseUuid(row.get("subscription_plan_id"));
            if (planId != null) {
                missingPlanIds.add(planId);
            }
        }
        if (missingPlanIds.isEmpty()) {
            return;
        }
        Map<UUID, String> codesByPlanId =
                duePreviewRepository.findSubscriptionPlanCodesByPlanIds(missingPlanIds);
        if (codesByPlanId.isEmpty()) {
            return;
        }
        for (Map<String, Object> row : invoices) {
            if (row == null) continue;
            Object codeObj = row.get("subscription_plan_code");
            String code = codeObj != null ? codeObj.toString().trim() : "";
            if (!code.isEmpty()) continue;
            UUID planId = parseUuid(row.get("subscription_plan_id"));
            if (planId == null) continue;
            String resolved = codesByPlanId.get(planId);
            if (resolved != null && !resolved.isBlank()) {
                row.put("subscription_plan_code", resolved.trim());
            }
        }
    }

    private static BigDecimal sumNumericColumn(List<Map<String, Object>> rows, String columnKey) {
        if (rows == null || rows.isEmpty()) {
            return BigDecimal.ZERO;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (Map<String, Object> row : rows) {
            sum = sum.add(parseCellToBigDecimal(row != null ? row.get(columnKey) : null));
        }
        return sum;
    }

    private static BigDecimal parseCellToBigDecimal(Object value) {
        if (value == null) {
            return BigDecimal.ZERO;
        }
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    private static UUID parseUuid(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof UUID u) {
            return u;
        }
        try {
            return UUID.fromString(value.toString().trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private void attachDuePreviewLineItems(List<Map<String, Object>> invoices, Map<UUID, String> descriptionsByScheduleId) {
        if (invoices == null || invoices.isEmpty()) {
            return;
        }
        for (Map<String, Object> inv : invoices) {
            UUID scheduleId = parseUuid(inv.get("billing_schedule_id"));
            String description = scheduleId != null ? descriptionsByScheduleId.get(scheduleId) : null;
            if (description == null || description.isBlank()) {
                description = buildFallbackLineDescription(inv);
            }
            BigDecimal unitPrice = pickLineUnitPrice(inv);
            BigDecimal tax = parseCellToBigDecimal(inv.get("tax_amount"));
            BigDecimal discount = parseCellToBigDecimal(inv.get("discount_amount"));
            BigDecimal lineTotal = resolveLineTotal(inv, unitPrice, tax, discount);

            Map<String, Object> line = new LinkedHashMap<>();
            line.put("description", description);
            line.put("quantity", 1);
            line.put("unit_price", unitPrice);
            line.put("tax_amount", tax);
            line.put("discount_amount", discount);
            line.put("line_total", lineTotal);
            inv.put("line_items", List.of(line));
        }
    }

    private static String buildFallbackLineDescription(Map<String, Object> inv) {
        String agreement = inv.get("agreement_name") != null ? inv.get("agreement_name").toString().trim() : "";
        LocalDate cycle = parseLocalDateCell(inv.get("price_cycle_start"));
        if (cycle == null) {
            cycle = parseLocalDateCell(inv.get("payment_due_date"));
        }
        String cyclePart;
        if (cycle != null) {
            cyclePart = cycle.format(LINE_CYCLE_DATE_FORMAT) + " cycle";
        } else if (inv.get("cycle_number") != null && !inv.get("cycle_number").toString().isBlank()) {
            cyclePart = "cycle " + inv.get("cycle_number").toString().trim();
        } else {
            cyclePart = "billing cycle";
        }
        String emDash = "\u2014";
        if (!agreement.isEmpty()) {
            return agreement + " " + emDash + " " + cyclePart;
        }
        return "Subscription " + emDash + " " + cyclePart;
    }

    private static LocalDate parseLocalDateCell(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate d) {
            return d;
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return null;
        }
        try {
            return LocalDate.parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static BigDecimal pickLineUnitPrice(Map<String, Object> inv) {
        BigDecimal eff = parseCellToBigDecimal(inv.get("effective_unit_price"));
        if (eff.signum() != 0) {
            return eff;
        }
        BigDecimal up = parseCellToBigDecimal(inv.get("unit_price"));
        if (up.signum() != 0) {
            return up;
        }
        return parseCellToBigDecimal(inv.get("sub_total"));
    }

    private static BigDecimal resolveLineTotal(Map<String, Object> inv, BigDecimal unitPrice, BigDecimal tax, BigDecimal discount) {
        BigDecimal baseTotal = parseCellToBigDecimal(inv.get("base_total_amount"));
        if (baseTotal.signum() != 0) {
            return baseTotal;
        }
        BigDecimal total = parseCellToBigDecimal(inv.get("total_amount"));
        BigDecimal adjustment = parseCellToBigDecimal(inv.get("adjustment_amount"));
        if (total.signum() != 0 || adjustment.signum() != 0) {
            return total.subtract(adjustment);
        }
        return unitPrice.add(tax).subtract(discount);
    }

    /**
     * Parse a single CSV line into values (handles quoted fields).
     */
    private String[] parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                values.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        values.add(current.toString());
        return values.toArray(String[]::new);
    }

    /**
     * Enqueue async due-preview generation (enterprise path for 30k+ rows).
     * HTTP returns immediately with {@code status=QUEUED} and {@code pollUrl}; worker builds CSV + S3.
     */
    @Transactional
    public Map<String, Object> generateDuePreview(DuePreviewRequest request) {
        log.info("Enqueue due preview: billRunId={}, dueDate={}, requestLocationId={}, createdBy={}",
                request.billRunId(), request.dueDate(), request.locationId(), request.createdBy());

        UUID billingRunId = request.billRunId();
        var existingRun = billingRunRepository.findById(billingRunId);
        if (existingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        if (request.dueDate() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "dueDate is required");
        }

        UUID createdByUuid = null;
        try {
            createdByUuid = UUID.fromString(request.createdBy());
        } catch (Exception e) {
            // leave null if not a valid UUID
        }

        // Create PENDING stage without marking RUNNING yet (worker calls startStageRun)
        UUID stageRunId = stageRunRepository.createStageRun(
                billingRunId,
                "DUE_PREVIEW",
                OffsetDateTime.now(),
                null,
                createdByUuid,
                false);

        // Deny → re-run: reopen approval so the new preview can be approved/denied again.
        try {
            approvalRepository.resetRejectedApprovalsToPending(billingRunId);
        } catch (Exception e) {
            log.warn("Could not reset rejected approvals after due-preview re-run (non-blocking): {}", e.getMessage());
        }

        Map<String, Object> seedSummary = new LinkedHashMap<>();
        seedSummary.put("due_date", request.dueDate().toString());
        if (request.locationId() != null) {
            seedSummary.put("location_id", request.locationId().toString());
        }
        seedSummary.put("created_by", request.createdBy());
        seedSummary.put("billing_run_id", billingRunId.toString());
        seedSummary.put("queued_at", OffsetDateTime.now(ZoneOffset.UTC).toString());
        seedSummary.put("phase", "QUEUED");
        seedSummary.put("progress_percent", 0);
        stageRunRepository.updateStageRunSummary(stageRunId, seedSummary);

        boolean queuedStatusApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, "QUEUED");
        log.info(
                "Due preview enqueued: stageRunId={} billingRunId={} queuedStatusInDb={}",
                stageRunId,
                billingRunId,
                queuedStatusApplied);

        applicationEventPublisher.publishEvent(DuePreviewQueuedEvent.of(stageRunId));
        auditLogRepository.insertAuditLog(
                "DUE_PREVIEW",
                "STAGE_RUN",
                stageRunId,
                "ENQUEUED",
                request.createdBy() != null ? request.createdBy() : "system",
                seedSummary);

        StageRunDto stage = stageRunRepository.findById(stageRunId);
        String statusCode = stage != null && stage.statusCode() != null
                ? stage.statusCode()
                : (queuedStatusApplied ? "QUEUED" : "PENDING");
        String pollUrl = "/api/billing/due-preview/runs/" + stageRunId;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("billing_run_id", billingRunId);
        response.put("billing_run_code", existingRun.billingRunCode());
        response.put("stage_run_id", stageRunId);
        response.put("due_date", request.dueDate().toString());
        response.put("location_id", request.locationId());
        response.put("created_by", request.createdBy());
        response.put("status", statusCode);
        response.put("statusCode", statusCode);
        response.put("pollUrl", pollUrl);
        response.put("async", true);
        response.put("phase", "QUEUED");
        response.put("progress_percent", 0);
        // Counts/S3 filled when job completes — poll GET for progress
        response.put("s3_path", null);
        response.put("file_name", null);
        response.put("total_instances", null);
        response.put("eligible_count", null);
        response.put("not_eligible_count", null);
        response.put("total_amount", null);
        response.put("eligible_total_amount", null);
        response.put("generated_at", null);
        return response;
    }

    private static boolean isDuePreviewInFlight(String status) {
        return "QUEUED".equals(status)
                || "RUNNING".equals(status)
                || "PENDING".equals(status)
                || "SCHEDULED".equals(status);
    }

    private static Map<String, Object> buildInFlightDuePreviewDetails(
            StageRunDto stageRun, Map<String, Object> summaryJson) {
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("run_id", stageRun.stageRunId());
        run.put("run_code", stageRun.stageRunCode());
        run.put("generated_at", stageRun.startedOn());
        run.put("status", stageRun.statusCode());
        run.put("status_display_name", stageRun.statusDisplayName());
        run.put("filename", summaryJson.get("file_name"));
        run.put("invoices", summaryJson.getOrDefault("total_instances", 0));
        run.put("totalAmount", Boolean.TRUE.equals(summaryJson.get("mixed_currency"))
                ? null
                : summaryJson.getOrDefault("total_amount", BigDecimal.ZERO));
        run.put("totalTax", BigDecimal.ZERO);
        run.put("totalDiscount", BigDecimal.ZERO);
        run.put("summary_json", summaryJson);
        run.put("phase", summaryJson.get("phase"));
        run.put("progress_percent", summaryJson.get("progress_percent"));
        run.put("processed", summaryJson.get("processed"));
        if (summaryJson.get("error") != null) {
            run.put("error", summaryJson.get("error"));
        }
        // Creator filled by instance helper below — static builder keeps summary fields only;
        // caller should invoke putStageRunCreator after if needed.
        Object createdBy = summaryJson.get("created_by");
        if (createdBy != null) {
            run.put("created_by", createdBy);
            run.put("createdBy", createdBy);
        }
        Object createdByName = summaryJson.get("created_by_name");
        if (createdByName != null) {
            run.put("created_by_name", createdByName);
            run.put("createdByName", createdByName);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("run", run);
        response.put("invoices", List.of());
        response.put("status", stageRun.statusCode());
        response.put("async", true);
        response.put("in_progress", isDuePreviewInFlight(stageRun.statusCode()));
        return response;
    }

    /** Lightweight COMPLETED/FAILED payload for pollers — no CSV parse. */
    private static Map<String, Object> buildSummaryOnlyDuePreviewDetails(
            StageRunDto stageRun, Map<String, Object> summaryJson, String s3Path) {
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("run_id", stageRun.stageRunId());
        run.put("run_code", stageRun.stageRunCode());
        run.put("generated_at", summaryJson.getOrDefault("generated_at", stageRun.endedOn()));
        run.put("status", stageRun.statusCode());
        run.put("status_display_name", stageRun.statusDisplayName());
        run.put("filename", summaryJson.get("file_name"));
        run.put("s3_path", s3Path);
        run.put("invoices", summaryJson.getOrDefault("total_instances", 0));
        run.put("eligible_count", summaryJson.get("eligible_count"));
        run.put("not_eligible_count", summaryJson.get("not_eligible_count"));
        run.put("totalAmount", Boolean.TRUE.equals(summaryJson.get("mixed_currency"))
                ? null
                : summaryJson.getOrDefault("total_amount", BigDecimal.ZERO));
        run.put("eligible_total_amount", Boolean.TRUE.equals(summaryJson.get("mixed_currency"))
                ? null
                : summaryJson.get("eligible_total_amount"));
        run.put("summary_json", summaryJson);
        run.put("phase", summaryJson.getOrDefault("phase", "COMPLETED"));
        run.put("progress_percent", summaryJson.getOrDefault("progress_percent", 100));
        Object createdBy = summaryJson.get("created_by");
        if (createdBy != null) {
            run.put("created_by", createdBy);
            run.put("createdBy", createdBy);
        }
        Object createdByName = summaryJson.get("created_by_name");
        if (createdByName != null) {
            run.put("created_by_name", createdByName);
            run.put("createdByName", createdByName);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("run", run);
        response.put("invoices", List.of());
        response.put("status", stageRun.statusCode());
        response.put("async", true);
        response.put("in_progress", false);
        response.put("include_invoices", false);
        return response;
    }

    private void putStageRunCreator(
            Map<String, Object> run, UUID stageRunId, Map<String, Object> summaryJson) {
        Map<String, Object> creator = duePreviewRepository.findStageRunCreator(stageRunId);
        Object createdBy = creator.get("created_by");
        Object createdByName = creator.get("created_by_name");
        if (createdBy == null && summaryJson != null) {
            createdBy = summaryJson.get("created_by");
        }
        if (createdByName == null && summaryJson != null) {
            createdByName = summaryJson.get("created_by_name");
        }
        if (createdBy != null) {
            run.put("created_by", createdBy);
            run.put("createdBy", createdBy);
        }
        if (createdByName != null) {
            run.put("created_by_name", createdByName);
            run.put("createdByName", createdByName);
        }
    }

    @SuppressWarnings("unchecked")
    private void enrichRunCreator(
            Map<String, Object> response, UUID stageRunId, Map<String, Object> summaryJson) {
        Object runObj = response.get("run");
        if (runObj instanceof Map<?, ?> runMap) {
            putStageRunCreator((Map<String, Object>) runMap, stageRunId, summaryJson);
        }
    }

    /**
     * @deprecated Sync path removed — kept private helpers unused; CSV build lives in {@link io.clubone.billing.service.duepreview.DuePreviewJobRunner}.
     */
    @SuppressWarnings("unused")
    private String generateCSV(List<Map<String, Object>> instances) {
        StringBuilder csv = new StringBuilder();

        // CSV Header
        csv.append("billing_schedule_id,subscription_instance_id,subscription_plan_id,subscription_plan_code,subscription_id,cycle_number,")
                .append("payment_due_date,start_date,last_billed_on,")
                .append("client_role_id,role_id,client_first_name,client_last_name,client_email,")
                .append("client_agreement_id,client_agreement_status,agreement_name,")
                .append("location_name,currency_code,")
                .append("client_payment_method_id,payment_method_name,payment_type_name,card_last4,")
                .append("contract_start_date,contract_end_date,")
                .append("unit_price,effective_unit_price,price_cycle_start,price_cycle_end,")
                .append("sub_total,tax_amount,discount_amount,base_total_amount,adjustment_amount,total_amount,")
                .append("subscription_instance_status_name,eligible,eligibility_reason\n");

        // CSV Rows
        for (Map<String, Object> instance : instances) {
            csv.append(formatCSVValue(instance.get("billing_schedule_id"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_instance_id"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_plan_id"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_plan_code"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_id"))).append(",");
            csv.append(formatCSVValue(instance.get("cycle_number"))).append(",");
            csv.append(formatCSVValue(instance.get("payment_due_date"))).append(",");
            csv.append(formatCSVValue(instance.get("start_date"))).append(",");
            csv.append(formatCSVValue(instance.get("last_billed_on"))).append(",");
            csv.append(formatCSVValue(instance.get("client_role_id"))).append(",");
            csv.append(formatCSVValue(instance.get("role_id"))).append(",");
            csv.append(formatCSVValue(instance.get("client_first_name"))).append(",");
            csv.append(formatCSVValue(instance.get("client_last_name"))).append(",");
            csv.append(formatCSVValue(instance.get("client_email"))).append(",");
            csv.append(formatCSVValue(instance.get("client_agreement_id"))).append(",");
            csv.append(formatCSVValue(instance.get("client_agreement_status"))).append(",");
            csv.append(formatCSVValue(instance.get("agreement_name"))).append(",");
            csv.append(formatCSVValue(instance.get("location_name"))).append(",");
            csv.append(formatCSVValue(instance.get("currency_code"))).append(",");
            csv.append(formatCSVValue(instance.get("client_payment_method_id"))).append(",");
            csv.append(formatCSVValue(instance.get("payment_method_name"))).append(",");
            csv.append(formatCSVValue(instance.get("payment_type_name"))).append(",");
            csv.append(formatCSVValue(instance.get("card_last4"))).append(",");
            csv.append(formatCSVValue(instance.get("contract_start_date"))).append(",");
            csv.append(formatCSVValue(instance.get("contract_end_date"))).append(",");
            csv.append(formatCSVValue(instance.get("unit_price"))).append(",");
            csv.append(formatCSVValue(instance.get("effective_unit_price"))).append(",");
            csv.append(formatCSVValue(instance.get("price_cycle_start"))).append(",");
            csv.append(formatCSVValue(instance.get("price_cycle_end"))).append(",");
            csv.append(formatCSVValue(instance.get("sub_total"))).append(",");
            csv.append(formatCSVValue(instance.get("tax_amount"))).append(",");
            csv.append(formatCSVValue(instance.get("discount_amount"))).append(",");
            csv.append(formatCSVValue(instance.get("base_total_amount"))).append(",");
            csv.append(formatCSVValue(instance.get("adjustment_amount"))).append(",");
            csv.append(formatCSVValue(instance.get("total_amount"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_instance_status_name"))).append(",");
            csv.append(formatCSVValue(instance.get("eligible"))).append(",");
            csv.append(formatCSVValue(instance.get("eligibility_reason"))).append("\n");
        }

        return csv.toString();
    }

    /**
     * Format a value for CSV (handle nulls and escape commas/quotes).
     */
    private String formatCSVValue(Object value) {
        if (value == null) {
            return "";
        }
        String str = value.toString();
        // Escape quotes and wrap in quotes if contains comma or quote
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            str = str.replace("\"", "\"\"");
            return "\"" + str + "\"";
        }
        return str;
    }
}
