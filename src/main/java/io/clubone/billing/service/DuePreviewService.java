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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Service for due preview operations. Uses an existing billing_run (request.billRunId), creates only a billing_stage_run
 * and optionally audit log and snapshot; stores the output in S3.
 */
@Service
public class DuePreviewService {

    private static final Logger log = LoggerFactory.getLogger(DuePreviewService.class);

    private final DuePreviewRepository duePreviewRepository;
    private final BillingRunRepository billingRunRepository;
    private final StageRunRepository stageRunRepository;
    private final S3Service s3Service;
    private final AuditLogRepository auditLogRepository;
    private final SnapshotRepository snapshotRepository;
    private final ApprovalRepository approvalRepository;

    public DuePreviewService(
            DuePreviewRepository duePreviewRepository,
            BillingRunRepository billingRunRepository,
            StageRunRepository stageRunRepository,
            S3Service s3Service,
            AuditLogRepository auditLogRepository,
            SnapshotRepository snapshotRepository,
            ApprovalRepository approvalRepository) {
        this.duePreviewRepository = duePreviewRepository;
        this.billingRunRepository = billingRunRepository;
        this.stageRunRepository = stageRunRepository;
        this.s3Service = s3Service;
        this.auditLogRepository = auditLogRepository;
        this.snapshotRepository = snapshotRepository;
        this.approvalRepository = approvalRepository;
    }

    /**
     * List due preview run history with pagination.
     *
     * @param limit    page size
     * @param offset   offset for pagination
     * @param sortBy   optional sort field (e.g. generated_at, status, run_id)
     * @param sortOrder asc or desc
     * @return paginated list of due preview run history records
     */
    public PageResponse<DuePreviewRunHistoryDto> listDuePreviewRunHistory(
            int limit, int offset, String sortBy, String sortOrder) {
        int total = duePreviewRepository.countDuePreviewRunHistory();
        List<DuePreviewRunHistoryDto> data = duePreviewRepository.findDuePreviewRunHistory(limit, offset, sortBy, sortOrder);
        return PageResponse.of(data, total, limit, offset);
    }

    /**
     * Get due preview run details by stage_run_id. Loads run metadata from DB and invoice rows from S3 CSV.
     *
     * @param stageRunId billing_stage_run.stage_run_id (DUE_PREVIEW stage)
     * @return Map with "run" (run_id, run_code, generated_at, status, filename, invoices, totalAmount, summary_json) and "invoices" (list of row maps from CSV)
     */
    public Map<String, Object> getDuePreviewRunDetails(UUID stageRunId) {
        StageRunDto stageRun = stageRunRepository.findById(stageRunId);
        if (stageRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
        }
        if (!"DUE_PREVIEW".equals(stageRun.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Not a due preview run: " + stageRunId);
        }

        Map<String, Object> summaryJson = stageRun.summaryJson() != null ? new HashMap<>(stageRun.summaryJson()) : new HashMap<>();
        String s3Path = (String) summaryJson.get("s3_path");
        if (s3Path == null || s3Path.isBlank()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Due preview file not found in S3 for run: " + stageRunId);
        }

        String csvContent = s3Service.downloadFromS3(s3Path);
        List<Map<String, Object>> invoices = parseDuePreviewCsv(csvContent);
        ensureInvoiceAttributes(invoices);

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

        Map<String, Object> run = new LinkedHashMap<>();
        run.put("run_id", stageRun.stageRunId());
        run.put("run_code", stageRun.stageRunCode());
        run.put("generated_at", stageRun.endedOn() != null ? stageRun.endedOn() : stageRun.startedOn());
        run.put("status", stageRun.statusCode());
        run.put("filename", summaryJson.get("file_name"));
        run.put("invoices", invoicesCount);
        run.put("totalAmount", totalAmount != null ? totalAmount : BigDecimal.ZERO);
        run.put("summary_json", summaryJson);

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
        String s3Path = (String) summaryJson.get("s3_path");
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
     * - If approved: creates INVOICE_GENERATION stage run (RUNNING) and updates billing_run.current_stage_code_id
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
        // Mark DUE_PREVIEW stage run as COMPLETED
        Map<String, Object> summaryJson = new HashMap<>();
        summaryJson.put("approved", approved);
        summaryJson.put("approver_id", request.approverId().toString());
        summaryJson.put("approver_role", approverRole);
        summaryJson.put("approval_level", approvalLevel);
        summaryJson.put("notes", request.notes());
        if (!approved && request.rejectionReason() != null) {
            summaryJson.put("rejection_reason", request.rejectionReason());
        }
        stageRunRepository.completeStageRun(stageRunId, summaryJson);

        // If approved: transition to INVOICE_GENERATION stage
        if (approved) {
            // Create INVOICE_GENERATION stage run if it doesn't exist
            StageRunDto existingInvoiceGen = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, "INVOICE_GENERATION");
            UUID invoiceGenStageRunId;
            if (existingInvoiceGen == null) {
                invoiceGenStageRunId = stageRunRepository.createStageRun(
                        billingRunId, "INVOICE_GENERATION", OffsetDateTime.now(), null, request.approverId());
            } else {
                invoiceGenStageRunId = existingInvoiceGen.stageRunId();
            }

            // Start INVOICE_GENERATION stage run (sets status to RUNNING)
            stageRunRepository.startStageRun(invoiceGenStageRunId);
            
            // Update billing_run.current_stage_code_id to INVOICE_GENERATION
            billingRunRepository.updateCurrentStage(billingRunId, "INVOICE_GENERATION");
            
            log.info("Transitioned to INVOICE_GENERATION stage: billingRunId={}, stageRunId={}", billingRunId, invoiceGenStageRunId);
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
            "role_id", "client_agreement_status", "agreement_name", "location_name",
            "payment_method_name", "payment_type_name", "card_last4", "subscription_id"
    };

    /**
     * Ensure each invoice map has the standard attributes (role_id, client_agreement_status, agreement_name,
     * location_name, payment_method_name, payment_type_name, card_last4, subscription_id) so the API response
     * is consistent; missing keys get null.
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
     * Generate due preview file and upload to S3. Uses the existing billing_run (request.billRunId); creates only
     * billing_stage_run and optionally billing_audit_log and billing_run_snapshot.
     *
     * @param request The due preview request (must reference an existing billing_run_id)
     * @return Response with billing_run_id, stage_run_id, S3 path and summary
     */
    @Transactional
    public Map<String, Object> generateDuePreview(DuePreviewRequest request) {
        log.info("Generating due preview: billRunId={}, dueDate={}, locationId={}, createdBy={}",
                request.billRunId(), request.dueDate(), request.locationId(), request.createdBy());

        UUID billingRunId = request.billRunId();
        var existingRun = billingRunRepository.findById(billingRunId);
        if (existingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }

        UUID createdByUuid = null;
        try {
            createdByUuid = UUID.fromString(request.createdBy());
        } catch (Exception e) {
            // leave null if not a valid UUID
        }

        // Create and start a stage run for DUE_PREVIEW under the existing billing run
        UUID stageRunId = stageRunRepository.createStageRun(
                billingRunId,
                "DUE_PREVIEW",
                OffsetDateTime.now(),
                null, // idempotencyKey
                createdByUuid);
        stageRunRepository.startStageRun(stageRunId);
        log.info("Created and started DUE_PREVIEW stage run: stageRunId={}", stageRunId);

        // Get due invoices
        List<Map<String, Object>> dueInvoices = duePreviewRepository.getDueInvoicesForPreview(
                request.dueDate(), request.locationId());

        log.info("Found {} due subscription instances for preview", dueInvoices.size());

        // Process subscription instances (check eligibility, similar to BillingItemProcessor)
        List<Map<String, Object>> processedInstances = new ArrayList<>();
        int eligibleCount = 0;
        int notEligibleCount = 0;
        BigDecimal totalAmount = BigDecimal.ZERO;
        BigDecimal eligibleTotalAmount = BigDecimal.ZERO;

        for (Map<String, Object> instance : dueInvoices) {
            UUID subscriptionInstanceId = (UUID) instance.get("subscription_instance_id");
            BigDecimal total = (BigDecimal) instance.getOrDefault("total_amount", BigDecimal.ZERO);

            totalAmount = totalAmount.add(total != null ? total : BigDecimal.ZERO);

            // Check eligibility
            boolean eligible = duePreviewRepository.isEligible(subscriptionInstanceId, request.dueDate());

            Map<String, Object> processedInstance = new HashMap<>(instance);
            processedInstance.put("eligible", eligible);
            processedInstance.put("eligibility_reason", eligible ? "ELIGIBLE" : "NOT_ELIGIBLE");

            if (eligible) {
                eligibleCount++;
                eligibleTotalAmount = eligibleTotalAmount.add(total != null ? total : BigDecimal.ZERO);
            } else {
                notEligibleCount++;
            }

            processedInstances.add(processedInstance);
        }

        // Generate CSV content
        String csvContent = generateCSV(processedInstances);

        // Generate file name (use billing run id for traceability) with current date/time
        LocalDateTime now = LocalDateTime.now();
        String dateTimeStr = now.format(DateTimeFormatter.ofPattern("dd-MM_HHmm"));
        String fileName = String.format("due-preview-%s-%s-%s.csv",
                billingRunId.toString().substring(0, 8),
                request.dueDate().toString().replace("-", ""),
                dateTimeStr);

        // Upload to S3
        String s3Path = s3Service.uploadToS3(csvContent, fileName, "text/csv");

        log.info("Due preview generated and uploaded: s3Path={}, totalInstances={}, eligibleCount={}, notEligibleCount={}",
                s3Path, processedInstances.size(), eligibleCount, notEligibleCount);

        // Build summary for stage run
        Map<String, Object> summaryJson = new HashMap<>();
        summaryJson.put("s3_path", s3Path);
        summaryJson.put("file_name", fileName);
        summaryJson.put("total_instances", processedInstances.size());
        summaryJson.put("eligible_count", eligibleCount);
        summaryJson.put("not_eligible_count", notEligibleCount);
        summaryJson.put("total_amount", totalAmount);
        summaryJson.put("eligible_total_amount", eligibleTotalAmount);
        summaryJson.put("generated_at", LocalDate.now().toString());

        // Complete the stage run with summary (no billing_run update)
        stageRunRepository.completeStageRun(stageRunId, summaryJson);
        log.info("Completed DUE_PREVIEW stage run: stageRunId={}", stageRunId);

        // Audit log: due preview generated for this stage run
        auditLogRepository.insertAuditLog(
                "DUE_PREVIEW",
                "STAGE_RUN",
                stageRunId,
                "GENERATED",
                request.createdBy(),
                summaryJson);

        // Snapshot: store due-preview file reference (optional; may fail if DUE_PREVIEW snapshot type not in lu_snapshot_type)
        try {
            snapshotRepository.createSnapshot(
                    billingRunId,
                    "DUE_PREVIEW",
                    "DUE_PREVIEW",
                    summaryJson,
                    s3Path,
                    createdByUuid);
        } catch (Exception e) {
            log.warn("Could not create due-preview snapshot (non-blocking): {}. Ensure lu_snapshot_type has DUE_PREVIEW.", e.getMessage());
        }

        Map<String, Object> response = new HashMap<>();
        response.put("billing_run_id", billingRunId);
        response.put("billing_run_code", existingRun.billingRunCode());
        response.put("stage_run_id", stageRunId);
        response.put("due_date", request.dueDate().toString());
        response.put("location_id", request.locationId());
        response.put("created_by", request.createdBy());
        response.put("s3_path", s3Path);
        response.put("file_name", fileName);
        response.put("total_instances", processedInstances.size());
        response.put("eligible_count", eligibleCount);
        response.put("not_eligible_count", notEligibleCount);
        response.put("total_amount", totalAmount);
        response.put("eligible_total_amount", eligibleTotalAmount);
        response.put("generated_at", LocalDate.now().toString());

        return response;
    }

    /**
     * Generate CSV content from processed subscription instances.
     */
    private String generateCSV(List<Map<String, Object>> instances) {
        StringBuilder csv = new StringBuilder();

        // CSV Header
        csv.append("subscription_instance_id,subscription_plan_id,subscription_id,cycle_number,")
                .append("payment_due_date,start_date,last_billed_on,")
                .append("client_role_id,role_id,client_first_name,client_last_name,client_email,")
                .append("client_agreement_id,client_agreement_status,agreement_name,")
                .append("location_name,")
                .append("client_payment_method_id,payment_method_name,payment_type_name,card_last4,")
                .append("contract_start_date,contract_end_date,")
                .append("unit_price,effective_unit_price,price_cycle_start,price_cycle_end,")
                .append("sub_total,tax_amount,discount_amount,total_amount,")
                .append("subscription_instance_status_name,eligible,eligibility_reason\n");

        // CSV Rows
        for (Map<String, Object> instance : instances) {
            csv.append(formatCSVValue(instance.get("subscription_instance_id"))).append(",");
            csv.append(formatCSVValue(instance.get("subscription_plan_id"))).append(",");
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
