package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.repo.ApprovalRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.*;

/**
 * Service for billing run operations.
 */
@Service
public class BillingRunService {

    private static final Logger log = LoggerFactory.getLogger(BillingRunService.class);

    private final BillingRunRepository billingRunRepository;
    private final StageRunRepository stageRunRepository;
    private final ApprovalRepository approvalRepository;

    public BillingRunService(
            BillingRunRepository billingRunRepository,
            StageRunRepository stageRunRepository,
            ApprovalRepository approvalRepository) {
        this.billingRunRepository = billingRunRepository;
        this.stageRunRepository = stageRunRepository;
        this.approvalRepository = approvalRepository;
    }

    public PageResponse<BillingRunDto> listBillingRuns(
            String statusCode, String currentStageCode, LocalDate dueDateFrom,
            LocalDate dueDateTo, UUID locationId, Integer limit, Integer offset,
            String sortBy, String sortOrder) {

        List<BillingRunDto> runs = billingRunRepository.findBillingRuns(
                statusCode, currentStageCode, dueDateFrom, dueDateTo, locationId,
                limit, offset, sortBy, sortOrder);

        Integer total = billingRunRepository.countBillingRuns(
                statusCode, currentStageCode, dueDateFrom, dueDateTo, locationId);

        return PageResponse.of(runs, total, limit, offset);
    }

    public BillingRunDto getBillingRun(UUID billingRunId) {
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            return null;
        }

        // Load stages and approvals
        List<StageRunDto> stages = stageRunRepository.findByBillingRunId(billingRunId);
        List<ApprovalDto> approvals = approvalRepository.findByBillingRunId(billingRunId);

        return new BillingRunDto(
                billingRun.billingRunId(), billingRun.billingRunCode(), billingRun.dueDate(),
                billingRun.locationId(), billingRun.locationName(),
                billingRun.billingRunStatus(), billingRun.currentStage(), billingRun.approvalStatus(),
                billingRun.startedOn(), billingRun.endedOn(), billingRun.summaryJson(),
                billingRun.createdBy(), billingRun.createdOn(), billingRun.modifiedOn(),
                billingRun.sourceRunId(), billingRun.sourceRunCode(), billingRun.approvedBy(),
                billingRun.approvedOn(), billingRun.approvalNotes(), stages, approvals
        );
    }

    @Transactional
    public BillingRunDto createBillingRun(CreateBillingRunRequest request) {
        // Check for duplicate idempotency key
        if (request.idempotencyKey() != null) {
            BillingRunDto existing = billingRunRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing != null) {
                throw new IllegalStateException("Billing run already exists with this idempotency key");
            }
        }

        UUID billingRunId = billingRunRepository.createBillingRun(
                request.dueDate(), request.locationId(),
                request.createdBy(), request.idempotencyKey());

        return getBillingRun(billingRunId);
    }

    @Transactional
    public BillingRunDto updateBillingRun(UUID billingRunId, UpdateBillingRunRequest request) {
        BillingRunDto existing = billingRunRepository.findById(billingRunId);
        if (existing == null) {
            return null;
        }

        billingRunRepository.updateBillingRun(
                billingRunId, request.summaryJson(), request.approvalNotes());

        return getBillingRun(billingRunId);
    }

    @Transactional
    public BillingRunDto cancelBillingRun(UUID billingRunId, CancelBillingRunRequest request) {
        BillingRunDto existing = billingRunRepository.findById(billingRunId);
        if (existing == null) {
            return null;
        }

        billingRunRepository.cancelBillingRun(billingRunId);
        // TODO: Update approval_notes with cancellation reason

        return getBillingRun(billingRunId);
    }

    @Transactional
    public BulkCreateBillingRunResponse bulkCreateBillingRuns(BulkCreateBillingRunRequest request) {
        List<BulkCreateBillingRunResponse.BillingRunSummary> created = new ArrayList<>();
        List<BulkCreateBillingRunResponse.ErrorDetail> errors = new ArrayList<>();

        for (int i = 0; i < request.runs().size(); i++) {
            BulkCreateBillingRunRequest.BillingRunItem item = request.runs().get(i);
            try {
                UUID billingRunId = billingRunRepository.createBillingRun(
                        item.dueDate(), item.locationId(),
                        request.createdBy(), null);

                BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
                created.add(new BulkCreateBillingRunResponse.BillingRunSummary(
                        billingRun.billingRunId(),
                        billingRun.billingRunCode(),
                        billingRun.dueDate(),
                        billingRun.locationId()
                ));
            } catch (Exception e) {
                log.error("Failed to create billing run at index {}: {}", i, e.getMessage(), e);
                errors.add(new BulkCreateBillingRunResponse.ErrorDetail(
                        i,
                        "CREATE_FAILED",
                        e.getMessage()
                ));
            }
        }

        return new BulkCreateBillingRunResponse(
                created.size(),
                errors.size(),
                created,
                errors
        );
    }

    public BillingRunDto getBillingRunByKey(String idempotencyKey) {
        return billingRunRepository.findByIdempotencyKey(idempotencyKey);
    }
}
