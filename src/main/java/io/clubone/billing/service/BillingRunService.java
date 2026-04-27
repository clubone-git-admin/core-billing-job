package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.LocationLevelRepository.LocationRow;
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
    private final DuePreviewService duePreviewService;
    private final BillingRunScopeService billingRunScopeService;

    public BillingRunService(
            BillingRunRepository billingRunRepository,
            StageRunRepository stageRunRepository,
            ApprovalRepository approvalRepository,
            DuePreviewService duePreviewService,
            BillingRunScopeService billingRunScopeService) {
        this.billingRunRepository = billingRunRepository;
        this.stageRunRepository = stageRunRepository;
        this.approvalRepository = approvalRepository;
        this.duePreviewService = duePreviewService;
        this.billingRunScopeService = billingRunScopeService;
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

        // Load stages and approvals (rehydrate DUE_PREVIEW file refs from snapshot when approve overwrote summary_json)
        List<StageRunDto> stageHistory = duePreviewService.enrichDuePreviewStageSummaries(
                billingRunId, stageRunRepository.findByBillingRunId(billingRunId));
        List<ApprovalDto> approvals = approvalRepository.findByBillingRunId(billingRunId);

        return new BillingRunDto(
                billingRun.billingRunId(), billingRun.billingRunCode(), billingRun.dueDate(),
                billingRun.locationId(), billingRun.locationName(),
                billingRun.billingRunStatus(), billingRun.currentStage(), billingRun.approvalStatus(),
                billingRun.startedOn(), billingRun.endedOn(), billingRun.summaryJson(),
                billingRun.createdBy(), billingRun.createdOn(), billingRun.modifiedOn(),
                billingRun.sourceRunId(), billingRun.sourceRunCode(), billingRun.approvedBy(),
                billingRun.approvedOn(), billingRun.approvalNotes(),
                billingRun.locationLevelId(), billingRun.includeChildLocations(),
                billingRun.scopeSummary(),
                stageHistory, approvals,
                billingRun.inclusionLocationNames(),
                billingRun.exclusionLocationNames()
        );
    }

    @Transactional
    public BillingRunDto createBillingRun(CreateBillingRunRequest request) {
        validateCreateBillingRunRequest(request);
        if (request.idempotencyKey() != null) {
            BillingRunDto existing = billingRunRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing != null) {
                throw new IllegalStateException("Billing run already exists with this idempotency key");
            }
        }

        BillingRunScopeService.ScopeResolutionResult scope =
                billingRunScopeService.resolveForCreate(request);
        if (scope.included().isEmpty()) {
            throw new IllegalArgumentException(
                    "No locations left in scope after exclusions and in-flight run checks");
        }
        List<UUID> includeIds =
                scope.included().stream().map(LocationRow::locationId).toList();

        UUID primary =
                request.locationId() != null
                        ? request.locationId()
                        : scope.included().get(0).locationId();

        UUID levelId;
        Boolean includeChild;
        if (request.isUseInclusionScopes()) {
            levelId = null;
            includeChild = false;
        } else {
            levelId = request.locationLevelId();
            includeChild = request.includeChildLocations();
        }

        UUID billingRunId =
                billingRunRepository.createBillingRun(
                        request.dueDate(),
                        primary,
                        levelId,
                        includeChild,
                        request.createdBy(),
                        request.idempotencyKey(),
                        includeIds,
                        scope.scopeSummary());

        return getBillingRun(billingRunId);
    }

    public ScopePreviewResponse scopePreview(ScopePreviewRequest request) {
        validateScopePreviewRequest(request);
        return billingRunScopeService.scopePreview(request);
    }

    private static void validateScopePreviewRequest(ScopePreviewRequest request) {
        if (request.inclusionScopes() != null) {
            if (request.inclusionScopes().isEmpty()) {
                throw new IllegalArgumentException("inclusionScopes must not be empty when provided");
            }
            return;
        }
        if (request.locationLevelId() == null && request.applicationId() == null) {
            throw new IllegalArgumentException("Either inclusionScopes, locationLevelId, or applicationId is required");
        }
    }

    private static void validateCreateBillingRunRequest(CreateBillingRunRequest request) {
        if (request.inclusionScopes() != null) {
            if (request.inclusionScopes().isEmpty()) {
                throw new IllegalArgumentException("inclusionScopes must not be empty when provided");
            }
        }
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
