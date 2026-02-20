package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.ApprovalRepository;
import io.clubone.billing.repo.BillingRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Service for approval workflow operations.
 */
@Service
public class ApprovalService {

    private static final Logger log = LoggerFactory.getLogger(ApprovalService.class);

    private final ApprovalRepository approvalRepository;
    private final BillingRunRepository billingRunRepository;

    public ApprovalService(
            ApprovalRepository approvalRepository,
            BillingRunRepository billingRunRepository) {
        this.approvalRepository = approvalRepository;
        this.billingRunRepository = billingRunRepository;
    }

    public List<ApprovalDto> listApprovals(UUID billingRunId) {
        return approvalRepository.findByBillingRunId(billingRunId);
    }

    @Transactional
    public BillingRunDto approveBillingRun(UUID billingRunId, ApproveBillingRunRequest request) {
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            return null;
        }

        // Approve at the specified level
        approvalRepository.approve(
                billingRunId, request.approvalLevel(), request.approverId(), request.notes());

        // Update billing run approval status
        approvalRepository.updateBillingRunApprovalStatus(
                billingRunId, "APPROVED", request.approverId(), request.notes());

        return billingRunRepository.findById(billingRunId);
    }

    @Transactional
    public BillingRunDto rejectBillingRun(UUID billingRunId, RejectBillingRunRequest request) {
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            return null;
        }

        // Reject at the specified level
        approvalRepository.reject(
                billingRunId, request.approvalLevel(), request.approverId(), request.notes());

        // Update billing run approval status
        approvalRepository.updateBillingRunApprovalStatus(
                billingRunId, "REJECTED", request.approverId(), request.notes());

        return billingRunRepository.findById(billingRunId);
    }

    public PageResponse<BillingRunDto> listPendingApprovals(
            String approverRole, Integer approvalLevel, Integer limit, Integer offset) {
        // TODO: Implement query for pending approvals
        // This requires a more complex query joining approvals with billing runs
        // For now, return empty result
        return PageResponse.of(List.of(), 0, limit, offset);
    }
}
