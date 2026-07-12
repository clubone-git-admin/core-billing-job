package io.clubone.billing.service.actualcharge;

import io.clubone.billing.api.dto.ActualChargePaymentStatusUpdateRequest;
import io.clubone.billing.batch.model.BillingStatus;
import io.clubone.billing.repo.ActualChargeRepository;
import io.clubone.billing.repo.BillingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class ActualChargePendingReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ActualChargePendingReconciliationService.class);
    private static final Set<String> SUCCESS_GATEWAY_STATUSES = Set.of("CAPTURED", "SETTLED", "SUCCESS", "PAID");
    private static final Set<String> FAILED_GATEWAY_STATUSES = Set.of("FAILED", "EXPIRED", "REVERSED", "CANCELLED");

    private final ActualChargeRepository actualChargeRepository;
    private final BillingRepository billingRepository;
    private final int pollBatchSize;
    private final int staleMinutes;

    public ActualChargePendingReconciliationService(
            ActualChargeRepository actualChargeRepository,
            BillingRepository billingRepository,
            @Value("${clubone.billing.actual-charge.pending.poll.batch-size:200}") int pollBatchSize,
            @Value("${clubone.billing.actual-charge.pending.poll.stale-minutes:2}") int staleMinutes) {
        this.actualChargeRepository = actualChargeRepository;
        this.billingRepository = billingRepository;
        this.pollBatchSize = Math.max(1, pollBatchSize);
        this.staleMinutes = Math.max(1, staleMinutes);
    }

    @Transactional
    public Map<String, Object> reconcilePendingCharges() {
        UUID finalizedStatusId = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_FINALIZED.getCode());
        UUID failedStatusId = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_PAYMENT_FAILED.getCode());
        if (finalizedStatusId == null || failedStatusId == null) {
            throw new IllegalStateException("Missing billing_status rows for LIVE_FINALIZED or LIVE_PAYMENT_FAILED");
        }

        List<ActualChargeRepository.PendingChargeRow> pendingRows =
                actualChargeRepository.findPendingLiveRowsForReconciliation(pollBatchSize, staleMinutes);
        int inspected = pendingRows.size();
        int finalized = 0;
        int failed = 0;
        int unchanged = 0;

        for (ActualChargeRepository.PendingChargeRow row : pendingRows) {
            String gatewayStatus = normalize(actualChargeRepository.findGatewayTransactionStatus(
                    row.clientPaymentTransactionId(),
                    row.applicationId()));
            if (gatewayStatus == null || gatewayStatus.isBlank()) {
                unchanged++;
                continue;
            }
            if (SUCCESS_GATEWAY_STATUSES.contains(gatewayStatus)) {
                actualChargeRepository.updateHistoryStatus(
                        row.subscriptionBillingHistoryId(),
                        finalizedStatusId,
                        null,
                        row.applicationId());
                finalized++;
                continue;
            }
            if (FAILED_GATEWAY_STATUSES.contains(gatewayStatus)) {
                actualChargeRepository.updateHistoryStatus(
                        row.subscriptionBillingHistoryId(),
                        failedStatusId,
                        "Gateway status=" + gatewayStatus,
                        row.applicationId());
                failed++;
                continue;
            }
            unchanged++;
        }

        Map<String, Object> summary = new HashMap<>();
        summary.put("inspected", inspected);
        summary.put("finalized", finalized);
        summary.put("failed", failed);
        summary.put("unchanged", unchanged);
        summary.put("batchSize", pollBatchSize);
        summary.put("staleMinutes", staleMinutes);
        log.info("actual-charge pending poller summary={}", summary);
        return summary;
    }

    @Transactional
    public Map<String, Object> applyWebhookStatus(ActualChargePaymentStatusUpdateRequest request) {
        UUID finalizedStatusId = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_FINALIZED.getCode());
        UUID failedStatusId = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_PAYMENT_FAILED.getCode());
        if (finalizedStatusId == null || failedStatusId == null) {
            throw new IllegalStateException("Missing billing_status rows for LIVE_FINALIZED or LIVE_PAYMENT_FAILED");
        }
        String gatewayStatus = normalize(request.gatewayStatus());
        ActualChargeRepository.PendingChargeRow row =
                actualChargeRepository.findLatestPendingByClientPaymentTransactionId(request.clientPaymentTransactionId());
        if (row == null) {
            return Map.of(
                    "updated", false,
                    "reason", "No pending live history row found for clientPaymentTransactionId",
                    "clientPaymentTransactionId", request.clientPaymentTransactionId(),
                    "gatewayStatus", gatewayStatus);
        }

        boolean updated = false;
        if (SUCCESS_GATEWAY_STATUSES.contains(gatewayStatus)) {
            actualChargeRepository.updateHistoryStatus(
                    row.subscriptionBillingHistoryId(),
                    finalizedStatusId,
                    null,
                    row.applicationId());
            updated = true;
        } else if (FAILED_GATEWAY_STATUSES.contains(gatewayStatus)) {
            String reason = request.reason() != null && !request.reason().isBlank()
                    ? request.reason()
                    : "Gateway status=" + gatewayStatus;
            actualChargeRepository.updateHistoryStatus(
                    row.subscriptionBillingHistoryId(),
                    failedStatusId,
                    reason,
                    row.applicationId());
            updated = true;
        }

        return Map.of(
                "updated", updated,
                "subscriptionBillingHistoryId", row.subscriptionBillingHistoryId(),
                "billingRunId", row.billingRunId(),
                "invoiceId", row.invoiceId(),
                "gatewayStatus", gatewayStatus,
                "clientPaymentTransactionId", request.clientPaymentTransactionId());
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
