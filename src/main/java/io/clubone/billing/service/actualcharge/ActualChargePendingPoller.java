package io.clubone.billing.service.actualcharge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "clubone.billing.actual-charge.pending.poll.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class ActualChargePendingPoller {

    private static final Logger log = LoggerFactory.getLogger(ActualChargePendingPoller.class);

    private final ActualChargePendingReconciliationService reconciliationService;

    public ActualChargePendingPoller(ActualChargePendingReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @Scheduled(fixedDelayString = "${clubone.billing.actual-charge.pending.poll.fixed-delay-ms:60000}")
    public void reconcilePendingCharges() {
        try {
            reconciliationService.reconcilePendingCharges();
        } catch (Exception ex) {
            log.error("actual-charge pending poller failed", ex);
        }
    }
}
