package io.clubone.billing.service.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Reclaims stale RUNNING / abandoned QUEUED invoice-generation and mock-charge stage runs.
 */
@Component
@ConditionalOnProperty(
        name = "clubone.billing.scheduled-stage.stale-reclaim.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class StaleStageRunReclaimPoller {

    private static final Logger log = LoggerFactory.getLogger(StaleStageRunReclaimPoller.class);

    private final StaleStageRunReclaimService reclaimService;

    public StaleStageRunReclaimPoller(StaleStageRunReclaimService reclaimService) {
        this.reclaimService = reclaimService;
    }

    @Scheduled(fixedDelayString = "${clubone.billing.scheduled-stage.stale-reclaim.fixed-delay-ms:60000}")
    public void reclaim() {
        try {
            reclaimService.reclaimStale();
        } catch (Exception ex) {
            log.error("stale-stage reclaim poller failed", ex);
        }
    }
}
