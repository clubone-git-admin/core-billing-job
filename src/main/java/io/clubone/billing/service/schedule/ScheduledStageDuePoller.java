package io.clubone.billing.service.schedule;

import io.clubone.billing.api.dto.StageRunDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Fires due {@code SCHEDULED} invoice-generation / mock-charge / actual-charge stage runs.
 * Multi-instance safe: {@code FOR UPDATE SKIP LOCKED} claim then enqueue.
 * Worker execution uses a second atomic claim ({@code → RUNNING}) so only one worker runs.
 */
@Component
@ConditionalOnProperty(
        name = "clubone.billing.scheduled-stage.poll.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class ScheduledStageDuePoller {

    private static final Logger log = LoggerFactory.getLogger(ScheduledStageDuePoller.class);

    private final ScheduledStageClaimService claimService;
    private final ScheduledStageDispatchService dispatchService;
    private final int batchSize;

    public ScheduledStageDuePoller(
            ScheduledStageClaimService claimService,
            ScheduledStageDispatchService dispatchService,
            @Value("${clubone.billing.scheduled-stage.poll.batch-size:50}") int batchSize) {
        this.claimService = claimService;
        this.dispatchService = dispatchService;
        this.batchSize = Math.max(1, Math.min(batchSize, 200));
    }

    @Scheduled(fixedDelayString = "${clubone.billing.scheduled-stage.poll.fixed-delay-ms:30000}")
    public void fireDueScheduledStages() {
        List<StageRunDto> claimed;
        try {
            claimed = claimService.claimDue(batchSize);
        } catch (Exception ex) {
            log.error("scheduled-stage due poller claim failed", ex);
            return;
        }
        if (claimed.isEmpty()) {
            return;
        }
        log.info("scheduled-stage due poller: claimed {} stage run(s)", claimed.size());
        for (StageRunDto stage : claimed) {
            try {
                dispatchService.dispatchClaimed(stage);
            } catch (Exception ex) {
                log.error("scheduled-stage due poller dispatch failed stageRunId={} stage={}",
                        stage.stageRunId(), stage.stageCode(), ex);
            }
        }
    }
}
