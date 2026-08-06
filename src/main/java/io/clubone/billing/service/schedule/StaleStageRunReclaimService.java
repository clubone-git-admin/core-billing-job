package io.clubone.billing.service.schedule;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.util.UtcClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Reclaims abandoned IG/mock-charge stage runs after a worker crash or lost queued event.
 */
@Service
public class StaleStageRunReclaimService {

    private static final Logger log = LoggerFactory.getLogger(StaleStageRunReclaimService.class);

    private final ScheduledStageClaimService claimService;
    private final ScheduledStageDispatchService dispatchService;
    private final int batchSize;
    private final int staleMinutes;

    public StaleStageRunReclaimService(
            ScheduledStageClaimService claimService,
            ScheduledStageDispatchService dispatchService,
            @Value("${clubone.billing.scheduled-stage.stale-reclaim.batch-size:20}") int batchSize,
            @Value("${clubone.billing.scheduled-stage.stale-reclaim.stale-minutes:15}") int staleMinutes) {
        this.claimService = claimService;
        this.dispatchService = dispatchService;
        this.batchSize = Math.max(1, Math.min(batchSize, 200));
        this.staleMinutes = Math.max(2, staleMinutes);
    }

    public void reclaimStale() {
        OffsetDateTime staleBefore = UtcClock.now().minusMinutes(staleMinutes);
        List<StageRunDto> fromRunning = claimService.claimStaleRunning(staleBefore, batchSize);
        List<StageRunDto> fromQueued = claimService.claimStaleQueued(staleBefore, batchSize);
        if (fromRunning.isEmpty() && fromQueued.isEmpty()) {
            return;
        }
        log.warn(
                "stale-stage reclaim: runningReclaimed={} queuedRedispatched={} staleMinutes={}",
                fromRunning.size(),
                fromQueued.size(),
                staleMinutes);

        for (StageRunDto stage : fromRunning) {
            dispatchOne(stage, "STALE_RUNNING");
        }
        for (StageRunDto stage : fromQueued) {
            dispatchOne(stage, "STALE_QUEUED");
        }
    }

    private void dispatchOne(StageRunDto stage, String reason) {
        try {
            dispatchService.dispatchClaimed(stage, reason);
        } catch (Exception ex) {
            log.error("stale-stage reclaim dispatch failed stageRunId={} stage={} reason={}",
                    stage.stageRunId(), stage.stageCode(), reason, ex);
        }
    }
}
