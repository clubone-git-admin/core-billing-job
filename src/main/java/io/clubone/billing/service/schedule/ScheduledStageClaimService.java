package io.clubone.billing.service.schedule;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.util.UtcClock;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Transactional claim helpers for due schedule + stale reclaim (multi-instance safe).
 */
@Service
public class ScheduledStageClaimService {

    private final StageRunRepository stageRunRepository;

    public ScheduledStageClaimService(StageRunRepository stageRunRepository) {
        this.stageRunRepository = stageRunRepository;
    }

    @Transactional
    public List<StageRunDto> claimDue(int batchSize) {
        return stageRunRepository.claimDueScheduledStageRuns(UtcClock.now(), batchSize);
    }

    @Transactional
    public List<StageRunDto> claimStaleRunning(OffsetDateTime staleBeforeUtc, int batchSize) {
        return stageRunRepository.claimStaleRunningStageRuns(staleBeforeUtc, batchSize);
    }

    @Transactional
    public List<StageRunDto> claimStaleQueued(OffsetDateTime staleBeforeUtc, int batchSize) {
        return stageRunRepository.claimStaleQueuedStageRuns(staleBeforeUtc, batchSize);
    }
}
