package io.clubone.billing.service.schedule;

import io.clubone.billing.repo.StageRunRepository;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Throttled {@code modified_on} heartbeat so stale-reclaim does not steal a live worker lease.
 */
public final class StageRunLeaseHeartbeat {

    private final StageRunRepository stageRunRepository;
    private final UUID stageRunId;
    private final long intervalNanos;
    private long lastBeatNanos;

    public StageRunLeaseHeartbeat(StageRunRepository stageRunRepository, UUID stageRunId, long intervalSeconds) {
        this.stageRunRepository = stageRunRepository;
        this.stageRunId = stageRunId;
        this.intervalNanos = TimeUnit.SECONDS.toNanos(Math.max(5, intervalSeconds));
        this.lastBeatNanos = System.nanoTime();
    }

    public void maybeTouch() {
        long now = System.nanoTime();
        if (now - lastBeatNanos < intervalNanos) {
            return;
        }
        stageRunRepository.touchStageRun(stageRunId);
        lastBeatNanos = now;
    }
}
