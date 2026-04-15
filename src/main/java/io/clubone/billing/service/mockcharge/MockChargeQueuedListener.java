package io.clubone.billing.service.mockcharge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class MockChargeQueuedListener {

    private static final Logger log = LoggerFactory.getLogger(MockChargeQueuedListener.class);
    private static final String MDC_STAGE_RUN = "mockChargeStageRunId";

    private final MockChargeJobRunner jobRunner;

    public MockChargeQueuedListener(MockChargeJobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onMockChargeQueued(MockChargeQueuedEvent event) {
        UUID stageRunId = event.stageRunId();
        MDC.put(MDC_STAGE_RUN, stageRunId.toString());
        String thread = Thread.currentThread().getName();
        long t0 = System.nanoTime();
        try {
            log.info(
                    "mock-charge async [AFTER_COMMIT] MockChargeQueuedEvent received thread={} stageRunId={} — starting jobRunner.process (transaction committed)",
                    thread,
                    stageRunId);
            jobRunner.process(stageRunId);
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            log.info(
                    "mock-charge async [AFTER_COMMIT] jobRunner.process finished OK thread={} stageRunId={} durationMs={}",
                    thread,
                    stageRunId,
                    ms);
        } catch (Exception e) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            log.error(
                    "mock-charge async [AFTER_COMMIT] jobRunner.process failed thread={} stageRunId={} durationMs={}",
                    thread,
                    stageRunId,
                    ms,
                    e);
        } finally {
            MDC.remove(MDC_STAGE_RUN);
        }
    }
}
