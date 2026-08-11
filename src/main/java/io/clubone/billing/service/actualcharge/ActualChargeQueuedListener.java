package io.clubone.billing.service.actualcharge;

import io.clubone.billing.batch.util.LoggingUtils;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.security.TenantContext;
import io.clubone.billing.security.TenantContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Runs actual (live) charge processing after the HTTP transaction commits so polling sees QUEUED/RUNNING first.
 */
@Component
public class ActualChargeQueuedListener {

    private static final Logger log = LoggerFactory.getLogger(ActualChargeQueuedListener.class);
    private static final String MDC_STAGE_RUN = "actualChargeStageRunId";

    private final ActualChargeJobRunner jobRunner;
    private final StageRunRepository stageRunRepository;

    public ActualChargeQueuedListener(ActualChargeJobRunner jobRunner, StageRunRepository stageRunRepository) {
        this.jobRunner = jobRunner;
        this.stageRunRepository = stageRunRepository;
    }

    @Async("actualChargeAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onActualChargeQueued(ActualChargeQueuedEvent event) {
        UUID stageRunId = event.stageRunId();
        UUID billingRunId = event.billingRunId();
        MDC.put(MDC_STAGE_RUN, stageRunId.toString());
        if (billingRunId != null) {
            LoggingUtils.setBillingRunId(billingRunId);
        }
        String thread = Thread.currentThread().getName();
        long t0 = System.nanoTime();
        try {
            log.info(
                    "actual-charge async [AFTER_COMMIT] ActualChargeQueuedEvent received thread={} billingRunId={} stageRunId={} "
                            + "— starting jobRunner.process (DB transaction committed)",
                    thread,
                    billingRunId,
                    stageRunId);
            TenantContext ctx = event.tenantContext();
            if (ctx == null) {
                ctx = TenantContext.get();
            }
            if (ctx == null) {
                ctx = stageRunRepository.resolveBackgroundTenant(stageRunId);
            }
            if (ctx == null) {
                log.error("actual-charge async aborted: no tenant for stageRunId={}", stageRunId);
                return;
            }
            TenantContexts.run(ctx, () -> jobRunner.process(stageRunId));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            log.info(
                    "actual-charge async [AFTER_COMMIT] jobRunner.process finished OK thread={} billingRunId={} stageRunId={} durationMs={}",
                    thread,
                    billingRunId,
                    stageRunId,
                    ms);
        } catch (Exception e) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            log.error(
                    "actual-charge async [AFTER_COMMIT] jobRunner.process failed thread={} billingRunId={} stageRunId={} durationMs={}",
                    thread,
                    billingRunId,
                    stageRunId,
                    ms,
                    e);
        } finally {
            MDC.remove(MDC_STAGE_RUN);
            if (billingRunId != null) {
                MDC.remove("billingRunId");
            }
        }
    }
}
