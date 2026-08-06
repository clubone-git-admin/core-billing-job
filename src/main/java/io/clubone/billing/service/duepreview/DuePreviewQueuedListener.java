package io.clubone.billing.service.duepreview;

import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.security.TenantContext;
import io.clubone.billing.security.TenantContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;

/**
 * Runs due-preview generation after the HTTP transaction commits so polling sees QUEUED → RUNNING → COMPLETED.
 */
@Component
public class DuePreviewQueuedListener {

    private static final Logger log = LoggerFactory.getLogger(DuePreviewQueuedListener.class);

    private final DuePreviewJobRunner jobRunner;
    private final StageRunRepository stageRunRepository;

    public DuePreviewQueuedListener(DuePreviewJobRunner jobRunner, StageRunRepository stageRunRepository) {
        this.jobRunner = jobRunner;
        this.stageRunRepository = stageRunRepository;
    }

    @Async("duePreviewAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onDuePreviewQueued(DuePreviewQueuedEvent event) {
        UUID stageRunId = event.stageRunId();
        log.info(
                "DuePreviewQueuedEvent received (after DB commit), dispatching async job: stageRunId={} thread={}",
                stageRunId,
                Thread.currentThread().getName());
        try {
            TenantContext ctx = event.tenantContext();
            if (ctx == null) {
                ctx = stageRunRepository.resolveBackgroundTenant(stageRunId);
            }
            if (ctx == null) {
                log.error("Due preview async handler aborted: no tenant for stageRunId={}", stageRunId);
                return;
            }
            TenantContexts.run(ctx, () -> jobRunner.process(stageRunId));
        } catch (Exception e) {
            log.error("Due preview async handler failed: stageRunId={}", stageRunId, e);
        }
    }
}
