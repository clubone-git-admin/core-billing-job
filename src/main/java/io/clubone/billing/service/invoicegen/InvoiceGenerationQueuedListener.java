package io.clubone.billing.service.invoicegen;

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
 * Runs invoice generation after the HTTP transaction commits so polling sees QUEUED/RUNNING, then WAITING when done.
 * Worker threads do not inherit request {@link TenantContext}; this listener restores it (from the event
 * or from the stage-run row) so repository calls remain auth-free on the async path.
 */
@Component
public class InvoiceGenerationQueuedListener {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationQueuedListener.class);

    private final InvoiceGenerationJobRunner jobRunner;
    private final StageRunRepository stageRunRepository;

    public InvoiceGenerationQueuedListener(
            InvoiceGenerationJobRunner jobRunner, StageRunRepository stageRunRepository) {
        this.jobRunner = jobRunner;
        this.stageRunRepository = stageRunRepository;
    }

    @Async("billingAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onInvoiceGenerationQueued(InvoiceGenerationQueuedEvent event) {
        UUID stageRunId = event.stageRunId();
        log.info("InvoiceGenerationQueuedEvent received (after DB commit), dispatching async job: stageRunId={} thread={}",
                stageRunId, Thread.currentThread().getName());
        try {
            TenantContext ctx = event.tenantContext();
            if (ctx == null) {
                ctx = stageRunRepository.resolveBackgroundTenant(stageRunId);
            }
            if (ctx == null) {
                log.error("Invoice generation async handler aborted: no tenant for stageRunId={}", stageRunId);
                return;
            }
            TenantContexts.run(ctx, () -> jobRunner.process(stageRunId));
        } catch (Exception e) {
            log.error("Invoice generation async handler failed: stageRunId={}", stageRunId, e);
        }
    }
}
