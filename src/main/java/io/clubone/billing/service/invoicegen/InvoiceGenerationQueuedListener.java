package io.clubone.billing.service.invoicegen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;

/**
 * Runs invoice generation after the HTTP transaction commits so polling sees QUEUED/RUNNING, then WAITING when done.
 */
@Component
public class InvoiceGenerationQueuedListener {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationQueuedListener.class);

    private final InvoiceGenerationJobRunner jobRunner;

    public InvoiceGenerationQueuedListener(InvoiceGenerationJobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    @Async("billingAsyncExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onInvoiceGenerationQueued(InvoiceGenerationQueuedEvent event) {
        UUID stageRunId = event.stageRunId();
        log.info("InvoiceGenerationQueuedEvent received (after DB commit), dispatching async job: stageRunId={} thread={}",
                stageRunId, Thread.currentThread().getName());
        try {
            jobRunner.process(stageRunId);
        } catch (Exception e) {
            log.error("Invoice generation async handler failed: stageRunId={}", stageRunId, e);
        }
    }
}
