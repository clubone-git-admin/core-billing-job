package io.clubone.billing.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Step execution listener to log detailed information about step execution.
 * Helps debug issues with reader returning no records.
 */
@Component
public class BillingStepExecutionListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(BillingStepExecutionListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Starting step: {} with jobExecutionId={}", 
            stepExecution.getStepName(), stepExecution.getJobExecutionId());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("Completed step: {} - readCount={}, writeCount={}, filterCount={}, skipCount={}, commitCount={}, rollbackCount={}", 
            stepExecution.getStepName(),
            stepExecution.getReadCount(),
            stepExecution.getWriteCount(),
            stepExecution.getFilterCount(),
            stepExecution.getSkipCount(),
            stepExecution.getCommitCount(),
            stepExecution.getRollbackCount());
        
        if (stepExecution.getReadCount() == 0 && "processDueInvoicesStep".equals(stepExecution.getStepName())) {
            log.warn("WARNING: Reader returned 0 records. This could mean:");
            log.warn("  1. No invoices match the criteria (is_active=true, schedule_status IN ('PENDING','DUE'), payment_due_date <= asOfDate)");
            log.warn("  2. All invoices are filtered out by idempotency checks");
            log.warn("  3. Invoice status is not 'PENDING' or 'DUE'");
            log.warn("  4. All invoices were already successfully billed in previous LIVE runs");
        }
        
        return stepExecution.getExitStatus();
    }
}
