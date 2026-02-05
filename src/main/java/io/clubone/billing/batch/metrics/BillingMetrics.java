package io.clubone.billing.batch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Metrics collection for billing job operations.
 * Provides Prometheus-compatible metrics for monitoring and alerting.
 */
@Component
public class BillingMetrics {

    private static final Logger log = LoggerFactory.getLogger(BillingMetrics.class);

    private final Counter invoicesProcessed;
    private final Counter invoicesFailed;
    private final Counter invoicesSkipped;
    private final Counter paymentsSuccessful;
    private final Counter paymentsFailed;
    private final Counter paymentsPending;
    private final Timer jobExecutionTime;
    private final Timer invoiceProcessingTime;
    private final Timer paymentCallTime;
    private final Gauge dlqSize;
    private final Gauge activeJobCount;
    private final MeterRegistry registry;

    public BillingMetrics(MeterRegistry registry, JdbcTemplate jdbc) {
        this.registry = registry;
        // Invoice processing metrics
        this.invoicesProcessed = Counter.builder("billing.invoices.processed")
            .description("Total invoices processed")
            .tag("type", "processed")
            .register(registry);

        this.invoicesFailed = Counter.builder("billing.invoices.failed")
            .description("Total invoices failed")
            .tag("type", "failed")
            .register(registry);

        this.invoicesSkipped = Counter.builder("billing.invoices.skipped")
            .description("Total invoices skipped (not eligible)")
            .tag("type", "skipped")
            .register(registry);

        // Payment metrics
        this.paymentsSuccessful = Counter.builder("billing.payments.successful")
            .description("Successful payment attempts")
            .tag("type", "successful")
            .register(registry);

        this.paymentsFailed = Counter.builder("billing.payments.failed")
            .description("Failed payment attempts")
            .tag("type", "failed")
            .register(registry);

        this.paymentsPending = Counter.builder("billing.payments.pending")
            .description("Pending payment attempts")
            .tag("type", "pending")
            .register(registry);

        // Timing metrics
        this.jobExecutionTime = Timer.builder("billing.job.execution.time")
            .description("Total job execution time")
            .register(registry);

        this.invoiceProcessingTime = Timer.builder("billing.invoice.processing.time")
            .description("Time to process a single invoice")
            .register(registry);

        this.paymentCallTime = Timer.builder("billing.payment.call.time")
            .description("Time for payment service call")
            .register(registry);

        // Gauge metrics
        this.dlqSize = Gauge.builder("billing.dlq.size", () -> getDlqSize(jdbc))
            .description("Current size of dead letter queue")
            .register(registry);

        this.activeJobCount = Gauge.builder("billing.jobs.active", () -> getActiveJobCount(jdbc))
            .description("Number of active billing jobs")
            .register(registry);
    }

    public void recordInvoiceProcessed(String status) {
        Counter.builder("billing.invoices.processed")
            .tag("status", status != null ? status : "unknown")
            .register(registry)
            .increment();
        invoicesProcessed.increment();
    }

    public void recordInvoiceFailed(String reason) {
        Counter.builder("billing.invoices.failed")
            .tag("reason", reason != null ? reason : "unknown")
            .register(registry)
            .increment();
        invoicesFailed.increment();
    }

    public void recordInvoiceSkipped(String reason) {
        Counter.builder("billing.invoices.skipped")
            .tag("reason", reason != null ? reason : "unknown")
            .register(registry)
            .increment();
        invoicesSkipped.increment();
    }

    public void recordPaymentSuccess() {
        paymentsSuccessful.increment();
    }

    public void recordPaymentFailure(String reason) {
        Counter.builder("billing.payments.failed")
            .tag("reason", reason != null ? reason : "unknown")
            .register(registry)
            .increment();
        paymentsFailed.increment();
    }

    public void recordPaymentPending() {
        paymentsPending.increment();
    }

    public Timer.Sample startJobTimer() {
        return Timer.start();
    }

    public Timer.Sample startInvoiceProcessingTimer() {
        return Timer.start();
    }

    public Timer.Sample startPaymentCallTimer() {
        return Timer.start();
    }

    public void recordJobExecutionTime(Timer.Sample sample) {
        sample.stop(jobExecutionTime);
    }

    public void recordInvoiceProcessingTime(Timer.Sample sample) {
        sample.stop(invoiceProcessingTime);
    }

    public void recordPaymentCallTime(Timer.Sample sample) {
        sample.stop(paymentCallTime);
    }

    private int getDlqSize(JdbcTemplate jdbc) {
        try {
            Integer count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM client_subscription_billing.billing_dead_letter_queue WHERE resolved = false",
                Integer.class
            );
            return count != null ? count : 0;
        } catch (Exception e) {
            log.debug("Failed to get DLQ size", e);
            return 0;
        }
    }

    private int getActiveJobCount(JdbcTemplate jdbc) {
        try {
            Integer count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM client_subscription_billing.billing_run WHERE status = ?",
                Integer.class,
                io.clubone.billing.batch.model.BillingRunStatus.RUNNING.getCode()
            );
            return count != null ? count : 0;
        } catch (Exception e) {
            log.debug("Failed to get active job count", e);
            return 0;
        }
    }
}
