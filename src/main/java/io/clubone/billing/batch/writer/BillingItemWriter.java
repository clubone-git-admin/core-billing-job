package io.clubone.billing.batch.writer;

import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import io.clubone.billing.batch.exception.BillingDataException;
import io.clubone.billing.batch.exception.BillingProcessingException;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.util.LoggingUtils;
import io.clubone.billing.repo.BillingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

import java.util.UUID;

public class BillingItemWriter implements ItemWriter<BillingWorkItem> {

	private static final Logger log = LoggerFactory.getLogger(BillingItemWriter.class);

	private final JdbcTemplate jdbc;
	private final BillingRepository repo;
	private final DeadLetterQueueService dlqService;
	private final BillingMetrics metrics;

	public BillingItemWriter(JdbcTemplate jdbc, BillingRepository repo, 
	                        DeadLetterQueueService dlqService, BillingMetrics metrics) {
		this.jdbc = jdbc;
		this.repo = repo;
		this.dlqService = dlqService;
		this.metrics = metrics;
	}

	@Override
	public void write(@NonNull Chunk<? extends BillingWorkItem> chunk) {
		int successCount = 0;
		int failureCount = 0;
		
		for (BillingWorkItem it : chunk) {
			try {
				LoggingUtils.setInvoiceId(it.getInvoiceId());
				LoggingUtils.setBillingRunId(it.getBillingRunId());
				writeOne(it);
				successCount++;
				metrics.recordInvoiceProcessed(it.getHistoryStatusCode());
			} catch (BillingDataException e) {
				// Data validation errors - add to DLQ and let Spring Batch skip
				failureCount++;
				dlqService.addToDLQ(it, e, "BillingDataException");
				metrics.recordInvoiceFailed("DATA_ERROR");
				log.error("Data error writing billing history: invoiceId={} billingRunId={} status={} error={}", 
					it.getInvoiceId(), it.getBillingRunId(), it.getHistoryStatusCode(), e.getMessage(), e);
				// Re-throw to let Spring Batch skip mechanism handle it
				throw e;
			} catch (DataAccessException e) {
				// Database errors - add to DLQ and let Spring Batch skip
				failureCount++;
				dlqService.addToDLQ(it, e, "DataAccessException");
				metrics.recordInvoiceFailed("DATABASE_ERROR");
				log.error("Database error writing billing history: invoiceId={} billingRunId={} status={}", 
					it.getInvoiceId(), it.getBillingRunId(), it.getHistoryStatusCode(), e);
				// Re-throw to let Spring Batch skip mechanism handle it
				throw new BillingProcessingException(
					"Database error writing billing history for invoice " + it.getInvoiceId(),
					it.getInvoiceId(),
					it.getBillingRunId(),
					"write_billing_history",
					e
				);
			} catch (Exception e) {
				// Unexpected errors - add to DLQ and let Spring Batch skip
				failureCount++;
				dlqService.addToDLQ(it, e, e.getClass().getSimpleName());
				metrics.recordInvoiceFailed("UNEXPECTED_ERROR");
				log.error("Unexpected error writing billing history: invoiceId={} billingRunId={} status={}", 
					it.getInvoiceId(), it.getBillingRunId(), it.getHistoryStatusCode(), e);
				// Re-throw to let Spring Batch skip mechanism handle it
				throw new BillingProcessingException(
					"Unexpected error writing billing history for invoice " + it.getInvoiceId(),
					it.getInvoiceId(),
					it.getBillingRunId(),
					"write_billing_history",
					e
				);
			}
		}
		
		log.info("Billing history write completed: success={} failures={} total={}", 
			successCount, failureCount, chunk.size());
	}

	private void writeOne(BillingWorkItem it) {
		UUID statusId = repo.resolveBillingStatusIdByCode(it.getHistoryStatusCode());
		if (statusId == null) {
			throw new BillingDataException(
				"Missing billing status code in lookup table",
				it.getInvoiceId(),
				it.getBillingRunId(),
				"historyStatusCode",
				it.getHistoryStatusCode()
			);
		}

		// Idempotency check: Verify record doesn't already exist for this run+invoice
		// This provides defense in depth in case reader check was bypassed
		try {
			Integer existingCount = jdbc.queryForObject(
				"SELECT COUNT(1) FROM client_subscription_billing.subscription_billing_history " +
				"WHERE billing_run_id = ?::uuid AND invoice_id = ?::uuid",
				Integer.class,
				it.getBillingRunId().toString(),
				it.getInvoiceId().toString()
			);
			
			if (existingCount != null && existingCount > 0) {
				log.warn("Billing history record already exists, skipping insert: invoiceId={} billingRunId={} existingCount={}", 
					it.getInvoiceId(), it.getBillingRunId(), existingCount);
				return; // Idempotent: already processed, skip insert
			}
		} catch (Exception e) {
			// If check fails, log but continue with insert attempt
			// Database constraint will catch duplicate if it exists
			log.debug("Idempotency check failed, proceeding with insert: invoiceId={} billingRunId={}", 
				it.getInvoiceId(), it.getBillingRunId(), e);
		}

		// Insert billing history record
		// If unique constraint exists on (billing_run_id, invoice_id), database will prevent duplicates
		// If constraint doesn't exist, the pre-check above should catch duplicates
		int rows = jdbc.update("""
				  INSERT INTO client_subscription_billing.subscription_billing_history (
			    subscription_instance_id, billing_attempt_on, client_payment_intent_id, client_payment_transaction_id,
			    billing_status_id, failure_reason, created_on, invoice_id,
			    is_mock, billing_run_id, simulated_on,
			    invoice_sub_total, invoice_tax_amount, invoice_discount_amount, invoice_total_amount
			  ) VALUES (
			    ?::uuid, now(), ?::uuid, ?::uuid,
			    ?::uuid, ?, now(), ?::uuid,
			    ?, ?::uuid, now(),
			    ?, ?, ?, ?
			  )
			""",
				it.getSubscriptionInstanceId().toString(),
				it.getClientPaymentIntentId() != null ? it.getClientPaymentIntentId().toString() : null,
				it.getClientPaymentTransactionId() != null ? it.getClientPaymentTransactionId().toString() : null,
				statusId.toString(),
				it.getFailureReason(),
				it.getInvoiceId().toString(),
				it.isMock(),
				it.getBillingRunId().toString(),
				it.getInvoiceSubTotal(),
				it.getInvoiceTaxAmount(),
				it.getInvoiceDiscountAmount(),
				it.getInvoiceTotalAmount()
		);

		if (rows != 1) {
			log.warn("Expected 1 row inserted for invoiceId={}, got {}", it.getInvoiceId(), rows);
		}

		if (!it.isMock() && it.isShouldUpdateSchedule()) {
			int updated = jdbc.update(
					"UPDATE client_subscription_billing.subscription_invoice_schedule "
							+ "SET schedule_status = ?, modified_on = now() WHERE invoice_id = ?::uuid",
					it.getScheduleNewStatus(), it.getInvoiceId().toString());
			log.debug("Updated schedule for invoiceId={} rows={}", it.getInvoiceId(), updated);
		}
	}
}
