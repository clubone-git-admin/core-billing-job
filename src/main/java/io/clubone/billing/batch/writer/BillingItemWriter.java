package io.clubone.billing.batch.writer;

import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.repo.BillingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

import java.util.UUID;

public class BillingItemWriter implements ItemWriter<BillingWorkItem> {

	private static final Logger log = LoggerFactory.getLogger(BillingItemWriter.class);

	private final JdbcTemplate jdbc;
	private final BillingRepository repo;

	public BillingItemWriter(JdbcTemplate jdbc, BillingRepository repo) {
		this.jdbc = jdbc;
		this.repo = repo;
	}

	@Override
	public void write(@NonNull Chunk<? extends BillingWorkItem> chunk) {
		int count = 0;
		for (BillingWorkItem it : chunk) {
			try {
				writeOne(it);
				count++;
			} catch (Exception e) {
				log.error("Failed to write billing history: invoiceId={} billingRunId={} status={}", it.getInvoiceId(), it.getBillingRunId(), it.getHistoryStatusCode(), e);
				throw new IllegalStateException("Failed to write billing history for invoice " + it.getInvoiceId() + ": " + e.getMessage(), e);
			}
		}
		log.debug("Wrote {} billing history record(s)", count);
	}

	private void writeOne(BillingWorkItem it) {
		UUID statusId = repo.resolveBillingStatusIdByCode(it.getHistoryStatusCode());
		if (statusId == null) {
			throw new IllegalStateException("Missing lu_billing_status.status_code=" + it.getHistoryStatusCode());
		}

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
