package io.clubone.billing.batch.payment;

import io.clubone.billing.batch.BillingJobProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class NoopPaymentService implements PaymentService {

	private static final Logger log = LoggerFactory.getLogger(NoopPaymentService.class);

	private final BillingJobProperties props;

	public NoopPaymentService(BillingJobProperties props) {
		this.props = props;
	}

	@Override
	public PaymentResult billInvoiceRecurring(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
	                                         long amountMinor, String currencyCode) {
		System.out.println("NOOP");
		log.debug("NOOP payment: invoiceId={} amountMinor={}", invoiceId, amountMinor);
		return PaymentResult.ok("NOOP-" + invoiceId);
	}
}
