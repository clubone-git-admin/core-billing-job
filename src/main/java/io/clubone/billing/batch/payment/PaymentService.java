package io.clubone.billing.batch.payment;

import java.util.UUID;

public interface PaymentService {
	PaymentResult billInvoiceRecurring(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId, long amountMinor,
			String currencyCode);
}
