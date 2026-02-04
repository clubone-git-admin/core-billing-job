package io.clubone.billing.batch.payment;

import java.util.UUID;

public class PaymentResult {
	private final boolean success;
	private final String gatewayRef;
	private final String failureReason;
	private final UUID transactionId;

	// Optional: if you integrate with DB inserts for
	// client_payment_intent/transaction later
	private final UUID clientPaymentIntentId;
	private final UUID clientPaymentTransactionId;

	public PaymentResult(boolean success, String gatewayRef, String failureReason, UUID clientPaymentIntentId,
			UUID clientPaymentTransactionId, UUID transactionId) {
		this.success = success;
		this.gatewayRef = gatewayRef;
		this.failureReason = failureReason;
		this.clientPaymentIntentId = clientPaymentIntentId;
		this.clientPaymentTransactionId = clientPaymentTransactionId;
		this.transactionId = transactionId;
	}

	public static PaymentResult ok(String gatewayRef) {
		return new PaymentResult(true, gatewayRef, null, null, null, null);
	}

	public static PaymentResult fail(String reason) {
		return new PaymentResult(false, null, reason, null, null, null);
	}

	public boolean isSuccess() {
		return success;
	}

	public String getGatewayRef() {
		return gatewayRef;
	}

	public String getFailureReason() {
		return failureReason;
	}

	public UUID getClientPaymentIntentId() {
		return clientPaymentIntentId;
	}

	public UUID getClientPaymentTransactionId() {
		return clientPaymentTransactionId;
	}

	public UUID getTransactionId() {
		return transactionId;
	}

}
