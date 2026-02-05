package io.clubone.billing.batch.model;

/**
 * Enumeration of all billing status codes used in the system.
 * These correspond to values in client_subscription_billing.lu_billing_status table.
 */
public enum BillingStatus {
	// Mock mode statuses
	MOCK_EVALUATED("MOCK_EVALUATED"),
	MOCK_SKIPPED_NOT_ELIGIBLE("MOCK_SKIPPED_NOT_ELIGIBLE"),
	MOCK_ERROR("MOCK_ERROR"),

	// Live mode statuses
	LIVE_SUCCESS("LIVE_SUCCESS"),
	LIVE_FAILED("LIVE_FAILED"),
	LIVE_SKIPPED_NOT_ELIGIBLE("LIVE_SKIPPED_NOT_ELIGIBLE"),
	LIVE_ERROR("LIVE_ERROR"),
	LIVE_PAYMENT_FAILED("LIVE_PAYMENT_FAILED"),
	LIVE_FINALIZED("LIVE_FINALIZED"),

	// Async capture flow
	PENDING_CAPTURE("PENDING_CAPTURE");

	private final String code;

	BillingStatus(String code) {
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	@Override
	public String toString() {
		return code;
	}

	/**
	 * Get BillingStatus from status code string.
	 * @param code Status code string
	 * @return BillingStatus enum value, or null if not found
	 */
	public static BillingStatus fromCode(String code) {
		if (code == null) {
			return null;
		}
		for (BillingStatus status : values()) {
			if (status.code.equalsIgnoreCase(code)) {
				return status;
			}
		}
		return null;
	}
}
