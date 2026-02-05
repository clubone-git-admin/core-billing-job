package io.clubone.billing.batch;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "clubone.billing")
public class BillingJobProperties {
	private int chunkSize = 300;
	private boolean updateInvoice = false;
	private boolean updateSchedule = true;
	
	// Idempotency settings
	private boolean preventDuplicateAcrossRuns = true; // Prevent processing invoices already billed in previous runs
	private boolean useRowLocking = true; // Use FOR UPDATE SKIP LOCKED for concurrent safety

	private Payment payment = new Payment();
	private Noop noop = new Noop();

	public boolean isUpdateInvoice() {
		return updateInvoice;
	}

	public void setUpdateInvoice(boolean updateInvoice) {
		this.updateInvoice = updateInvoice;
	}

	public boolean isUpdateSchedule() {
		return updateSchedule;
	}

	public void setUpdateSchedule(boolean updateSchedule) {
		this.updateSchedule = updateSchedule;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public boolean isPreventDuplicateAcrossRuns() {
		return preventDuplicateAcrossRuns;
	}

	public void setPreventDuplicateAcrossRuns(boolean preventDuplicateAcrossRuns) {
		this.preventDuplicateAcrossRuns = preventDuplicateAcrossRuns;
	}

	public boolean isUseRowLocking() {
		return useRowLocking;
	}

	public void setUseRowLocking(boolean useRowLocking) {
		this.useRowLocking = useRowLocking;
	}

	public Payment getPayment() {
		return payment;
	}

	public void setPayment(Payment payment) {
		this.payment = payment;
	}

	public Noop getNoop() {
		return noop;
	}

	public void setNoop(Noop noop) {
		this.noop = noop;
	}

	public static class Payment {
		private String strategy = "NOOP"; // NOOP | HTTP
		private Http http = new Http();

		public String getStrategy() {
			return strategy;
		}

		public void setStrategy(String strategy) {
			this.strategy = strategy;
		}

		public Http getHttp() {
			return http;
		}

		public void setHttp(Http http) {
			this.http = http;
		}

		public static class Http {
			private String baseUrl = "http://localhost:8010";
			private String createAndCapturePath = "/api/payments/capture-invoice";
			private int timeoutMs = 8000;
			private String validateMethodPath = "/payment/razorpay/billing/validate-method";
			private String createIntentPath = "/payment/api/payment/razorpay/intents";
			private String chargeAtWillPath = "/payment/razorpay/billing/charge-at-will";

			private String txnBaseUrl = "http://localhost:8012"; // transaction service
			private String finalizePath = "/transaction/api/transactions/v3/finalize";

			private String actorId = "00000000-0000-0000-0000-000000000001"; // system user
			private String paymentTypeCode = "RECURRING";
			
			// Test mode configuration for simulating retry failures
			private TestMode testMode = new TestMode();

			/**
			 * Test mode configuration for simulating retry failures.
			 */
			public static class TestMode {
				private boolean enabled = false;
				private int failAttempts = 2; // Fail first N attempts, then succeed
				private String exceptionType = "SOCKET_TIMEOUT"; // SOCKET_TIMEOUT | ILLEGAL_STATE | REST_CLIENT

				public boolean isEnabled() {
					return enabled;
				}

				public void setEnabled(boolean enabled) {
					this.enabled = enabled;
				}

				public int getFailAttempts() {
					return failAttempts;
				}

				public void setFailAttempts(int failAttempts) {
					this.failAttempts = failAttempts;
				}

				public String getExceptionType() {
					return exceptionType;
				}

				public void setExceptionType(String exceptionType) {
					this.exceptionType = exceptionType;
				}
			}

			public String getValidateMethodPath() {
				return validateMethodPath;
			}

			public void setValidateMethodPath(String validateMethodPath) {
				this.validateMethodPath = validateMethodPath;
			}

			public String getCreateIntentPath() {
				return createIntentPath;
			}

			public void setCreateIntentPath(String createIntentPath) {
				this.createIntentPath = createIntentPath;
			}

			public String getChargeAtWillPath() {
				return chargeAtWillPath;
			}

			public void setChargeAtWillPath(String chargeAtWillPath) {
				this.chargeAtWillPath = chargeAtWillPath;
			}

			public String getTxnBaseUrl() {
				return txnBaseUrl;
			}

			public void setTxnBaseUrl(String txnBaseUrl) {
				this.txnBaseUrl = txnBaseUrl;
			}

			public String getFinalizePath() {
				return finalizePath;
			}

			public void setFinalizePath(String finalizePath) {
				this.finalizePath = finalizePath;
			}

			public String getActorId() {
				return actorId;
			}

			public void setActorId(String actorId) {
				this.actorId = actorId;
			}

			public String getPaymentTypeCode() {
				return paymentTypeCode;
			}

			public void setPaymentTypeCode(String paymentTypeCode) {
				this.paymentTypeCode = paymentTypeCode;
			}

			public String getBaseUrl() {
				return baseUrl;
			}

			public void setBaseUrl(String baseUrl) {
				this.baseUrl = baseUrl;
			}

			public String getCreateAndCapturePath() {
				return createAndCapturePath;
			}

			public void setCreateAndCapturePath(String createAndCapturePath) {
				this.createAndCapturePath = createAndCapturePath;
			}

			public int getTimeoutMs() {
				return timeoutMs;
			}

			public void setTimeoutMs(int timeoutMs) {
				this.timeoutMs = timeoutMs;
			}

			public TestMode getTestMode() {
				return testMode;
			}

			public void setTestMode(TestMode testMode) {
				this.testMode = testMode;
			}
		}
	}

	public static class Noop {
		private String outcome = "SUCCESS"; // SUCCESS | FAIL_RANDOM_10PCT | ALWAYS_FAIL

		public String getOutcome() {
			return outcome;
		}

		public void setOutcome(String outcome) {
			this.outcome = outcome;
		}
	}
}
