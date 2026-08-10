package io.clubone.billing.batch.payment;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.model.GatewayStatus;
import io.clubone.billing.batch.ratelimit.BillingRateLimiter;
import io.clubone.billing.repo.ActualChargeRepository;
import io.clubone.billing.service.currency.GatewayMidCurrencyService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unchecked exception wrapper for SocketTimeoutException to enable test mode.
 * Spring Retry will catch this via the cause chain.
 */
class TestSocketTimeoutException extends RuntimeException {
	TestSocketTimeoutException(String message) {
		super(message, new SocketTimeoutException(message));
	}
}

@Service
public class HttpPaymentService implements PaymentService {

	private static final Logger log = LoggerFactory.getLogger(HttpPaymentService.class);

	private final BillingJobProperties props;
	private final RestTemplate rt;
	private final BillingRateLimiter rateLimiter;
	private final BillingMetrics metrics;
	private final GatewayMidCurrencyService gatewayMidCurrencyService;
	private final ActualChargeRepository actualChargeRepository;

	// ThreadLocal to track attempt counts per call for test mode
	private final ThreadLocal<Map<String, AtomicInteger>> attemptCounters = ThreadLocal.withInitial(ConcurrentHashMap::new);

	public HttpPaymentService(
			BillingJobProperties props,
			BillingRateLimiter rateLimiter,
			BillingMetrics metrics,
			GatewayMidCurrencyService gatewayMidCurrencyService,
			ActualChargeRepository actualChargeRepository) {
		this.props = props;
		this.rateLimiter = rateLimiter;
		this.metrics = metrics;
		this.gatewayMidCurrencyService = gatewayMidCurrencyService;
		this.actualChargeRepository = actualChargeRepository;
		this.rt = createRestTemplate();
	}

	private RestTemplate createRestTemplate() {
		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
		int timeoutMs = props.getPayment().getHttp().getTimeoutMs();
		factory.setConnectTimeout(timeoutMs);
		factory.setReadTimeout(timeoutMs);
		return new RestTemplate(factory);
	}

	@Override
	@Retry(name = "paymentService")
	@CircuitBreaker(name = "paymentService", fallbackMethod = "paymentFallback")
	public PaymentResult billInvoiceRecurring(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode) {
		log.info("billInvoiceRecurring REQ: invoiceId={} clientRoleId={} clientPaymentMethodId={} amountMinor={} currencyCode={}",
				invoiceId, clientRoleId, clientPaymentMethodId, amountMinor, currencyCode);

		BillingJobProperties.Payment.Http.TestMode testMode = props.getPayment().getHttp().getTestMode();

		if (testMode.isEnabled()) {
			if (attemptCounters.get().isEmpty()) {
				log.info("TEST MODE ENABLED: Will simulate {} failures per HTTP call with exception type: {}",
					testMode.getFailAttempts(),
					testMode.getExceptionType());
			}
		}

		if (!rateLimiter.tryConsumePayment()) {
			log.warn("Payment service rate limit exceeded: invoiceId={}", invoiceId);
			clearAttemptCounters();
			return PaymentResult.fail("RATE_LIMIT_EXCEEDED");
		}

		String gatewayName = actualChargeRepository
				.findGatewayNameForClientPaymentMethod(clientPaymentMethodId)
				.orElse("");
		if (gatewayName.isBlank()) {
			clearAttemptCounters();
			return PaymentResult.fail("GATEWAY_UNKNOWN: no active gateway for clientPaymentMethodId");
		}
		log.info("billInvoiceRecurring resolved gateway={} for clientPaymentMethodId={}", gatewayName, clientPaymentMethodId);

		var timer = metrics.startPaymentCallTimer();
		try {
			if ("ADYEN".equalsIgnoreCase(gatewayName)) {
				PaymentResult result = billInvoiceRecurringAdyen(
						invoiceId, clientRoleId, clientPaymentMethodId, amountMinor, currencyCode);
				metrics.recordPaymentCallTime(timer);
				if (!result.isSuccess() && result.getFailureReason() != null
						&& !GatewayStatus.PENDING_CAPTURE.getCode().equalsIgnoreCase(result.getFailureReason())) {
					metrics.recordPaymentFailure(result.getFailureReason());
				}
				clearAttemptCounters();
				return result;
			}

			if (!"RAZORPAY".equalsIgnoreCase(gatewayName)) {
				metrics.recordPaymentCallTime(timer);
				clearAttemptCounters();
				return PaymentResult.fail("GATEWAY_UNSUPPORTED: " + gatewayName);
			}

			PaymentResult result = billInvoiceRecurringRazorpay(
					invoiceId, clientRoleId, clientPaymentMethodId, amountMinor, currencyCode);
			metrics.recordPaymentCallTime(timer);
			clearAttemptCounters();
			return result;

		} catch (Exception e) {
			log.warn("billInvoiceRecurring exception (will be retried by Resilience4j): invoiceId={} error={}", invoiceId, e.getMessage());
			metrics.recordPaymentCallTime(timer);

			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException("Payment service error: " + e.getMessage(), e);
			}
		}
	}

	private PaymentResult billInvoiceRecurringRazorpay(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode) {
		// 1) validate-method
		String validateUrl = props.getPayment().getHttp().getBaseUrl() + props.getPayment().getHttp().getValidateMethodPath();
		Map<String, Object> validateReq = Map.of(
				"clientRoleId", clientRoleId.toString(),
				"clientPaymentMethodId", clientPaymentMethodId.toString());
		Map<String, Object> validateResp = postJson("validate-method", validateUrl, validateReq);
		log.debug("billInvoiceRecurring validate-method RESP: invoiceId={} response={}", invoiceId, validateResp);

		Object validObj = validateResp.get("valid");
		boolean valid = Boolean.TRUE.equals(validObj) || "true".equalsIgnoreCase(String.valueOf(validObj));
		if (!valid) {
			log.warn("billInvoiceRecurring validate-method failed (valid=false): invoiceId={} response={}", invoiceId, validateResp);
			return PaymentResult.fail("VALIDATE_METHOD_INVALID: valid=false");
		}

		// 2) create intent
		String createIntentUrl = props.getPayment().getHttp().getBaseUrl() + props.getPayment().getHttp().getCreateIntentPath();
		Map<String, Object> createIntentReq = new HashMap<>();
		createIntentReq.put("clientRoleId", clientRoleId.toString());
		createIntentReq.put("invoiceId", invoiceId.toString());
		createIntentReq.put("clientPaymentMethodId", clientPaymentMethodId.toString());
		createIntentReq.put("amountMinor", amountMinor);
		createIntentReq.put("currency", currencyCode);
		createIntentReq.put("paymentTypeCode", props.getPayment().getHttp().getPaymentTypeCode());
		gatewayMidCurrencyService
				.resolveMidForPayment(clientPaymentMethodId, currencyCode, null, clientRoleId)
				.ifPresent(mid -> {
					createIntentReq.put("midCode", mid);
					log.info("billInvoiceRecurring resolved MID for currency {}: midCode={}", currencyCode, mid);
				});
		Map<String, Object> intentResp = postJson("create-intent", createIntentUrl, createIntentReq);
		log.debug("billInvoiceRecurring create-intent RESP: invoiceId={} response={}", invoiceId, intentResp);

		Object intentIdObj = intentResp.get("intentId");
		Object razorpayOrderIdObj = intentResp.get("razorpayOrderId");
		if (intentIdObj == null || razorpayOrderIdObj == null) {
			log.warn("billInvoiceRecurring create-intent failed (missing intentId/razorpayOrderId): invoiceId={} response={}", invoiceId, intentResp);
			return PaymentResult.fail("CREATE_INTENT_FAILED: missing intentId or razorpayOrderId");
		}
		UUID intentId = UUID.fromString(String.valueOf(intentIdObj));
		String razorpayOrderId = String.valueOf(razorpayOrderIdObj);

		// 3) charge-at-will
		String chargeUrl = props.getPayment().getHttp().getBaseUrl() + props.getPayment().getHttp().getChargeAtWillPath();
		Map<String, Object> chargeReq = Map.of(
				"intentId", intentId.toString(),
				"invoiceId", invoiceId.toString(),
				"clientRoleId", clientRoleId.toString(),
				"clientPaymentMethodId", clientPaymentMethodId.toString(),
				"paymentTypeCode", props.getPayment().getHttp().getPaymentTypeCode(),
				"runMode", "LIVE",
				"actorId", props.getPayment().getHttp().getActorId());
		Map<String, Object> chargeResp = postJson("charge-at-will", chargeUrl, chargeReq);
		log.debug("billInvoiceRecurring charge-at-will RESP: invoiceId={} response={}", invoiceId, chargeResp);

		String status = String.valueOf(chargeResp.getOrDefault("status", "UNKNOWN"));
		Object clientPaymentTxnIdObj = chargeResp.get("clientPaymentTransactionId");
		if (clientPaymentTxnIdObj == null) {
			log.warn("billInvoiceRecurring charge-at-will failed (missing clientPaymentTransactionId): invoiceId={} response={}", invoiceId, chargeResp);
			return new PaymentResult(false, null, "CHARGE_AT_WILL_FAILED: missing clientPaymentTransactionId", intentId, null, null);
		}

		UUID clientPaymentTxnId = UUID.fromString(String.valueOf(chargeResp.get("clientPaymentTransactionId")));

		if (GatewayStatus.FAILED.getCode().equalsIgnoreCase(status)) {
			log.warn("billInvoiceRecurring charge-at-will FAILED: invoiceId={} status={} intentId={} txnId={}",
					invoiceId, status, intentId, clientPaymentTxnId);
			metrics.recordPaymentFailure("PAYMENT_FAILED");
			return new PaymentResult(false, null, "PAYMENT_FAILED", intentId, clientPaymentTxnId, null);
		}

		if (GatewayStatus.PENDING_CAPTURE.getCode().equalsIgnoreCase(status)
				|| GatewayStatus.AUTHORIZED.getCode().equalsIgnoreCase(status)
				|| GatewayStatus.CREATED.getCode().equalsIgnoreCase(status)) {
			log.info("billInvoiceRecurring charge-at-will PENDING: invoiceId={} status={} intentId={} txnId={}",
					invoiceId, status, intentId, clientPaymentTxnId);
			return new PaymentResult(
				false,
				GatewayStatus.PENDING_CAPTURE.getCode(),
				GatewayStatus.PENDING_CAPTURE.getCode(),
				intentId,
				clientPaymentTxnId,
				null
			);
		}

		if (!GatewayStatus.CAPTURED.getCode().equalsIgnoreCase(status)) {
			log.warn("billInvoiceRecurring charge-at-will unexpected status: invoiceId={} status={} intentId={} txnId={}",
					invoiceId, status, intentId, clientPaymentTxnId);
			metrics.recordPaymentFailure("UNSUPPORTED_STATUS:" + status);
			return new PaymentResult(false, null, "UNSUPPORTED_STATUS:" + status, intentId, clientPaymentTxnId, null);
		}

		log.info("billInvoiceRecurring RESP (success): invoiceId={} intentId={} clientPaymentTxnId={} razorpayOrderId={}",
				invoiceId, intentId, clientPaymentTxnId, razorpayOrderId);
		return new PaymentResult(true, "RZP_ORDER:" + razorpayOrderId, null, intentId, clientPaymentTxnId, null);
	}

	private PaymentResult billInvoiceRecurringAdyen(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode) {
		String chargeUrl = props.getPayment().getHttp().getBaseUrl()
				+ props.getPayment().getHttp().getAdyenRecurringChargePath();

		Map<String, Object> chargeReq = new HashMap<>();
		chargeReq.put("clientRoleId", clientRoleId.toString());
		chargeReq.put("invoiceId", invoiceId.toString());
		chargeReq.put("clientPaymentMethodId", clientPaymentMethodId.toString());
		chargeReq.put("amountMinor", amountMinor);
		chargeReq.put("currencyCode", currencyCode);
		chargeReq.put("paymentTypeCode", props.getPayment().getHttp().getAdyenPaymentTypeCode());
		chargeReq.put("methodTypeCode", "CARD");
		chargeReq.put("environment", props.getPayment().getHttp().getAdyenEnvironment());

		Map<String, Object> chargeResp = postJson("adyen-recurring-charge", chargeUrl, chargeReq);
		log.debug("billInvoiceRecurring Adyen RESP: invoiceId={} response={}", invoiceId, chargeResp);

		Object intentIdObj = firstNonNull(chargeResp.get("paymentIntentId"), chargeResp.get("intentId"));
		Object txnIdObj = firstNonNull(chargeResp.get("transactionId"), chargeResp.get("clientPaymentTransactionId"));
		String status = String.valueOf(chargeResp.getOrDefault("status", "UNKNOWN"));
		String resultCode = String.valueOf(chargeResp.getOrDefault("resultCode", ""));

		if (intentIdObj == null || txnIdObj == null) {
			log.warn("billInvoiceRecurring Adyen failed (missing paymentIntentId/transactionId): invoiceId={} response={}",
					invoiceId, chargeResp);
			return PaymentResult.fail("ADYEN_CHARGE_FAILED: missing paymentIntentId or transactionId");
		}

		UUID intentId = UUID.fromString(String.valueOf(intentIdObj));
		UUID clientPaymentTxnId = UUID.fromString(String.valueOf(txnIdObj));
		String normalized = status.trim().toUpperCase(Locale.ROOT);
		String resultNorm = resultCode.trim().toUpperCase(Locale.ROOT);

		if ("FAILED".equals(normalized) || "REFUSED".equals(normalized) || "ERROR".equals(normalized)
				|| "CANCELLED".equals(normalized) || "CANCELED".equals(normalized)
				|| "REFUSED".equals(resultNorm) || "ERROR".equals(resultNorm) || "CANCELLED".equals(resultNorm)) {
			log.warn("billInvoiceRecurring Adyen FAILED: invoiceId={} status={} resultCode={} intentId={} txnId={}",
					invoiceId, status, resultCode, intentId, clientPaymentTxnId);
			return new PaymentResult(false, null, "PAYMENT_FAILED:" + status, intentId, clientPaymentTxnId, null);
		}

		if ("PENDING".equals(normalized) || "RECEIVED".equals(normalized)
				|| "PENDING".equals(resultNorm) || "RECEIVED".equals(resultNorm)) {
			log.info("billInvoiceRecurring Adyen PENDING: invoiceId={} status={} resultCode={} intentId={} txnId={}",
					invoiceId, status, resultCode, intentId, clientPaymentTxnId);
			return new PaymentResult(
					false,
					GatewayStatus.PENDING_CAPTURE.getCode(),
					GatewayStatus.PENDING_CAPTURE.getCode(),
					intentId,
					clientPaymentTxnId,
					null);
		}

		// Adyen ContAuth often returns AUTHORISED (auto-capture or capture pending).
		if ("CAPTURED".equals(normalized) || "SETTLED".equals(normalized) || "SUCCESS".equals(normalized)
				|| "PAID".equals(normalized) || "AUTHORISED".equals(normalized) || "AUTHORIZED".equals(normalized)
				|| "AUTHORISED".equals(resultNorm) || "AUTHORIZED".equals(resultNorm)) {
			log.info("billInvoiceRecurring Adyen SUCCESS: invoiceId={} status={} resultCode={} intentId={} txnId={}",
					invoiceId, status, resultCode, intentId, clientPaymentTxnId);
			return new PaymentResult(true, "ADYEN_PSP:" + chargeResp.getOrDefault("pspReference", ""), null, intentId,
					clientPaymentTxnId, null);
		}

		log.warn("billInvoiceRecurring Adyen unexpected status: invoiceId={} status={} resultCode={} intentId={} txnId={}",
				invoiceId, status, resultCode, intentId, clientPaymentTxnId);
		return new PaymentResult(false, null, "UNSUPPORTED_STATUS:" + status, intentId, clientPaymentTxnId, null);
	}

	private static Object firstNonNull(Object a, Object b) {
		return a != null ? a : b;
	}

	private void clearAttemptCounters() {
		attemptCounters.remove();
	}

	/**
	 * Fallback method for circuit breaker.
	 */
	public PaymentResult paymentFallback(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode, Throwable throwable) {
		log.warn("Circuit breaker fallback triggered: invoiceId={} error={}", invoiceId,
			throwable != null ? throwable.getMessage() : "Unknown error");
		clearAttemptCounters();
		return PaymentResult.fail("CIRCUIT_BREAKER_OPEN: " +
			(throwable != null ? throwable.getMessage() : "Service unavailable"));
	}

	private Map<String, Object> postJson(String callName, String url, Map<String, Object> body) {
		BillingJobProperties.Payment.Http.TestMode testMode = props.getPayment().getHttp().getTestMode();

		if (testMode.isEnabled()) {
			Map<String, AtomicInteger> counters = attemptCounters.get();
			int attempt = counters.computeIfAbsent(callName, k -> new AtomicInteger(0)).incrementAndGet();
			int failAttempts = testMode.getFailAttempts();

			if (attempt <= failAttempts) {
				String exceptionType = testMode.getExceptionType();
				log.warn("TEST MODE: Simulating failure for {} (attempt {}/{}) with exception type: {}",
					callName, attempt, failAttempts, exceptionType);

				switch (exceptionType.toUpperCase()) {
					case "SOCKET_TIMEOUT":
						throw new TestSocketTimeoutException("TEST MODE: Simulated socket timeout on attempt " + attempt);
					case "ILLEGAL_STATE":
						throw new IllegalStateException("TEST MODE: Simulated illegal state error on attempt " + attempt);
					case "REST_CLIENT":
						throw new RestClientException("TEST MODE: Simulated REST client error on attempt " + attempt) {};
					default:
						throw new IllegalStateException("TEST MODE: Simulated error on attempt " + attempt);
				}
			} else {
				log.info("TEST MODE: Allowing success for {} after {} failed attempts", callName, failAttempts);
				counters.remove(callName);
			}
		}

		log.debug("HttpPaymentService {} REQ: url={} body={}", callName, url, body);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, headers);

		ResponseEntity<Map<String, Object>> resp = rt.exchange(url, HttpMethod.POST, req,
				new ParameterizedTypeReference<Map<String, Object>>() {});
		int statusCode = resp.getStatusCode().value();
		Map<String, Object> respBody = resp.getBody();

		log.debug("HttpPaymentService {} RESP: url={} statusCode={} body={}", callName, url, statusCode, respBody);

		if (testMode.isEnabled()) {
			attemptCounters.get().remove(callName);
		}

		if (statusCode >= 500 && statusCode < 600) {
			log.warn("HttpPaymentService {} received 5xx error, will retry: url={} statusCode={}", callName, url, statusCode);
			throw new IllegalStateException("Server error " + statusCode + " url=" + url);
		}

		if (!resp.getStatusCode().is2xxSuccessful() || respBody == null) {
			log.error("HttpPaymentService {} failed: url={} statusCode={} body={}", callName, url, statusCode, respBody);
			throw new IllegalStateException("POST failed " + resp.getStatusCode() + " url=" + url);
		}
		return respBody;
	}
}
