package io.clubone.billing.batch.payment;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.ratelimit.BillingRateLimiter;
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
	
	// ThreadLocal to track attempt counts per call for test mode
	private final ThreadLocal<Map<String, AtomicInteger>> attemptCounters = ThreadLocal.withInitial(ConcurrentHashMap::new);

	public HttpPaymentService(BillingJobProperties props, BillingRateLimiter rateLimiter, BillingMetrics metrics) {
		this.props = props;
		this.rateLimiter = rateLimiter;
		this.metrics = metrics;
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
		
		// Get test mode configuration (cache to avoid repeated calls and help IDE resolution)
		BillingJobProperties.Payment.Http.TestMode testMode = props.getPayment().getHttp().getTestMode();
		
		// Clear test mode counters at start of each invoice processing
		// Note: Counters persist across Resilience4j Retry attempts, allowing us to test retry behavior
		if (testMode.isEnabled()) {
			// Only clear on first call (check if counters map is empty)
			if (attemptCounters.get().isEmpty()) {
				log.info("TEST MODE ENABLED: Will simulate {} failures per HTTP call with exception type: {}", 
					testMode.getFailAttempts(),
					testMode.getExceptionType());
			}
		}
		
		// Rate limiting
		if (!rateLimiter.tryConsumePayment()) {
			log.warn("Payment service rate limit exceeded: invoiceId={}", invoiceId);
			return PaymentResult.fail("RATE_LIMIT_EXCEEDED");
		}

		var timer = metrics.startPaymentCallTimer();
		try {
			// 1) validate-method
			String validateUrl = props.getPayment().getHttp().getBaseUrl() + props.getPayment().getHttp().getValidateMethodPath();
			Map<String, Object> validateReq = Map.of("clientRoleId", clientRoleId.toString(), "clientPaymentMethodId", clientPaymentMethodId.toString());
			Map<String, Object> validateResp = postJson("validate-method", validateUrl, validateReq);
			log.info("billInvoiceRecurring validate-method RESP: invoiceId={} response={}", invoiceId, validateResp);

			Object validObj = validateResp.get("valid");
			boolean valid = Boolean.TRUE.equals(validObj) || "true".equalsIgnoreCase(String.valueOf(validObj));
			if (!valid) {
				log.warn("billInvoiceRecurring validate-method failed (valid=false): invoiceId={} response={}", invoiceId, validateResp);
				return PaymentResult.fail("VALIDATE_METHOD_INVALID: valid=false");
			}

			// 2) create intent
			String createIntentUrl = props.getPayment().getHttp().getBaseUrl() + props.getPayment().getHttp().getCreateIntentPath();
			Map<String, Object> createIntentReq = Map.of(
					"clientRoleId", clientRoleId.toString(),
					"invoiceId", invoiceId.toString(),
					"clientPaymentMethodId", clientPaymentMethodId.toString(),
					"amountMinor", amountMinor,
					"currency", currencyCode,
					"paymentTypeCode", props.getPayment().getHttp().getPaymentTypeCode());
			Map<String, Object> intentResp = postJson("create-intent", createIntentUrl, createIntentReq);
			log.info("billInvoiceRecurring create-intent RESP: invoiceId={} response={}", invoiceId, intentResp);

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
			log.info("billInvoiceRecurring charge-at-will RESP: invoiceId={} response={}", invoiceId, chargeResp);

			String status = String.valueOf(chargeResp.getOrDefault("status", "UNKNOWN"));
			Object clientPaymentTxnIdObj = chargeResp.get("clientPaymentTransactionId");
			if (clientPaymentTxnIdObj == null) {
				log.warn("billInvoiceRecurring charge-at-will failed (missing clientPaymentTransactionId): invoiceId={} response={}", invoiceId, chargeResp);
				return new PaymentResult(false, null, "CHARGE_AT_WILL_FAILED: missing clientPaymentTransactionId", intentId, null, null);
			}
			
			UUID clientPaymentTxnId = UUID.fromString(String.valueOf(chargeResp.get("clientPaymentTransactionId")));

			// 3A) Failure
			// status returned by charge-at-will is now: CAPTURED | PENDING_CAPTURE | FAILED
			if ("FAILED".equalsIgnoreCase(status)) {
			    log.warn("billInvoiceRecurring charge-at-will FAILED: invoiceId={} status={} intentId={} txnId={}",
			            invoiceId, status, intentId, clientPaymentTxnId);
			    metrics.recordPaymentCallTime(timer);
			    metrics.recordPaymentFailure("PAYMENT_FAILED");
			    return new PaymentResult(false, null, "PAYMENT_FAILED", intentId, clientPaymentTxnId, null);
			}

			if ("PENDING_CAPTURE".equalsIgnoreCase(status) || "AUTHORIZED".equalsIgnoreCase(status) || "CREATED".equalsIgnoreCase(status)) {
			    log.info("billInvoiceRecurring charge-at-will PENDING: invoiceId={} status={} intentId={} txnId={}",
			            invoiceId, status, intentId, clientPaymentTxnId);
			    return new PaymentResult(false, "PENDING_CAPTURE", "PENDING_CAPTURE", intentId, clientPaymentTxnId, null);
			}

			if (!"CAPTURED".equalsIgnoreCase(status)) {
			    log.warn("billInvoiceRecurring charge-at-will unexpected status: invoiceId={} status={} intentId={} txnId={}",
			            invoiceId, status, intentId, clientPaymentTxnId);
			    metrics.recordPaymentCallTime(timer);
			    metrics.recordPaymentFailure("UNSUPPORTED_STATUS:" + status);
			    return new PaymentResult(false, null, "UNSUPPORTED_STATUS:" + status, intentId, clientPaymentTxnId, null);
			}

			log.info("billInvoiceRecurring RESP (success): invoiceId={} intentId={} clientPaymentTxnId={} transactionId={} razorpayOrderId={}",
					invoiceId, intentId, clientPaymentTxnId, null, razorpayOrderId);
			metrics.recordPaymentCallTime(timer);
			return new PaymentResult(true, "RZP_ORDER:" + razorpayOrderId, null, intentId, clientPaymentTxnId, null);

		} catch (Exception e) {
			// Log the error but let it propagate so Resilience4j Retry can handle retries
			// Resilience4j Retry will catch RuntimeException and retry
			// After retries exhausted, exception will propagate to Spring Batch
			log.warn("billInvoiceRecurring exception (will be retried by Resilience4j): invoiceId={} error={}", invoiceId, e.getMessage());
			metrics.recordPaymentCallTime(timer);
			
			// Convert checked exceptions to RuntimeException so Resilience4j Retry can handle them
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException("Payment service error: " + e.getMessage(), e);
			}
		}
	}

	/**
	 * Fallback method for circuit breaker.
	 * Called when circuit breaker is open or when an exception occurs.
	 * 
	 * @param invoiceId Invoice ID
	 * @param clientRoleId Client role ID
	 * @param clientPaymentMethodId Payment method ID
	 * @param amountMinor Amount in minor currency units
	 * @param currencyCode Currency code
	 * @param throwable The exception that triggered the fallback
	 * @return PaymentResult indicating failure
	 */
	public PaymentResult paymentFallback(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode, Throwable throwable) {
		log.warn("Circuit breaker fallback triggered: invoiceId={} error={}", invoiceId, 
			throwable != null ? throwable.getMessage() : "Unknown error");
		return PaymentResult.fail("CIRCUIT_BREAKER_OPEN: " + 
			(throwable != null ? throwable.getMessage() : "Service unavailable"));
	}

	/**
	 * Makes HTTP POST request to payment service.
	 * Note: Retries are handled at the method level by Resilience4j Retry on billInvoiceRecurring().
	 * Spring Retry doesn't work on self-invocations (private methods called from same class).
	 * 
	 * @param callName Name of the call for logging
	 * @param url URL to call
	 * @param body Request body
	 * @return Response body as Map
	 * @throws RuntimeException if request fails (will be retried by Resilience4j Retry)
	 */
	private Map<String, Object> postJson(String callName, String url, Map<String, Object> body) {
		// Get test mode configuration (cache to avoid repeated calls and help IDE resolution)
		BillingJobProperties.Payment.Http.TestMode testMode = props.getPayment().getHttp().getTestMode();
		
		// Test mode: Simulate failures for testing retry mechanism
		if (testMode.isEnabled()) {
			Map<String, AtomicInteger> counters = attemptCounters.get();
			int attempt = counters.computeIfAbsent(callName, k -> new AtomicInteger(0)).incrementAndGet();
			int failAttempts = testMode.getFailAttempts();
			
			if (attempt <= failAttempts) {
				String exceptionType = testMode.getExceptionType();
				log.warn("TEST MODE: Simulating failure for {} (attempt {}/{}) with exception type: {}", 
					callName, attempt, failAttempts, exceptionType);
				
				// Throw exception based on configured type
				switch (exceptionType.toUpperCase()) {
					case "SOCKET_TIMEOUT":
						// Use unchecked wrapper that Spring Retry can catch
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
				// Reset counter for this call after successful attempt
				counters.remove(callName);
			}
		}
		
		log.info("HttpPaymentService {} REQ: url={} body={}", callName, url, body);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, headers);

		ResponseEntity<Map<String, Object>> resp = rt.exchange(url, HttpMethod.POST, req,
				new ParameterizedTypeReference<Map<String, Object>>() {});
		int statusCode = resp.getStatusCode().value();
		Map<String, Object> respBody = resp.getBody();

		log.info("HttpPaymentService {} RESP: url={} statusCode={} body={}", callName, url, statusCode, respBody);
		
		// Clear test mode counter on success
		if (testMode.isEnabled()) {
			attemptCounters.get().remove(callName);
		}
		
		// Retry on 5xx server errors (transient failures)
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
