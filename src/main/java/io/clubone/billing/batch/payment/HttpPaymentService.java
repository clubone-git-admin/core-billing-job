package io.clubone.billing.batch.payment;

import io.clubone.billing.batch.BillingJobProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.UUID;

@Service
public class HttpPaymentService implements PaymentService {

	private static final Logger log = LoggerFactory.getLogger(HttpPaymentService.class);

	private final BillingJobProperties props;
	private final RestTemplate rt = new RestTemplate();

	public HttpPaymentService(BillingJobProperties props) {
		this.props = props;
	}

	@Override
	public PaymentResult billInvoiceRecurring(UUID invoiceId, UUID clientRoleId, UUID clientPaymentMethodId,
			long amountMinor, String currencyCode) {
		log.info("billInvoiceRecurring REQ: invoiceId={} clientRoleId={} clientPaymentMethodId={} amountMinor={} currencyCode={}",
				invoiceId, clientRoleId, clientPaymentMethodId, amountMinor, currencyCode);

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
			    return new PaymentResult(false, null, "PAYMENT_FAILED", intentId, clientPaymentTxnId, null);
			}

			if ("PENDING_CAPTURE".equalsIgnoreCase(status) || "AUTHORIZED".equalsIgnoreCase(status) || "CREATED".equalsIgnoreCase(status)) {
			    // DO NOT finalize here; webhook will finalize when payment becomes captured
			    log.info("billInvoiceRecurring charge-at-will PENDING: invoiceId={} status={} intentId={} txnId={}",
			            invoiceId, status, intentId, clientPaymentTxnId);
			    return new PaymentResult(false, "PENDING_CAPTURE", "WAIT_FOR_WEBHOOK", intentId, clientPaymentTxnId, null);
			}

			if (!"CAPTURED".equalsIgnoreCase(status)) {
			    log.warn("billInvoiceRecurring charge-at-will unexpected status: invoiceId={} status={} intentId={} txnId={}",
			            invoiceId, status, intentId, clientPaymentTxnId);
			    return new PaymentResult(false, null, "UNSUPPORTED_STATUS:" + status, intentId, clientPaymentTxnId, null);
			}

			// Only CAPTURED can finalize immediately
			
			/*
			 * // 4) finalize in transaction-service String finalizeUrl =
			 * props.getPayment().getHttp().getTxnBaseUrl() +
			 * props.getPayment().getHttp().getFinalizePath(); Map<String, Object>
			 * finalizeReq = Map.of( "invoiceId", invoiceId.toString(), "clientRoleId",
			 * clientRoleId.toString(), "paymentGatewayCode", "RAZORPAY",
			 * "paymentMethodCode", "CARD", "paymentTypeCode",
			 * props.getPayment().getHttp().getPaymentTypeCode(), "createdBy",
			 * props.getPayment().getHttp().getActorId(), "clientPaymentTransactionId",
			 * clientPaymentTxnId.toString()); Map<String, Object> finalizeResp =
			 * postJson("finalize", finalizeUrl, finalizeReq);
			 * log.info("billInvoiceRecurring finalize RESP: invoiceId={} response={}",
			 * invoiceId, finalizeResp);
			 * 
			 * UUID transactionId = null; if (finalizeResp.get("transactionId") != null) {
			 * transactionId =
			 * UUID.fromString(String.valueOf(finalizeResp.get("transactionId"))); }
			 */
			log.info("billInvoiceRecurring RESP (success): invoiceId={} intentId={} clientPaymentTxnId={} transactionId={} razorpayOrderId={}",
					invoiceId, intentId, clientPaymentTxnId, null, razorpayOrderId);
			return new PaymentResult(true, "RZP_ORDER:" + razorpayOrderId, null, intentId, clientPaymentTxnId, null);

		} catch (Exception e) {
			log.error("billInvoiceRecurring RESP (error): invoiceId={} error={}", invoiceId, e.getMessage(), e);
			return PaymentResult.fail("HTTP_ERROR:" + e.getMessage());
		}
	}

	private Map<String, Object> postJson(String callName, String url, Map<String, Object> body) {
		log.info("HttpPaymentService {} REQ: url={} body={}", callName, url, body);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, headers);

		ResponseEntity<Map<String, Object>> resp = rt.exchange(url, HttpMethod.POST, req,
				new ParameterizedTypeReference<Map<String, Object>>() {});
		int statusCode = resp.getStatusCode().value();
		Map<String, Object> respBody = resp.getBody();

		log.info("HttpPaymentService {} RESP: url={} statusCode={} body={}", callName, url, statusCode, respBody);

		if (!resp.getStatusCode().is2xxSuccessful() || respBody == null) {
			log.error("HttpPaymentService {} failed: url={} statusCode={} body={}", callName, url, statusCode, respBody);
			throw new IllegalStateException("POST failed " + resp.getStatusCode() + " url=" + url);
		}
		return respBody;
	}
}
