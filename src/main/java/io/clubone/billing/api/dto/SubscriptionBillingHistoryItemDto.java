package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Row for {@code GET /api/v1/billing/runs/{billingRunId}/invoices} (mock charge grid and related lists).
 * JSON uses snake_case per FE contract; {@code failure_code} maps from {@code mock_charge_failure_code}
 * (set only for blocked/error mock outcomes — successful mock charge leaves them null).
 * {@code failure_reason} and {@code failure_code} are always serialized (explicit JSON {@code null}) so clients are not ambiguous.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record SubscriptionBillingHistoryItemDto(
        UUID subscriptionBillingHistoryId,
        UUID invoiceId,
        String invoiceNumber,
        UUID subscriptionInstanceId,
        OffsetDateTime billingAttemptOn,
        String billingStatusCode,
        /** Populated for mock failures/blocks; null when mock validation passed (see {@link #mockChargeStatus}). */
        @JsonInclude(JsonInclude.Include.ALWAYS)
        String failureReason,
        Boolean isMock,
        UUID billingRunId,
        UUID stageRunId,
        BigDecimal invoiceSubTotal,
        BigDecimal invoiceTaxAmount,
        BigDecimal invoiceDiscountAmount,
        BigDecimal invoiceTotalAmount,
        String mockChargeStatus,
        /** Maps from {@code mock_charge_failure_code}; null on successful mock — always present in JSON. */
        @JsonInclude(JsonInclude.Include.ALWAYS)
        String failureCode,
        Map<String, Object> mockChargeDetails,
        String clientName,
        /** Organization client id when linked; often null. */
        UUID clientId,
        /** Invoice / agreement role id for search and subtitles. */
        UUID clientRoleId,
        String agreementOrPlanName,
        LocalDate billingPeriodStart,
        LocalDate billingPeriodEnd,
        String locationName,
        UUID locationId,
        String paymentMethodType,
        UUID mandateId,
        String mandateStatus,
        OffsetDateTime mandateValidFrom,
        OffsetDateTime mandateValidTo,
        BigDecimal mandateMaxAmount,
        /** ISO currency code from {@code client_gateway_mandate.mandate_currency} when present. */
        String mandateCurrency,
        OffsetDateTime mandateLastVerifiedAt,
        String paymentLast4,
        String paymentExpiry,
        OffsetDateTime simulatedOn,
        /** {@code clients.client_role.role_id} (business role identifier), distinct from {@code client_role_id}. */
        String roleId,
        String agreementStatus,
        /** Agreement product name from {@code agreements.agreement}; may duplicate {@link #agreementOrPlanName} when agreement-backed. */
        String agreementName,
        /** Payment processor / gateway display (e.g. Stripe), when linked from {@code payment_gateway_supported_method}. */
        String gatewayName,
        /** {@code transactions.lu_invoice_status.status_name} (e.g. PENDING, DUE). */
        String invoiceStatus
) {
}
