package io.clubone.billing.service;

import io.clubone.billing.repo.ReconciliationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Service for billing reconciliation operations (v1 API).
 * Handles invoice and billing run reconciliation.
 */
@Service
public class BillingReconciliationService {

    private final ReconciliationRepository reconciliationRepository;

    public BillingReconciliationService(ReconciliationRepository reconciliationRepository) {
        this.reconciliationRepository = reconciliationRepository;
    }

    public Map<String, Object> getInvoiceReconciliation(UUID invoiceId) {
        Map<String, Object> data = reconciliationRepository.getInvoiceReconciliation(invoiceId);
        if (data == null) {
            return null;
        }

        // Build response structure
        Map<String, Object> invoice = Map.of(
                "invoice_id", data.get("invoice_id"),
                "invoice_number", data.get("invoice_number"),
                "invoice_total", data.get("invoice_total"),
                "invoice_status", data.get("invoice_status"),
                "created_on", data.get("invoice_created_on")
        );

        Map<String, Object> billingHistory = new HashMap<>();
        if (data.get("subscription_billing_history_id") != null) {
            billingHistory = Map.of(
                    "subscription_billing_history_id", data.get("subscription_billing_history_id"),
                    "billing_run_id", data.get("billing_run_id"),
                    "billing_run_code", data.get("billing_run_code"),
                    "stage_run_id", data.get("stage_run_id"),
                    "stage_run_code", data.get("stage_run_code"),
                    "billing_status", Map.of(
                            "status_code", data.get("billing_status_code"),
                            "display_name", data.get("billing_status_display")
                    ),
                    "billing_attempt_on", data.get("billing_attempt_on"),
                    "invoice_total_amount", data.get("invoice_total_amount")
            );
        }

        Map<String, Object> paymentIntent = new HashMap<>();
        if (data.get("client_payment_intent_id") != null) {
            paymentIntent = Map.of(
                    "client_payment_intent_id", data.get("client_payment_intent_id"),
                    "amount", data.get("intent_amount"),
                    "status", data.get("intent_status"),
                    "created_on", data.get("intent_created_on")
            );
        }

        Map<String, Object> paymentTransaction = new HashMap<>();
        if (data.get("client_payment_transaction_id") != null) {
            paymentTransaction = Map.of(
                    "client_payment_transaction_id", data.get("client_payment_transaction_id"),
                    "gateway_transaction_id", data.get("gateway_order_id"),
                    "amount", data.get("transaction_amount"),
                    "status", data.get("transaction_status"),
                    "created_on", data.get("transaction_created_on")
            );
        }

        // Check for mismatches
        List<Map<String, Object>> mismatches = new ArrayList<>();
        if (data.get("invoice_total") != null && data.get("transaction_amount") != null) {
            Double invoiceTotal = ((Number) data.get("invoice_total")).doubleValue();
            Double transactionAmount = ((Number) data.get("transaction_amount")).doubleValue();
            if (Math.abs(invoiceTotal - transactionAmount) > 0.01) {
                mismatches.add(Map.of(
                        "type", "AMOUNT_MISMATCH",
                        "description", String.format("Invoice amount (%.2f) differs from transaction amount (%.2f)",
                                invoiceTotal, transactionAmount),
                        "severity", "NONE"
                ));
            }
        }

        String reconciliationStatus = mismatches.isEmpty() ? "RECONCILED" : "UNRECONCILED";

        return Map.of(
                "invoice", invoice,
                "billing_history", billingHistory,
                "payment_intent", paymentIntent,
                "payment_transaction", paymentTransaction,
                "mismatches", mismatches,
                "reconciliation_status", reconciliationStatus
        );
    }

    public Map<String, Object> getBillingRunReconciliation(UUID billingRunId) {
        Map<String, Object> summary = reconciliationRepository.getBillingRunReconciliation(billingRunId);
        List<Map<String, Object>> mismatches = reconciliationRepository.getMismatches(billingRunId);

        // Calculate outstanding amount
        Double invoicedAmount = ((Number) summary.getOrDefault("invoiced_amount", 0.0)).doubleValue();
        Double collectedAmount = ((Number) summary.getOrDefault("collected_amount", 0.0)).doubleValue();
        Double failedAmount = ((Number) summary.getOrDefault("failed_amount", 0.0)).doubleValue();
        Double outstandingAmount = invoicedAmount - collectedAmount - failedAmount;

        Map<String, Object> summaryMap = new HashMap<>(summary);
        summaryMap.put("outstanding_amount", outstandingAmount);

        List<Map<String, Object>> mismatchList = mismatches.stream()
                .map(m -> Map.of(
                        "invoice_id", m.get("invoice_id"),
                        "invoice_number", m.get("invoice_number"),
                        "type", m.get("mismatch_type"),
                        "description", m.get("mismatch_type"),
                        "severity", m.get("severity")
                ))
                .toList();

        return Map.of(
                "billing_run_id", billingRunId,
                "summary", summaryMap,
                "mismatches", mismatchList
        );
    }

    @Transactional
    public Map<String, Object> reconcileInvoice(UUID invoiceId, Map<String, Object> request) {
        String reconciledByStr = (String) request.getOrDefault("resolved_by", "system");
        UUID reconciledBy = null;
        try {
            reconciledBy = UUID.fromString(reconciledByStr);
        } catch (Exception e) {
            // Use null if invalid UUID
        }

        String notes = (String) request.getOrDefault("reconciliation_notes", "");
        Boolean forceReconcile = (Boolean) request.getOrDefault("force_reconcile", false);

        reconciliationRepository.reconcileInvoice(invoiceId, reconciledBy, notes);

        return Map.of(
                "invoice_id", invoiceId,
                "reconciliation_status", "RECONCILED",
                "reconciled_by", reconciledByStr,
                "reconciled_on", OffsetDateTime.now(),
                "reconciliation_notes", notes
        );
    }
}

