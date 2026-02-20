package io.clubone.billing.service;

import io.clubone.billing.repo.CompareRepository;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for compare operations.
 */
@Service
public class CompareService {

    private final CompareRepository compareRepository;

    public CompareService(CompareRepository compareRepository) {
        this.compareRepository = compareRepository;
    }

    public Map<String, Object> compareRuns(UUID runA, UUID runB, String compareType) {
        Map<String, Object> summaryA = compareRepository.getBillingRunSummary(runA);
        Map<String, Object> summaryB = compareRepository.getBillingRunSummary(runB);

        if (summaryA == null || summaryB == null) {
            return null;
        }

        // Calculate differences
        Integer invoiceCountDelta = ((Number) summaryB.getOrDefault("invoices_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("invoices_count", 0)).intValue();
        Double totalAmountDelta = ((Number) summaryB.getOrDefault("total_amount", 0.0)).doubleValue() -
                ((Number) summaryA.getOrDefault("total_amount", 0.0)).doubleValue();
        Integer successCountDelta = ((Number) summaryB.getOrDefault("success_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("success_count", 0)).intValue();
        Integer failureCountDelta = ((Number) summaryB.getOrDefault("failure_count", 0)).intValue() -
                ((Number) summaryA.getOrDefault("failure_count", 0)).intValue();

        Map<String, Object> differences = Map.of(
                "invoice_count_delta", invoiceCountDelta,
                "total_amount_delta", totalAmountDelta,
                "success_count_delta", successCountDelta,
                "failure_count_delta", failureCountDelta
        );

        List<Map<String, Object>> invoiceDiffs = new ArrayList<>();
        List<Map<String, Object>> onlyInA = new ArrayList<>();
        List<Map<String, Object>> onlyInB = new ArrayList<>();

        if ("full".equals(compareType)) {
            invoiceDiffs = compareRepository.getInvoiceDifferences(runA, runB);
            onlyInA = compareRepository.getOnlyInRunA(runA, runB);
            onlyInB = compareRepository.getOnlyInRunB(runA, runB);
        }

        return Map.of(
                "run_a", Map.of(
                        "billing_run_id", summaryA.get("billing_run_id"),
                        "billing_run_code", summaryA.get("billing_run_code"),
                        "due_date", summaryA.get("due_date"),
                        "summary", Map.of(
                                "invoices_count", summaryA.get("invoices_count"),
                                "total_amount", summaryA.get("total_amount"),
                                "success_count", summaryA.get("success_count"),
                                "failure_count", summaryA.get("failure_count")
                        )
                ),
                "run_b", Map.of(
                        "billing_run_id", summaryB.get("billing_run_id"),
                        "billing_run_code", summaryB.get("billing_run_code"),
                        "due_date", summaryB.get("due_date"),
                        "summary", Map.of(
                                "invoices_count", summaryB.get("invoices_count"),
                                "total_amount", summaryB.get("total_amount"),
                                "success_count", summaryB.get("success_count"),
                                "failure_count", summaryB.get("failure_count")
                        )
                ),
                "differences", differences,
                "invoice_diffs", invoiceDiffs.stream().map(this::formatInvoiceDiff).collect(Collectors.toList()),
                "only_in_a", onlyInA.stream().map(this::formatInvoiceItem).collect(Collectors.toList()),
                "only_in_b", onlyInB.stream().map(this::formatInvoiceItem).collect(Collectors.toList())
        );
    }

    public Map<String, Object> compareWithPrevious(UUID billingRunId) {
        UUID previousRunId = compareRepository.findPreviousBillingRun(billingRunId);
        if (previousRunId == null) {
            return null;
        }

        return compareRuns(previousRunId, billingRunId, "full");
    }

    private Map<String, Object> formatInvoiceDiff(Map<String, Object> diff) {
        return Map.of(
                "subscription_instance_id", diff.get("subscription_instance_id"),
                "invoice_id_a", diff.get("invoice_id_a"),
                "invoice_id_b", diff.get("invoice_id_b"),
                "total_a", diff.get("total_a"),
                "total_b", diff.get("total_b"),
                "delta_total", diff.get("delta_total"),
                "status_a", diff.get("status_a"),
                "status_b", diff.get("status_b"),
                "failure_reason_a", diff.get("failure_reason_a"),
                "failure_reason_b", diff.get("failure_reason_b")
        );
    }

    private Map<String, Object> formatInvoiceItem(Map<String, Object> item) {
        return Map.of(
                "subscription_instance_id", item.get("subscription_instance_id"),
                "invoice_id", item.get("invoice_id"),
                "total_amount", item.get("total_amount"),
                "status", item.get("status")
        );
    }
}

