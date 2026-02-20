package io.clubone.billing.service;

import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.ForecastRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for export operations.
 */
@Service
public class ExportService {

    private final BillingRunRepository billingRunRepository;
    private final DLQRepository dlqRepository;
    private final ForecastRepository forecastRepository;

    public ExportService(
            BillingRunRepository billingRunRepository,
            DLQRepository dlqRepository,
            ForecastRepository forecastRepository) {
        this.billingRunRepository = billingRunRepository;
        this.dlqRepository = dlqRepository;
        this.forecastRepository = forecastRepository;
    }

    public byte[] exportBillingRun(UUID billingRunId, String format, String exportType, Boolean includeSnapshots) {
        io.clubone.billing.api.dto.BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            return null;
        }

        if ("csv".equalsIgnoreCase(format)) {
            return exportBillingRunToCSV(billingRun);
        } else if ("excel".equalsIgnoreCase(format) || "xlsx".equalsIgnoreCase(format)) {
            return exportBillingRunToExcel(billingRun);
        }

        return null;
    }

    public byte[] exportPreview(UUID billingRunId, String format) {
        // Similar to exportBillingRun but for preview data
        return exportBillingRun(billingRunId, format, "preview", false);
    }

    public byte[] exportInvoices(UUID billingRunId, String format, String statusFilter) {
        // Export invoices for a billing run
        if ("csv".equalsIgnoreCase(format)) {
            return exportInvoicesToCSV(billingRunId, statusFilter);
        } else if ("pdf".equalsIgnoreCase(format)) {
            // PDF generation would require a library like Apache PDFBox or iText
            return null; // Not implemented
        }

        return null;
    }

    public byte[] exportDLQ(Boolean resolved, String failureTypeCode, String format) {
        List<io.clubone.billing.api.dto.DLQItemDto> items = dlqRepository.findDLQItems(
                null, failureTypeCode, resolved, 10000, 0, "created_on", "desc");

        if ("csv".equalsIgnoreCase(format)) {
            return exportDLQToCSV(items);
        }

        return null;
    }

    public byte[] exportForecast(LocalDate date, String format) {
        List<Map<String, Object>> items = forecastRepository.getForecastAggregated(date, date, "day");

        if ("csv".equalsIgnoreCase(format)) {
            return exportForecastToCSV(items);
        } else if ("excel".equalsIgnoreCase(format) || "xlsx".equalsIgnoreCase(format)) {
            // Excel generation would require Apache POI
            return null; // Not implemented
        }

        return null;
    }

    private byte[] exportBillingRunToCSV(io.clubone.billing.api.dto.BillingRunDto billingRun) {
        StringBuilder csv = new StringBuilder();
        csv.append("billing_run_code,due_date,status,invoices_count,total_amount\n");
        
        Integer invoicesCount = billingRun.summaryJson() != null && billingRun.summaryJson().containsKey("invoices_count") ?
                ((Number) billingRun.summaryJson().get("invoices_count")).intValue() : 0;
        Double totalAmount = billingRun.summaryJson() != null && billingRun.summaryJson().containsKey("total_amount") ?
                ((Number) billingRun.summaryJson().get("total_amount")).doubleValue() : 0.0;

        csv.append(String.format("%s,%s,%s,%d,%.2f\n",
                billingRun.billingRunCode(),
                billingRun.dueDate(),
                billingRun.billingRunStatus().statusCode(),
                invoicesCount,
                totalAmount));

        return csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private byte[] exportBillingRunToExcel(io.clubone.billing.api.dto.BillingRunDto billingRun) {
        // Excel export would require Apache POI
        // For now, return CSV as fallback
        return exportBillingRunToCSV(billingRun);
    }

    private byte[] exportInvoicesToCSV(UUID billingRunId, String statusFilter) {
        // This would query invoice data and export to CSV
        // Simplified implementation
        StringBuilder csv = new StringBuilder();
        csv.append("invoice_id,invoice_number,amount,status\n");
        // Add actual invoice data here
        return csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private byte[] exportDLQToCSV(List<io.clubone.billing.api.dto.DLQItemDto> items) {
        StringBuilder csv = new StringBuilder();
        csv.append("dlq_id,billing_run_code,invoice_number,error_type,error_message,resolved,created_on\n");

        for (io.clubone.billing.api.dto.DLQItemDto item : items) {
            csv.append(String.format("%s,%s,%s,%s,%s,%s,%s\n",
                    item.dlqId(),
                    item.billingRunCode() != null ? item.billingRunCode() : "",
                    item.invoiceNumber() != null ? item.invoiceNumber() : "",
                    item.errorType(),
                    item.errorMessage() != null ? item.errorMessage().replace(",", ";") : "",
                    item.resolved(),
                    item.createdOn()));
        }

        return csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private byte[] exportForecastToCSV(List<Map<String, Object>> items) {
        StringBuilder csv = new StringBuilder();
        csv.append("payment_due_date,invoice_count,total_amount\n");

        for (Map<String, Object> item : items) {
            csv.append(String.format("%s,%s,%.2f\n",
                    item.get("payment_due_date"),
                    item.get("invoice_count"),
                    ((Number) item.getOrDefault("total_amount", 0.0)).doubleValue()));
        }

        return csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
