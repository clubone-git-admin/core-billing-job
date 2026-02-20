package io.clubone.billing.service;

import io.clubone.billing.api.dto.ForecastItemDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.api.dto.StatusDto;
import io.clubone.billing.repo.ForecastRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for forecast operations.
 */
@Service
public class ForecastService {

    private final ForecastRepository forecastRepository;

    public ForecastService(ForecastRepository forecastRepository) {
        this.forecastRepository = forecastRepository;
    }

    public List<Map<String, Object>> getForecastAggregated(LocalDate from, LocalDate to, String groupBy) {
        List<Map<String, Object>> items = forecastRepository.getForecastAggregated(from, to, groupBy);
        
        return items.stream()
                .map(item -> Map.of(
                        "payment_due_date", item.get("payment_due_date"),
                        "invoice_count", item.get("invoice_count"),
                        "total_amount", item.get("total_amount")
                ))
                .collect(Collectors.toList());
    }

    public PageResponse<ForecastItemDto> getForecast(LocalDate from, LocalDate to) {
        List<Map<String, Object>> items = forecastRepository.getForecastItems(from, to, 100, 0);
        Integer total = forecastRepository.countForecastItems(from, to);

        List<ForecastItemDto> forecastItems = items.stream()
                .map(this::mapToForecastItemDto)
                .collect(Collectors.toList());

        return PageResponse.of(forecastItems, total, 100, 0);
    }

    public Map<String, Object> getForecastSummary(LocalDate date) {
        Map<String, Object> summary = forecastRepository.getForecastSummary(date);
        
        return Map.of(
                "payment_due_date", date,
                "total_invoices", summary.get("total_invoices"),
                "total_amount", summary.get("total_amount"),
                "by_status", Map.of(
                        "PENDING", Map.of(
                                "count", summary.get("pending_count"),
                                "amount", summary.get("pending_amount")
                        ),
                        "DUE", Map.of(
                                "count", summary.get("due_count"),
                                "amount", summary.get("due_amount")
                        )
                )
        );
    }

    public PageResponse<ForecastItemDto> getForecastInvoices(
            LocalDate date, String search, UUID locationId, Boolean hasWarnings,
            Integer limit, Integer offset) {
        
        List<Map<String, Object>> items = forecastRepository.getForecastInvoices(
                date, search, locationId, hasWarnings, limit, offset);
        
        List<ForecastItemDto> forecastItems = items.stream()
                .map(this::mapToForecastItemDto)
                .collect(Collectors.toList());

        // Count total (simplified - in production, should count with same filters)
        Integer total = forecastRepository.countForecastItems(date, date);

        return PageResponse.of(forecastItems, total, limit, offset);
    }

    public List<ForecastItemDto> getSubscriptionForecast(UUID subscriptionInstanceId, LocalDate from, LocalDate to) {
        List<Map<String, Object>> items = forecastRepository.getSubscriptionForecast(
                subscriptionInstanceId, from, to);
        
        return items.stream()
                .map(item -> {
                    String scheduleStatus = (String) item.get("schedule_status");
                    StatusDto statusDto = new StatusDto(
                            scheduleStatus,
                            scheduleStatus,
                            null
                    );

                    return new ForecastItemDto(
                            (LocalDate) item.get("payment_due_date"),
                            (UUID) item.get("subscription_instance_id"),
                            (UUID) item.get("invoice_id"),
                            ((Number) item.getOrDefault("cycle_number", 0)).intValue(),
                            statusDto,
                            1, // invoice_count
                            null, // total_amount - would need to join with invoice
                            null, // location_id
                            null, // location_name
                            null, // client_id
                            null, // client_name
                            null, // agreement_id
                            List.of(), // warnings
                            List.of() // validation_errors
                    );
                })
                .collect(Collectors.toList());
    }

    private ForecastItemDto mapToForecastItemDto(Map<String, Object> item) {
        String scheduleStatus = (String) item.getOrDefault("schedule_status", "PENDING");
        StatusDto statusDto = new StatusDto(scheduleStatus, scheduleStatus, null);

        LocalDate paymentDueDate = (LocalDate) item.get("payment_due_date");
        UUID subscriptionInstanceId = item.get("subscription_instance_id") != null ? 
                (UUID) item.get("subscription_instance_id") : null;
        UUID invoiceId = item.get("invoice_id") != null ? 
                (UUID) item.get("invoice_id") : null;
        Integer cycleNumber = item.get("cycle_number") != null ? 
                ((Number) item.get("cycle_number")).intValue() : 0;
        Double totalAmount = item.get("total_amount") != null ? 
                ((Number) item.get("total_amount")).doubleValue() : null;

        return new ForecastItemDto(
                paymentDueDate,
                subscriptionInstanceId,
                invoiceId,
                cycleNumber,
                statusDto,
                1, // invoice_count
                totalAmount,
                null, // location_id
                null, // location_name
                null, // client_id
                null, // client_name
                null, // agreement_id
                Collections.emptyList(), // warnings
                Collections.emptyList() // validation_errors
        );
    }
}

