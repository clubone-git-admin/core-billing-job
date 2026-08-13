package io.clubone.billing.service;

import io.clubone.billing.api.dto.ForecastItemDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.api.dto.StatusDto;
import io.clubone.billing.repo.ForecastRepository;
import io.clubone.billing.repo.LocationLevelRepository;
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
    private final LocationLevelRepository locationLevelRepository;

    public ForecastService(
            ForecastRepository forecastRepository,
            LocationLevelRepository locationLevelRepository) {
        this.forecastRepository = forecastRepository;
        this.locationLevelRepository = locationLevelRepository;
    }

    private static String normalizeCurrency(String currencyCode) {
        if (currencyCode == null || currencyCode.isBlank()) {
            return null;
        }
        return currencyCode.trim().toUpperCase();
    }

    public List<Map<String, Object>> getForecastAggregated(
            LocalDate from,
            LocalDate to,
            String groupBy,
            UUID locationLevelId,
            Boolean includeChildLocations,
            String currencyCode) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);
        String ccy = normalizeCurrency(currencyCode);
        List<Map<String, Object>> items =
                forecastRepository.getForecastAggregated(from, to, groupBy, locationIds, ccy);

        return items.stream().map(item -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("payment_due_date", item.get("payment_due_date"));
            out.put("invoice_count", item.get("invoice_count"));
            out.put("total_amount", item.get("total_amount"));
            if (item.get("amount_reporting") != null) {
                out.put("amount_reporting", item.get("amount_reporting"));
            }
            if (item.get("currency_code") != null) {
                out.put("currency_code", item.get("currency_code"));
            }
            if (item.get("reporting_currency_code") != null) {
                out.put("reporting_currency_code", item.get("reporting_currency_code"));
            }
            return out;
        }).collect(Collectors.toList());
    }

    public PageResponse<ForecastItemDto> getForecast(
            LocalDate from,
            LocalDate to,
            UUID locationLevelId,
            Boolean includeChildLocations,
            String currencyCode) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);
        String ccy = normalizeCurrency(currencyCode);
        List<Map<String, Object>> items =
                forecastRepository.getForecastItems(from, to, 100, 0, locationIds, ccy);
        Integer total = forecastRepository.countForecastItems(from, to, locationIds, ccy);

        List<ForecastItemDto> forecastItems = items.stream()
                .map(this::mapToForecastItemDto)
                .collect(Collectors.toList());

        return PageResponse.of(forecastItems, total, 100, 0);
    }

    public Map<String, Object> getForecastSummary(LocalDate date, String currencyCode) {
        String ccy = normalizeCurrency(currencyCode);
        Map<String, Object> summary = forecastRepository.getForecastSummary(date, ccy);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("payment_due_date", date.toString());
        out.put("total_invoices", nzNum(summary.get("total_invoices")));
        out.put("total_amount", nzNum(summary.get("total_amount")));
        if (summary.get("amount_reporting") != null) {
            out.put("amount_reporting", nzNum(summary.get("amount_reporting")));
        }
        if (summary.get("currency_code") != null) {
            out.put("currency_code", summary.get("currency_code").toString());
        }
        Object reportingCcy = summary.get("reporting_currency_code");
        if (reportingCcy != null && !reportingCcy.toString().isBlank()) {
            out.put("reporting_currency_code", reportingCcy.toString());
            out.put("currency", reportingCcy.toString());
        } else if (summary.get("currency_code") != null) {
            out.put("currency", summary.get("currency_code").toString());
        }

        Map<String, Object> pending = new LinkedHashMap<>();
        pending.put("count", nzNum(summary.get("pending_count")));
        pending.put("amount", nzNum(summary.get("pending_amount")));
        Map<String, Object> due = new LinkedHashMap<>();
        due.put("count", nzNum(summary.get("due_count")));
        due.put("amount", nzNum(summary.get("due_amount")));
        Map<String, Object> byStatus = new LinkedHashMap<>();
        byStatus.put("PENDING", pending);
        byStatus.put("DUE", due);
        out.put("by_status", byStatus);
        return out;
    }

    public PageResponse<ForecastItemDto> getForecastInvoices(
            LocalDate date, String search, UUID locationId, Boolean hasWarnings,
            Integer limit, Integer offset, String currencyCode) {
        String ccy = normalizeCurrency(currencyCode);
        List<Map<String, Object>> items = forecastRepository.getForecastInvoices(
                date, search, locationId, hasWarnings, limit, offset, ccy);

        List<ForecastItemDto> forecastItems = items.stream()
                .map(this::mapToForecastItemDto)
                .collect(Collectors.toList());

        Integer total = forecastRepository.countForecastItems(date, date, null, ccy);

        return PageResponse.of(forecastItems, total, limit, offset);
    }

    /**
     * Breakdown for forecast detail Reports tab (client / location / agreement).
     */
    public Map<String, Object> getForecastReports(LocalDate date, String reportType, String currencyCode) {
        String ccy = normalizeCurrency(currencyCode);
        String type = reportType == null || reportType.isBlank() ? "client" : reportType.trim().toLowerCase();
        List<Map<String, Object>> rows = forecastRepository.getForecastBreakdown(date, type, ccy);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("report_type", type);
        out.put("payment_due_date", date.toString());
        out.put("data", rows);
        return out;
    }

    public List<ForecastItemDto> getSubscriptionForecast(UUID subscriptionInstanceId, LocalDate from, LocalDate to) {
        List<Map<String, Object>> items = forecastRepository.getSubscriptionForecast(
                subscriptionInstanceId, from, to);

        return items.stream()
                .map(this::mapToForecastItemDto)
                .collect(Collectors.toList());
    }

    private ForecastItemDto mapToForecastItemDto(Map<String, Object> item) {
        String scheduleStatus = str(item.get("schedule_status"));
        if (scheduleStatus == null) {
            scheduleStatus = "PENDING";
        }
        StatusDto statusDto = new StatusDto(scheduleStatus, scheduleStatus, null);

        LocalDate paymentDueDate = toLocalDate(item.get("payment_due_date"));
        UUID subscriptionInstanceId = toUuid(item.get("subscription_instance_id"));
        UUID invoiceId = toUuid(item.get("invoice_id"));
        Integer cycleNumber = item.get("cycle_number") != null ?
                ((Number) item.get("cycle_number")).intValue() : 0;
        Double totalAmount = toDouble(item.get("total_amount"));
        Double amountReporting = toDouble(item.get("amount_reporting"));
        String currencyCode = str(item.get("currency_code"));
        if (currencyCode != null) {
            currencyCode = currencyCode.toUpperCase();
        }
        UUID locationId = toUuid(item.get("location_id"));
        String locationName = str(item.get("location_name"));
        UUID clientId = toUuid(item.get("client_id"));
        String clientName = str(item.get("client_name"));
        UUID agreementId = toUuid(item.get("agreement_id"));

        return new ForecastItemDto(
                paymentDueDate,
                subscriptionInstanceId,
                invoiceId,
                str(item.get("invoice_number")),
                cycleNumber,
                statusDto,
                1,
                totalAmount,
                amountReporting,
                currencyCode,
                locationId,
                locationName,
                clientId,
                clientName,
                agreementId,
                str(item.get("agreement_name")),
                Collections.emptyList(),
                Collections.emptyList()
        );
    }

    private static Object nzNum(Object v) {
        if (v == null) {
            return 0;
        }
        if (v instanceof Number n) {
            return n;
        }
        try {
            return new java.math.BigDecimal(v.toString().trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private static String str(Object v) {
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static Double toDouble(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(v.toString().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static LocalDate toLocalDate(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof LocalDate ld) {
            return ld;
        }
        if (v instanceof java.sql.Date d) {
            return d.toLocalDate();
        }
        if (v instanceof java.util.Date d) {
            return new java.sql.Date(d.getTime()).toLocalDate();
        }
        String s = v.toString().trim();
        if (s.length() >= 10) {
            return LocalDate.parse(s.substring(0, 10));
        }
        return LocalDate.parse(s);
    }

    private static UUID toUuid(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof UUID u) {
            return u;
        }
        String s = v.toString().trim();
        if (s.isEmpty()) {
            return null;
        }
        return UUID.fromString(s);
    }

    private List<UUID> resolveLocationIds(UUID locationLevelId, Boolean includeChildLocations) {
        if (locationLevelId == null) {
            return List.of();
        }
        boolean includeChildren = includeChildLocations == null || includeChildLocations;
        return locationLevelRepository
                .resolveLocationsForLevel(locationLevelId, includeChildren)
                .stream()
                .map(LocationLevelRepository.LocationRow::locationId)
                .toList();
    }
}
