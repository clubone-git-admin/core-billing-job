package io.clubone.billing.service;

import io.clubone.billing.repo.MetricsRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for metrics and reporting operations.
 */
@Service
public class MetricsService {

    private final MetricsRepository metricsRepository;

    public MetricsService(MetricsRepository metricsRepository) {
        this.metricsRepository = metricsRepository;
    }

    public Map<String, Object> getRunMetrics(LocalDate fromDate, LocalDate toDate, String groupBy, String metric) {
        List<Map<String, Object>> dataPoints = metricsRepository.getRunMetrics(fromDate, toDate, groupBy, null);

        List<Map<String, Object>> formattedDataPoints = dataPoints.stream()
                .map(dp -> {
                    Integer totalRuns = ((Number) dp.getOrDefault("total_runs", 0)).intValue();
                    Integer successfulRuns = ((Number) dp.getOrDefault("successful_runs", 0)).intValue();
                    Double successRate = totalRuns > 0 ? (successfulRuns.doubleValue() / totalRuns.doubleValue()) * 100 : 0.0;

                    return Map.of(
                            "group", dp.get("group_date"),
                            "value", "success_rate".equals(metric) ? successRate : totalRuns,
                            "count", successfulRuns,
                            "total", totalRuns
                    );
                })
                .collect(Collectors.toList());

        // Calculate summary
        Double average = formattedDataPoints.stream()
                .mapToDouble(dp -> ((Number) dp.get("value")).doubleValue())
                .average()
                .orElse(0.0);

        Double min = formattedDataPoints.stream()
                .mapToDouble(dp -> ((Number) dp.get("value")).doubleValue())
                .min()
                .orElse(0.0);

        Double max = formattedDataPoints.stream()
                .mapToDouble(dp -> ((Number) dp.get("value")).doubleValue())
                .max()
                .orElse(0.0);

        return Map.of(
                "metric", metric,
                "group_by", groupBy,
                "period", Map.of("from", fromDate, "to", toDate),
                "data_points", formattedDataPoints,
                "summary", Map.of(
                        "average", average,
                        "min", min,
                        "max", max
                )
        );
    }

    public Map<String, Object> getStageMetrics(String stageCode, LocalDate fromDate, LocalDate toDate) {
        List<Map<String, Object>> stages = metricsRepository.getStageMetrics(stageCode, fromDate, toDate);

        List<Map<String, Object>> stageList = stages.stream()
                .map(s -> {
                    Integer totalRuns = ((Number) s.getOrDefault("total_runs", 0)).intValue();
                    Integer successfulRuns = ((Number) s.getOrDefault("successful_runs", 0)).intValue();
                    Integer failedRuns = ((Number) s.getOrDefault("failed_runs", 0)).intValue();
                    Double successRate = totalRuns > 0 ? (successfulRuns.doubleValue() / totalRuns.doubleValue()) * 100 : 0.0;

                    Map<String, Object> stage = new HashMap<>();
                    stage.put("stage_code", s.get("stage_code"));
                    stage.put("stage_display_name", s.get("stage_display_name"));
                    stage.put("total_runs", totalRuns);
                    stage.put("successful_runs", successfulRuns);
                    stage.put("failed_runs", failedRuns);
                    stage.put("success_rate", successRate);

                    if (s.get("avg_duration_seconds") != null) {
                        stage.put("avg_duration_seconds", ((Number) s.get("avg_duration_seconds")).intValue());
                    }
                    if (s.get("min_duration_seconds") != null) {
                        stage.put("min_duration_seconds", ((Number) s.get("min_duration_seconds")).intValue());
                    }
                    if (s.get("max_duration_seconds") != null) {
                        stage.put("max_duration_seconds", ((Number) s.get("max_duration_seconds")).intValue());
                    }

                    return stage;
                })
                .collect(Collectors.toList());

        return Map.of("stages", stageList);
    }

    public Map<String, Object> getChargeMetrics(LocalDate fromDate, LocalDate toDate, String groupBy) {
        Map<String, Object> summary = metricsRepository.getChargeMetricsSummary(fromDate, toDate);
        List<Map<String, Object>> byFailureType = (List<Map<String, Object>>) 
                metricsRepository.getChargeMetricsByFailureType(fromDate, toDate).get("by_failure_type");
        List<Map<String, Object>> byErrorType = metricsRepository.getChargeMetricsByErrorType(fromDate, toDate);

        Integer totalCharges = ((Number) summary.getOrDefault("total_charges", 0)).intValue();
        Integer successfulCharges = ((Number) summary.getOrDefault("successful_charges", 0)).intValue();
        Integer failedCharges = ((Number) summary.getOrDefault("failed_charges", 0)).intValue();
        Double successRate = totalCharges > 0 ? (successfulCharges.doubleValue() / totalCharges.doubleValue()) * 100 : 0.0;

        Map<String, Object> summaryMap = new HashMap<>(summary);
        summaryMap.put("success_rate", successRate);

        Map<String, Object> byFailureTypeMap = new HashMap<>();
        for (Map<String, Object> ft : byFailureType) {
            String code = (String) ft.get("failure_type_code");
            Integer count = ((Number) ft.getOrDefault("count", 0)).intValue();
            Double amount = ((Number) ft.getOrDefault("amount", 0.0)).doubleValue();
            Double totalFailed = ((Number) summary.getOrDefault("total_amount_failed", 0.0)).doubleValue();
            Double percentage = totalFailed > 0 ? (amount / totalFailed) * 100 : 0.0;

            byFailureTypeMap.put(code, Map.of(
                    "count", count,
                    "amount", amount,
                    "percentage", percentage
            ));
        }

        Map<String, Object> byErrorTypeMap = new HashMap<>();
        for (Map<String, Object> et : byErrorType) {
            String errorType = (String) et.get("error_type");
            byErrorTypeMap.put(errorType, Map.of(
                    "count", et.get("count"),
                    "amount", et.get("amount")
            ));
        }

        return Map.of(
                "summary", summaryMap,
                "by_failure_type", byFailureTypeMap,
                "by_error_type", byErrorTypeMap
        );
    }

    public Map<String, Object> getExecutiveSummary(LocalDate fromDate, LocalDate toDate, UUID locationId) {
        Map<String, Object> summary = metricsRepository.getExecutiveSummary(fromDate, toDate, locationId);

        Integer totalRuns = ((Number) summary.getOrDefault("total_runs", 0)).intValue();
        Integer totalInvoices = ((Number) summary.getOrDefault("total_invoices", 0)).intValue();
        Integer successfulInvoices = ((Number) summary.getOrDefault("successful_invoices", 0)).intValue();
        Double successRate = totalInvoices > 0 ? (successfulInvoices.doubleValue() / totalInvoices.doubleValue()) * 100 : 0.0;

        Map<String, Object> summaryMap = Map.of(
                "total_runs", totalRuns,
                "total_invoices", totalInvoices,
                "total_amount", summary.get("total_amount"),
                "success_rate", successRate,
                "avg_processing_time_hours", summary.getOrDefault("avg_processing_time_hours", 0.0),
                "total_charged", summary.getOrDefault("total_amount_charged", 0.0),
                "total_failed", summary.getOrDefault("total_amount_failed", 0.0)
        );

        Map<String, Object> trends = Map.of(
                "runs_trend", "stable", // Would need historical comparison
                "amount_trend", "stable",
                "success_rate_trend", "stable"
        );

        Map<String, Object> performance = Map.of(
                "fastest_run_minutes", summary.getOrDefault("fastest_run_minutes", 0),
                "slowest_run_minutes", summary.getOrDefault("slowest_run_minutes", 0),
                "avg_run_minutes", summary.getOrDefault("avg_run_minutes", 0)
        );

        // Top issues would come from DLQ analysis
        List<Map<String, Object>> topIssues = List.of();

        return Map.of(
                "period", Map.of("from", fromDate, "to", toDate),
                "summary", summaryMap,
                "trends", trends,
                "top_issues", topIssues,
                "performance", performance
        );
    }
}

