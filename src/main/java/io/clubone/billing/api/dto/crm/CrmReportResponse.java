package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Generic response for CRM report APIs. Each row is a map of column name to value (e.g. from repository query).
 * total is set for paginated reports (e.g. location leads/opportunities, calls, meetings, timeline).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmReportResponse(
        @JsonProperty("report_type") String reportType,
        @JsonProperty("rows") List<Map<String, Object>> rows,
        @JsonProperty("total") Long total
) {
    public static CrmReportResponse of(String reportType, List<Map<String, Object>> rows) {
        return new CrmReportResponse(reportType, rows, null);
    }

    public static CrmReportResponse of(String reportType, List<Map<String, Object>> rows, long total) {
        return new CrmReportResponse(reportType, rows, total);
    }
}
