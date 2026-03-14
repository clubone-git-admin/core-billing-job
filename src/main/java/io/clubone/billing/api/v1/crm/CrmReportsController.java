package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.CrmReportResponse;
import io.clubone.billing.service.CrmReportsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * CRM Report APIs. All require X-Application-Id, X-Location-Id, X-Actor-Id.
 * Reports are location(s)-specific: use query param location_ids (comma-separated UUIDs). Omit = all locations.
 * Base path: /api/crm/reports
 */
@RestController
@RequestMapping("/api/crm/reports")
@Tag(name = "CRM Reports", description = "Location leads/opportunities, lead source, conversion, aging, owner performance, campaign, calls, meetings, cases, SLA, forecast, growth, opportunity pipeline")
public class CrmReportsController {

    private final CrmReportsService reportsService;

    public CrmReportsController(CrmReportsService reportsService) {
        this.reportsService = reportsService;
    }

    /** Parse comma-separated UUID string to list. Empty/null/blank = null (all locations). */
    private static List<UUID> parseLocationIds(String locationIdsParam) {
        if (locationIdsParam == null || locationIdsParam.isBlank()) return null;
        List<UUID> list = Arrays.stream(locationIdsParam.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> {
                    try { return UUID.fromString(s); } catch (IllegalArgumentException e) { return null; }
                })
                .filter(u -> u != null)
                .distinct()
                .collect(Collectors.toList());
        return list.isEmpty() ? null : list;
    }

    @GetMapping("/location/leads")
    @Operation(summary = "Location-specific leads", description = "Leads filtered by location_ids (comma-separated). Omit = all locations.")
    public ResponseEntity<CrmReportResponse> getLeadsByLocation(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        return ResponseEntity.ok(reportsService.getLeadsByLocation(parseLocationIds(location_ids), limit, offset));
    }

    @GetMapping("/location/opportunities")
    @Operation(summary = "Location-specific opportunities")
    public ResponseEntity<CrmReportResponse> getOpportunitiesByLocation(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        return ResponseEntity.ok(reportsService.getOpportunitiesByLocation(parseLocationIds(location_ids), limit, offset));
    }

    @GetMapping("/lead-source")
    @Operation(summary = "Lead Source Report", description = "Lead count by source. location_ids = comma-separated; omit = all locations.")
    public ResponseEntity<CrmReportResponse> getLeadSourceReport(
            @RequestParam(name = "location_ids", required = false) String location_ids) {
        return ResponseEntity.ok(reportsService.getLeadSourceReport(parseLocationIds(location_ids)));
    }

    @GetMapping("/lead-conversion")
    @Operation(summary = "Lead Conversion Report", description = "Converted vs not converted by source; optional date range and location_ids")
    public ResponseEntity<CrmReportResponse> getLeadConversionReport(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to) {
        return ResponseEntity.ok(reportsService.getLeadConversionReport(parseLocationIds(location_ids), date_from, date_to));
    }

    @GetMapping("/lead-aging")
    @Operation(summary = "Lead Aging Report", description = "Lead count by status and age bucket (0-7, 8-30, 31-90, 90+ days)")
    public ResponseEntity<CrmReportResponse> getLeadAgingReport(
            @RequestParam(name = "location_ids", required = false) String location_ids) {
        return ResponseEntity.ok(reportsService.getLeadAgingReport(parseLocationIds(location_ids)));
    }

    @GetMapping("/lead-owner-performance")
    @Operation(summary = "Lead Owner Performance", description = "Total and converted lead count by owner")
    public ResponseEntity<CrmReportResponse> getLeadOwnerPerformanceReport(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to) {
        return ResponseEntity.ok(reportsService.getLeadOwnerPerformanceReport(parseLocationIds(location_ids), date_from, date_to));
    }

    @GetMapping("/campaign-performance")
    @Operation(summary = "Campaign Performance", description = "Campaigns with expected vs actual leads/revenue")
    public ResponseEntity<CrmReportResponse> getCampaignPerformanceReport(
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to) {
        return ResponseEntity.ok(reportsService.getCampaignPerformanceReport(date_from, date_to));
    }

    @GetMapping("/calls-made")
    @Operation(summary = "Calls Made Report", description = "Activities of type CALL in date range")
    public ResponseEntity<CrmReportResponse> getCallsMadeReport(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        return ResponseEntity.ok(reportsService.getCallsMadeReport(parseLocationIds(location_ids), date_from, date_to, limit, offset));
    }

    @GetMapping("/customer-interaction-timeline")
    @Operation(summary = "Customer Interaction Timeline", description = "Recent activities across org; optional entity_type (LEAD/CONTACT/OPPORTUNITY) and date range")
    public ResponseEntity<CrmReportResponse> getCustomerInteractionTimelineReport(
            @RequestParam(required = false) String entity_type,
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        return ResponseEntity.ok(reportsService.getCustomerInteractionTimelineReport(entity_type, date_from, date_to, limit, offset));
    }

    @GetMapping("/meetings-scheduled")
    @Operation(summary = "Meetings Scheduled", description = "Activities of type EVENT in date range")
    public ResponseEntity<CrmReportResponse> getMeetingsScheduledReport(
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        return ResponseEntity.ok(reportsService.getMeetingsScheduledReport(date_from, date_to, limit, offset));
    }

    @GetMapping("/case-tickets-by-category")
    @Operation(summary = "Case Tickets by Category", description = "Case count by type and status")
    public ResponseEntity<CrmReportResponse> getCaseTicketsByCategoryReport(
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to) {
        return ResponseEntity.ok(reportsService.getCaseTicketsByCategoryReport(date_from, date_to));
    }

    @GetMapping("/sla-compliance")
    @Operation(summary = "SLA Compliance", description = "Cases with SLA status (OK/BREACHED) based on resolve due date")
    public ResponseEntity<CrmReportResponse> getSlaComplianceReport(
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to) {
        return ResponseEntity.ok(reportsService.getSlaComplianceReport(date_from, date_to));
    }

    @GetMapping("/sales-forecast")
    @Operation(summary = "Sales Forecast Report", description = "Opportunity pipeline by stage with weighted amount (probability)")
    public ResponseEntity<CrmReportResponse> getSalesForecastReport(
            @RequestParam(name = "location_ids", required = false) String location_ids) {
        return ResponseEntity.ok(reportsService.getSalesForecastReport(parseLocationIds(location_ids)));
    }

    @GetMapping("/lead-growth-trends")
    @Operation(summary = "Lead Growth Trends", description = "Lead count by period (day/week/month); optional location_ids and date range")
    public ResponseEntity<CrmReportResponse> getLeadGrowthTrendsReport(
            @RequestParam(name = "location_ids", required = false) String location_ids,
            @RequestParam(required = false) String period,
            @RequestParam(required = false) String date_from,
            @RequestParam(required = false) String date_to,
            @RequestParam(required = false) Integer limit) {
        return ResponseEntity.ok(reportsService.getLeadGrowthTrendsReport(parseLocationIds(location_ids), period, date_from, date_to, limit));
    }

    @GetMapping("/opportunity-pipeline-by-stage")
    @Operation(summary = "Opportunity Pipeline by Stage", description = "Opportunity count and total amount by stage")
    public ResponseEntity<CrmReportResponse> getOpportunityPipelineByStageReport(
            @RequestParam(name = "location_ids", required = false) String location_ids) {
        return ResponseEntity.ok(reportsService.getOpportunityPipelineByStageReport(parseLocationIds(location_ids)));
    }
}
