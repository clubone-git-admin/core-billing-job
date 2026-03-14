package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.crm.CrmReportResponse;
import io.clubone.billing.repo.CrmReportsRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for CRM report APIs. Uses CrmRequestContext for org_client_id and optional location_id.
 */
@Service
public class CrmReportsService {

    private final CrmReportsRepository reportsRepository;
    private final CrmRequestContext context;

    public CrmReportsService(CrmReportsRepository reportsRepository, CrmRequestContext context) {
        this.reportsRepository = reportsRepository;
        this.context = context;
    }

    /** 1. Location-specific leads. location_ids = null or empty = all locations. */
    public CrmReportResponse getLeadsByLocation(List<UUID> locationIds, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadsByLocation(orgId, locationIds, limit, offset);
        long total = reportsRepository.countLeadsByLocation(orgId, locationIds);
        return CrmReportResponse.of("location_leads", rows, total);
    }

    /** 2. Location-specific opportunities. */
    public CrmReportResponse getOpportunitiesByLocation(List<UUID> locationIds, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getOpportunitiesByLocation(orgId, locationIds, limit, offset);
        long total = reportsRepository.countOpportunitiesByLocation(orgId, locationIds);
        return CrmReportResponse.of("location_opportunities", rows, total);
    }

    /** 3. Lead Source Report. location_ids = null or empty = all locations. */
    public CrmReportResponse getLeadSourceReport(List<UUID> locationIds) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadSourceReport(orgId, locationIds);
        return CrmReportResponse.of("lead_source", rows);
    }

    /** 4. Lead Conversion Report. */
    public CrmReportResponse getLeadConversionReport(List<UUID> locationIds, String dateFrom, String dateTo) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadConversionReport(orgId, locationIds, dateFrom, dateTo);
        return CrmReportResponse.of("lead_conversion", rows);
    }

    /** 5. Lead Aging Report. */
    public CrmReportResponse getLeadAgingReport(List<UUID> locationIds) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadAgingReport(orgId, locationIds);
        return CrmReportResponse.of("lead_aging", rows);
    }

    /** 6. Lead Owner Performance. */
    public CrmReportResponse getLeadOwnerPerformanceReport(List<UUID> locationIds, String dateFrom, String dateTo) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadOwnerPerformanceReport(orgId, locationIds, dateFrom, dateTo);
        return CrmReportResponse.of("lead_owner_performance", rows);
    }

    /** 7. Campaign Performance. */
    public CrmReportResponse getCampaignPerformanceReport(String dateFrom, String dateTo) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getCampaignPerformanceReport(orgId, dateFrom, dateTo);
        return CrmReportResponse.of("campaign_performance", rows);
    }

    /** 8. Calls Made Report. */
    public CrmReportResponse getCallsMadeReport(List<UUID> locationIds, String dateFrom, String dateTo, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getCallsMadeReport(orgId, locationIds, dateFrom, dateTo, limit, offset);
        return CrmReportResponse.of("calls_made", rows);
    }

    /** 9. Customer Interaction Timeline. */
    public CrmReportResponse getCustomerInteractionTimelineReport(String entityTypeCode, String dateFrom, String dateTo, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getCustomerInteractionTimelineReport(orgId, entityTypeCode, dateFrom, dateTo, limit, offset);
        return CrmReportResponse.of("customer_interaction_timeline", rows);
    }

    /** 10. Meetings Scheduled. */
    public CrmReportResponse getMeetingsScheduledReport(String dateFrom, String dateTo, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getMeetingsScheduledReport(orgId, dateFrom, dateTo, limit, offset);
        return CrmReportResponse.of("meetings_scheduled", rows);
    }

    /** 11. Case Tickets by Category. */
    public CrmReportResponse getCaseTicketsByCategoryReport(String dateFrom, String dateTo) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getCaseTicketsByCategoryReport(orgId, dateFrom, dateTo);
        return CrmReportResponse.of("case_tickets_by_category", rows);
    }

    /** 12. SLA Compliance. */
    public CrmReportResponse getSlaComplianceReport(String dateFrom, String dateTo) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getSlaComplianceReport(orgId, dateFrom, dateTo);
        return CrmReportResponse.of("sla_compliance", rows);
    }

    /** 13. Sales Forecast. location_ids = null or empty = all locations. */
    public CrmReportResponse getSalesForecastReport(List<UUID> locationIds) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getSalesForecastReport(orgId, locationIds);
        return CrmReportResponse.of("sales_forecast", rows);
    }

    /** 14. Lead Growth Trends. */
    public CrmReportResponse getLeadGrowthTrendsReport(List<UUID> locationIds, String period, String dateFrom, String dateTo, Integer limit) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getLeadGrowthTrendsReport(orgId, locationIds, period, dateFrom, dateTo, limit);
        return CrmReportResponse.of("lead_growth_trends", rows);
    }

    /** 15. Opportunity Pipeline by Stage. */
    public CrmReportResponse getOpportunityPipelineByStageReport(List<UUID> locationIds) {
        UUID orgId = context.getOrgClientId();
        List<Map<String, Object>> rows = reportsRepository.getOpportunityPipelineByStageReport(orgId, locationIds);
        return CrmReportResponse.of("opportunity_pipeline_by_stage", rows);
    }
}
