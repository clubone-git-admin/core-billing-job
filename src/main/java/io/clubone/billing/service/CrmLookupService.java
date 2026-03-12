package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmLookupRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Service for CRM lookups used by Leads UI.
 */
@Service
public class CrmLookupService {

    private final CrmLookupRepository repository;
    private final CrmRequestContext context;

    public CrmLookupService(CrmLookupRepository repository, CrmRequestContext context) {
        this.repository = repository;
        this.context = context;
    }

    public List<CrmLookupItemDto> getLeadStatuses() {
        return toLookupItems(repository.getLeadStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadSources() {
        return toLookupItems(repository.getLeadSources(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadTypes() {
        return toLookupItems(repository.getLeadTypes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadRecordTypes() {
        return toLookupItems(repository.getLeadRecordTypes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getSalutations() {
        return toLookupItems(repository.getSalutations(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getGenders() {
        return toLookupItems(repository.getGenders(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getHomeClubs() {
        return toLookupItems(repository.getHomeClubs());
    }

    public List<CrmLookupItemDto> getUsers() {
        return toLookupItems(repository.getMemberAdvisorUsers(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCountries() {
        return toLookupItems(repository.getCountries());
    }

    public List<CrmLookupItemDto> getStates(String countryCode) {
        return toLookupItems(repository.getStates(countryCode));
    }

    public List<CrmLookupItemDto> getAccountsForLeadRecordType(UUID leadRecordTypeId) {
        return toLookupItems(repository.getAccountsForLeadRecordType(context.getOrgClientId(), leadRecordTypeId));
    }

    public List<CrmLookupItemDto> getCampaigns() {
        return toLookupItems(repository.getCampaigns(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getContacts() {
        return toLookupItems(repository.getContacts(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityTypes() {
        return toLookupItems(repository.getActivityTypes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityStatuses() {
        return toLookupItems(repository.getActivityStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityOutcomes() {
        return toLookupItems(repository.getActivityOutcomes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityVisibilities() {
        return toLookupItems(repository.getActivityVisibilities(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEntityTypes() {
        return toLookupItems(repository.getEntityTypes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCallDirections() {
        return toLookupItems(repository.getCallDirections(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCallResults() {
        return toLookupItems(repository.getCallResults(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmailDeliveryStatuses() {
        return toLookupItems(repository.getEmailDeliveryStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getSmsDeliveryStatuses() {
        return toLookupItems(repository.getSmsDeliveryStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getWhatsappDeliveryStatuses() {
        return toLookupItems(repository.getWhatsappDeliveryStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEventStatuses() {
        return toLookupItems(repository.getEventStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEventPurposes() {
        return toLookupItems(repository.getEventPurposes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getRsvpStatuses() {
        return toLookupItems(repository.getRsvpStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskTypes() {
        return toLookupItems(repository.getTaskTypes(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskStatuses() {
        return toLookupItems(repository.getTaskStatuses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskPriorities() {
        return toLookupItems(repository.getTaskPriorities(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmpty() {
        return toLookupItems(repository.getEmptyLookup());
    }

    public List<CrmLookupItemDto> getEmailFromAddresses() {
        return toLookupItems(repository.getEmailFromAddresses(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmailTemplates() {
        return toLookupItems(repository.getEmailTemplates(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getSmsIdentities() {
        return toLookupItems(repository.getSmsIdentities(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getWhatsappIdentities() {
        return toLookupItems(repository.getWhatsappIdentities(context.getOrgClientId()));
    }

    public List<CrmLookupItemDto> getContactLifecycles() {
        return toLookupItems(repository.getContactLifecycles(context.getOrgClientId()));
    }

    public List<CrmCaseTypeLookupDto> getCaseTypes() {
        return repository.getCaseTypes(context.getOrgClientId()).stream()
                .map(r -> new CrmCaseTypeLookupDto(asStr(r.get("case_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCaseStatusLookupDto> getCaseStatuses() {
        return repository.getCaseStatuses(context.getOrgClientId()).stream()
                .map(r -> new CrmCaseStatusLookupDto(asStr(r.get("case_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCasePriorityLookupDto> getCasePriorities() {
        return repository.getCasePriorities(context.getOrgClientId()).stream()
                .map(r -> new CrmCasePriorityLookupDto(asStr(r.get("case_priority_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmAccountTypeLookupDto> getAccountTypes() {
        return repository.getAccountTypes(context.getOrgClientId()).stream()
                .map(r -> new CrmAccountTypeLookupDto(asStr(r.get("account_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskTypeLookupDto> getTaskTypesFull() {
        return repository.getTaskTypesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmTaskTypeLookupDto(asStr(r.get("task_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskStatusLookupDto> getTaskStatusesFull() {
        return repository.getTaskStatusesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmTaskStatusLookupDto(asStr(r.get("task_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskPriorityLookupDto> getTaskPrioritiesFull() {
        return repository.getTaskPrioritiesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmTaskPriorityLookupDto(asStr(r.get("task_priority_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmEntityTypeLookupDto> getEntityTypesFull() {
        return repository.getEntityTypesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmEntityTypeLookupDto(asStr(r.get("entity_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCampaignTypeLookupDto> getCampaignTypesFull() {
        return repository.getCampaignTypesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmCampaignTypeLookupDto(asStr(r.get("campaign_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCampaignStatusLookupDto> getCampaignStatusesFull() {
        return repository.getCampaignStatusesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmCampaignStatusLookupDto(asStr(r.get("campaign_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmOpportunityStageLookupDto> getOpportunityStagesFull() {
        return repository.getOpportunityStagesFull(context.getOrgClientId()).stream()
                .map(r -> new CrmOpportunityStageLookupDto(
                        asStr(r.get("opportunity_stage_id")),
                        asStr(r.get("code")),
                        asStr(r.get("display_name")),
                        r.get("display_order") != null ? ((Number) r.get("display_order")).intValue() : null,
                        r.get("default_probability") != null ? ((Number) r.get("default_probability")).intValue() : null))
                .collect(Collectors.toList());
    }

    public List<CrmContactMethodLookupDto> getContactMethods() {
        return repository.getContactMethods(context.getOrgClientId()).stream()
                .map(r -> new CrmContactMethodLookupDto(asStr(r.get("contact_method_id")), asStr(r.get("contact_method_name"))))
                .collect(Collectors.toList());
    }

    private static String asStr(Object v) { return v == null ? null : v.toString(); }

    private static List<CrmLookupItemDto> toLookupItems(List<java.util.Map<String, Object>> rows) {
        return rows.stream()
                .map(r -> new CrmLookupItemDto(
                        r.get("code") != null ? r.get("code").toString() : null,
                        r.get("display_name") != null ? r.get("display_name").toString() : null
                ))
                .collect(Collectors.toList());
    }
}

