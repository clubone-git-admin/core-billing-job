package io.clubone.billing.service;

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

    // TODO: Wire from auth/tenant context when available
    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");

    private final CrmLookupRepository repository;

    public CrmLookupService(CrmLookupRepository repository) {
        this.repository = repository;
    }

    public List<CrmLookupItemDto> getLeadStatuses() {
        return toLookupItems(repository.getLeadStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadSources() {
        return toLookupItems(repository.getLeadSources(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadTypes() {
        return toLookupItems(repository.getLeadTypes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getLeadRecordTypes() {
        return toLookupItems(repository.getLeadRecordTypes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getSalutations() {
        return toLookupItems(repository.getSalutations(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getGenders() {
        return toLookupItems(repository.getGenders(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getHomeClubs() {
        return toLookupItems(repository.getHomeClubs());
    }

    public List<CrmLookupItemDto> getUsers() {
        return toLookupItems(repository.getMemberAdvisorUsers(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCountries() {
        return toLookupItems(repository.getCountries());
    }

    public List<CrmLookupItemDto> getStates(String countryCode) {
        return toLookupItems(repository.getStates(countryCode));
    }

    public List<CrmLookupItemDto> getAccountsForLeadRecordType(UUID leadRecordTypeId) {
        return toLookupItems(repository.getAccountsForLeadRecordType(getOrgClientId(), leadRecordTypeId));
    }

    public List<CrmLookupItemDto> getCampaigns() {
        return toLookupItems(repository.getCampaigns(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getContacts() {
        return toLookupItems(repository.getContacts(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityTypes() {
        return toLookupItems(repository.getActivityTypes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityStatuses() {
        return toLookupItems(repository.getActivityStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityOutcomes() {
        return toLookupItems(repository.getActivityOutcomes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getActivityVisibilities() {
        return toLookupItems(repository.getActivityVisibilities(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEntityTypes() {
        return toLookupItems(repository.getEntityTypes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCallDirections() {
        return toLookupItems(repository.getCallDirections(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getCallResults() {
        return toLookupItems(repository.getCallResults(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmailDeliveryStatuses() {
        return toLookupItems(repository.getEmailDeliveryStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getSmsDeliveryStatuses() {
        return toLookupItems(repository.getSmsDeliveryStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getWhatsappDeliveryStatuses() {
        return toLookupItems(repository.getWhatsappDeliveryStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEventStatuses() {
        return toLookupItems(repository.getEventStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEventPurposes() {
        return toLookupItems(repository.getEventPurposes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getRsvpStatuses() {
        return toLookupItems(repository.getRsvpStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskTypes() {
        return toLookupItems(repository.getTaskTypes(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskStatuses() {
        return toLookupItems(repository.getTaskStatuses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getTaskPriorities() {
        return toLookupItems(repository.getTaskPriorities(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmpty() {
        return toLookupItems(repository.getEmptyLookup());
    }

    public List<CrmLookupItemDto> getEmailFromAddresses() {
        return toLookupItems(repository.getEmailFromAddresses(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getEmailTemplates() {
        return toLookupItems(repository.getEmailTemplates(getOrgClientId()));
    }

    public List<CrmLookupItemDto> getContactLifecycles() {
        return toLookupItems(repository.getContactLifecycles(getOrgClientId()));
    }

    public List<CrmCaseTypeLookupDto> getCaseTypes() {
        return repository.getCaseTypes(getOrgClientId()).stream()
                .map(r -> new CrmCaseTypeLookupDto(asStr(r.get("case_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCaseStatusLookupDto> getCaseStatuses() {
        return repository.getCaseStatuses(getOrgClientId()).stream()
                .map(r -> new CrmCaseStatusLookupDto(asStr(r.get("case_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCasePriorityLookupDto> getCasePriorities() {
        return repository.getCasePriorities(getOrgClientId()).stream()
                .map(r -> new CrmCasePriorityLookupDto(asStr(r.get("case_priority_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmAccountTypeLookupDto> getAccountTypes() {
        return repository.getAccountTypes(getOrgClientId()).stream()
                .map(r -> new CrmAccountTypeLookupDto(asStr(r.get("account_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskTypeLookupDto> getTaskTypesFull() {
        return repository.getTaskTypesFull(getOrgClientId()).stream()
                .map(r -> new CrmTaskTypeLookupDto(asStr(r.get("task_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskStatusLookupDto> getTaskStatusesFull() {
        return repository.getTaskStatusesFull(getOrgClientId()).stream()
                .map(r -> new CrmTaskStatusLookupDto(asStr(r.get("task_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmTaskPriorityLookupDto> getTaskPrioritiesFull() {
        return repository.getTaskPrioritiesFull(getOrgClientId()).stream()
                .map(r -> new CrmTaskPriorityLookupDto(asStr(r.get("task_priority_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmEntityTypeLookupDto> getEntityTypesFull() {
        return repository.getEntityTypesFull(getOrgClientId()).stream()
                .map(r -> new CrmEntityTypeLookupDto(asStr(r.get("entity_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCampaignTypeLookupDto> getCampaignTypesFull() {
        return repository.getCampaignTypesFull(getOrgClientId()).stream()
                .map(r -> new CrmCampaignTypeLookupDto(asStr(r.get("campaign_type_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmCampaignStatusLookupDto> getCampaignStatusesFull() {
        return repository.getCampaignStatusesFull(getOrgClientId()).stream()
                .map(r -> new CrmCampaignStatusLookupDto(asStr(r.get("campaign_status_id")), asStr(r.get("code")), asStr(r.get("display_name"))))
                .collect(Collectors.toList());
    }

    public List<CrmOpportunityStageLookupDto> getOpportunityStagesFull() {
        return repository.getOpportunityStagesFull(getOrgClientId()).stream()
                .map(r -> new CrmOpportunityStageLookupDto(
                        asStr(r.get("opportunity_stage_id")),
                        asStr(r.get("code")),
                        asStr(r.get("display_name")),
                        r.get("display_order") != null ? ((Number) r.get("display_order")).intValue() : null,
                        r.get("default_probability") != null ? ((Number) r.get("default_probability")).intValue() : null))
                .collect(Collectors.toList());
    }

    public List<CrmContactMethodLookupDto> getContactMethods() {
        return repository.getContactMethods(getOrgClientId()).stream()
                .map(r -> new CrmContactMethodLookupDto(asStr(r.get("contact_method_id")), asStr(r.get("contact_method_name"))))
                .collect(Collectors.toList());
    }

    private static String asStr(Object v) { return v == null ? null : v.toString(); }

    private UUID getOrgClientId() {
        return DEFAULT_ORG_CLIENT_ID;
    }

    private static List<CrmLookupItemDto> toLookupItems(List<java.util.Map<String, Object>> rows) {
        return rows.stream()
                .map(r -> new CrmLookupItemDto(
                        r.get("code") != null ? r.get("code").toString() : null,
                        r.get("display_name") != null ? r.get("display_name").toString() : null
                ))
                .collect(Collectors.toList());
    }
}

