package io.clubone.billing.service;

import io.clubone.billing.repo.PlansRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for subscription plans operations.
 */
@Service
public class PlansService {

    private final PlansRepository plansRepository;
    private final LocationLevelRepository locationLevelRepository;

    public PlansService(
            PlansRepository plansRepository,
            LocationLevelRepository locationLevelRepository) {
        this.plansRepository = plansRepository;
        this.locationLevelRepository = locationLevelRepository;
    }

    public Map<String, Object> listPlans(
            Boolean isActive,
            UUID clientAgreementId,
            UUID locationLevelId,
            Boolean includeChildLocations,
            Integer limit,
            Integer offset) {
        List<UUID> locationIds = resolveLocationIds(locationLevelId, includeChildLocations);
        List<Map<String, Object>> plans =
                plansRepository.findPlans(isActive, clientAgreementId, locationIds, limit, offset);
        Integer total = plansRepository.countPlans(isActive, clientAgreementId, locationIds);

        List<Map<String, Object>> planList = plans.stream()
                .map(p -> {
                    Object id = p.get("subscription_plan_id");
                    if (id instanceof UUID planId) {
                        Map<String, Object> full = getPlan(planId);
                        if (full != null) {
                            return full;
                        }
                    }
                    return formatPlan(p);
                })
                .collect(Collectors.toList());

        return Map.of(
                "data", planList,
                "total", total,
                "limit", limit,
                "offset", offset
        );
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

    public Map<String, Object> getPlan(UUID subscriptionPlanId) {
        Map<String, Object> plan = plansRepository.findById(subscriptionPlanId);
        if (plan == null) {
            return null;
        }

        Map<String, Object> formattedPlan = formatPlan(plan);

        // Add cycle prices
        List<Map<String, Object>> cyclePrices = plansRepository.getCyclePrices(subscriptionPlanId);
        formattedPlan.put("cycle_prices", cyclePrices.stream()
                .map(cp -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("subscription_plan_cycle_price_id", cp.get("subscription_plan_cycle_price_id"));
                    row.put("cycle_start", cp.get("cycle_start"));
                    row.put("cycle_end", cp.get("cycle_end"));
                    row.put("unit_price", cp.get("unit_price"));
                    row.put("effective_unit_price", cp.get("effective_unit_price"));
                    return row;
                })
                .collect(Collectors.toList()));

        // Add entitlements
        List<Map<String, Object>> entitlements = plansRepository.getEntitlements(subscriptionPlanId);
        formattedPlan.put("entitlements", entitlements.stream()
                .map(e -> {
                    Map<String, Object> mode = new HashMap<>();
                    mode.put("code", e.get("entitlement_mode_code"));
                    mode.put("display_name", e.get("entitlement_mode_display_name"));
                    Map<String, Object> row = new HashMap<>();
                    row.put("subscription_plan_entitlement_id", e.get("subscription_plan_entitlement_id"));
                    row.put("entitlement_mode", mode);
                    row.put("quantity_per_cycle", e.get("quantity_per_cycle"));
                    row.put("is_unlimited", e.get("is_unlimited"));
                    return row;
                })
                .collect(Collectors.toList()));

        // Add active instances count
        Integer activeInstancesCount = plansRepository.getActiveInstancesCount(subscriptionPlanId);
        formattedPlan.put("active_instances_count", activeInstancesCount);

        // Add instances (all statuses)
        List<Map<String, Object>> instances = plansRepository.findInstances(subscriptionPlanId, null, 10_000, 0);
        formattedPlan.put("instances", instances.stream().map(this::formatInstance).toList());

        // Add full cycle timeline with paid/upcoming/future bucketing
        List<Map<String, Object>> cycles = plansRepository.getBillingCycles(subscriptionPlanId);
        formattedPlan.put("cycles", cycles.stream().map(this::formatCycle).toList());

        // Add mandate + payment details
        Map<String, Object> mandate = plansRepository.getMandateAndPaymentDetails(subscriptionPlanId);
        formattedPlan.put("payment_and_mandate", mandate == null ? Map.of() : mandate);

        return formattedPlan;
    }

    public Map<String, Object> getPlanInstances(UUID subscriptionPlanId, String statusCode, Integer limit, Integer offset) {
        List<Map<String, Object>> instances = plansRepository.findInstances(subscriptionPlanId, statusCode, limit, offset);
        Integer total = plansRepository.countInstances(subscriptionPlanId, statusCode);

        List<Map<String, Object>> instanceList = instances.stream()
                .map(this::formatInstance)
                .collect(Collectors.toList());

        return Map.of(
                "data", instanceList,
                "total", total,
                "limit", limit,
                "offset", offset
        );
    }

    private Map<String, Object> formatPlan(Map<String, Object> plan) {
        Map<String, Object> result = new HashMap<>();
        result.put("subscription_plan_id", plan.get("subscription_plan_id"));
        result.put("subscription_plan_code", plan.get("subscription_plan_code"));
        result.put("client_payment_method_id", plan.get("client_payment_method_id"));
        result.put("package_item_id", plan.get("package_item_id"));
        result.put("package_plan_template_id", plan.get("package_plan_template_id"));
        result.put("term_total_cycles", plan.get("term_total_cycles"));
        Map<String, Object> freq = new HashMap<>();
        freq.put("frequency_name", plan.get("subscription_frequency_name"));
        freq.put("display_name", plan.get("subscription_frequency_name"));
        result.put("subscription_frequency", freq);
        result.put("interval_count", plan.get("interval_count"));
        Map<String, Object> dayRule = new HashMap<>();
        dayRule.put("billing_day", plan.get("billing_day"));
        dayRule.put("display_name", plan.get("billing_day_display_name"));
        result.put("subscription_billing_day_rule", dayRule);
        result.put("contract_start_date", plan.get("contract_start_date"));
        result.put("contract_end_date", plan.get("contract_end_date"));
        result.put("is_active", plan.get("is_active"));
        result.put("client_agreement_id", plan.get("client_agreement_id"));
        result.put("agreement_term_id", plan.get("agreement_term_id"));
        Map<String, Object> agreementTerm = new HashMap<>();
        agreementTerm.put("agreement_term_id", plan.get("agreement_term_id"));
        agreementTerm.put("duration_value", plan.get("agreement_term_duration_value"));
        agreementTerm.put("duration_unit_code", plan.get("agreement_term_duration_unit_code"));
        agreementTerm.put("duration_unit_name", plan.get("agreement_term_duration_unit_name"));
        result.put("agreement_term", agreementTerm);
        result.put("agreement_name", plan.get("agreement_name"));
        result.put("client_role_id", plan.get("client_role_id"));
        result.put("role_id", plan.get("role_external_id"));
        result.put("location_id", plan.get("location_id"));
        result.put("location_name", plan.get("location_name"));
        result.put("created_on", plan.get("created_on"));
        return result;
    }

    private Map<String, Object> formatCycle(Map<String, Object> cycle) {
        Map<String, Object> out = new HashMap<>();
        out.put("billing_schedule_id", cycle.get("billing_schedule_id"));
        out.put("subscription_instance_id", cycle.get("subscription_instance_id"));
        out.put("cycle_number", cycle.get("cycle_number"));
        out.put("billing_date", cycle.get("billing_date"));
        out.put("schedule_status", cycle.get("schedule_status"));
        out.put("final_amount", cycle.get("final_amount"));
        out.put("invoice_id", cycle.get("invoice_id"));
        out.put("invoice_number", cycle.get("invoice_number"));
        out.put("invoice_status", cycle.get("invoice_status"));
        out.put("invoice_total_amount", cycle.get("invoice_total_amount"));
        out.put("cycle_bucket", cycleBucket(cycle));
        return out;
    }

    private static String cycleBucket(Map<String, Object> cycle) {
        String invoiceStatus = cycle.get("invoice_status") == null
                ? null
                : String.valueOf(cycle.get("invoice_status")).trim().toUpperCase(Locale.ROOT);
        if ("PAID".equals(invoiceStatus)) {
            return "PAID";
        }
        Object bd = cycle.get("billing_date");
        LocalDate billingDate = bd instanceof LocalDate ? (LocalDate) bd : null;
        if (billingDate == null) {
            return "FUTURE";
        }
        LocalDate today = LocalDate.now();
        if (billingDate.isBefore(today) || billingDate.isEqual(today) || billingDate.isBefore(today.plusDays(31))) {
            return "UPCOMING";
        }
        return "FUTURE";
    }

    private Map<String, Object> formatInstance(Map<String, Object> instance) {
        return Map.of(
                "subscription_instance_id", instance.get("subscription_instance_id"),
                "subscription_plan_id", instance.get("subscription_plan_id"),
                "start_date", instance.get("start_date"),
                "next_billing_date", instance.get("next_billing_date"),
                "subscription_instance_status", Map.of(
                        "status_name", instance.get("subscription_instance_status_name"),
                        "display_name", instance.get("subscription_instance_status_name")
                ),
                "current_cycle_number", instance.get("current_cycle_number"),
                "last_billed_on", instance.get("last_billed_on"),
                "end_date", instance.get("end_date")
        );
    }
}

