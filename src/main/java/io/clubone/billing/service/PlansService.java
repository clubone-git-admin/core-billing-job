package io.clubone.billing.service;

import io.clubone.billing.repo.PlansRepository;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for subscription plans operations.
 */
@Service
public class PlansService {

    private final PlansRepository plansRepository;

    public PlansService(PlansRepository plansRepository) {
        this.plansRepository = plansRepository;
    }

    public Map<String, Object> listPlans(Boolean isActive, UUID clientAgreementId, Integer limit, Integer offset) {
        List<Map<String, Object>> plans = plansRepository.findPlans(isActive, clientAgreementId, limit, offset);
        Integer total = plansRepository.countPlans(isActive, clientAgreementId);

        List<Map<String, Object>> planList = plans.stream()
                .map(this::formatPlan)
                .collect(Collectors.toList());

        return Map.of(
                "data", planList,
                "total", total,
                "limit", limit,
                "offset", offset
        );
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
        result.put("created_on", plan.get("created_on"));
        return result;
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

