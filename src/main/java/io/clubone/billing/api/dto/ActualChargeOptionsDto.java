package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ActualChargeOptionsDto(Boolean regenerateAll, List<UUID> subscriptionInstanceIds) {}
