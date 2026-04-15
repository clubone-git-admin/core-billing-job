package io.clubone.billing.api.dto;

import java.util.UUID;

public record ActualChargeCancelRequest(UUID requestedBy, String reason) {}
