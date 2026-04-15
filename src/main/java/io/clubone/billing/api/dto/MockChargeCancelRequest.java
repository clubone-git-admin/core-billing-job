package io.clubone.billing.api.dto;

import java.util.UUID;

public record MockChargeCancelRequest(
        UUID requestedBy,
        String reason
) {}
