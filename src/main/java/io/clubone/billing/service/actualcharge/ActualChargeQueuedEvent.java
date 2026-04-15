package io.clubone.billing.service.actualcharge;

import java.util.UUID;

/** Published after commit so async worker and logs can correlate billing run + stage execution. */
public record ActualChargeQueuedEvent(UUID stageRunId, UUID billingRunId) {}
