package io.clubone.billing.service;

import io.clubone.billing.api.dto.billingprofile.*;
import io.clubone.billing.repo.AccessApplicationRepository;
import io.clubone.billing.repo.BillingProfileDefaultRepository;
import io.clubone.billing.repo.BillingProfileLevelOverrideRepository;
import io.clubone.billing.repo.BillingProfileLookupRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Service
public class BillingProfileSettingsService {

    private final AccessApplicationRepository accessApplicationRepository;
    private final BillingProfileLookupRepository billingProfileLookupRepository;
    private final BillingProfileDefaultRepository billingProfileDefaultRepository;
    private final BillingProfileLevelOverrideRepository billingProfileLevelOverrideRepository;

    public BillingProfileSettingsService(
            AccessApplicationRepository accessApplicationRepository,
            BillingProfileLookupRepository billingProfileLookupRepository,
            BillingProfileDefaultRepository billingProfileDefaultRepository,
            BillingProfileLevelOverrideRepository billingProfileLevelOverrideRepository) {
        this.accessApplicationRepository = accessApplicationRepository;
        this.billingProfileLookupRepository = billingProfileLookupRepository;
        this.billingProfileDefaultRepository = billingProfileDefaultRepository;
        this.billingProfileLevelOverrideRepository = billingProfileLevelOverrideRepository;
    }

    public BillingProfileLookupsResponse getBillingProfileLookups(UUID applicationId) {
        requireActiveApplication(applicationId);
        return new BillingProfileLookupsResponse(
                billingProfileLookupRepository.findChargeTriggerTypes(applicationId),
                billingProfileLookupRepository.findChargeEndConditions(applicationId),
                billingProfileLookupRepository.findBillingPeriodUnits(applicationId),
                billingProfileLookupRepository.findBillingTimings(applicationId),
                billingProfileLookupRepository.findSubscriptionBillingDayRules(applicationId),
                billingProfileLookupRepository.findBillingAlignments(applicationId),
                billingProfileLookupRepository.findProrationStrategies(applicationId),
                Collections.emptyList());
    }

    public BillingProfileDefaultDto getBillingProfileDefault(UUID applicationId) {
        requireActiveApplication(applicationId);
        BillingProfileDefaultDto existing = billingProfileDefaultRepository.findByApplicationId(applicationId);
        if (existing != null) {
            return existing;
        }
        return BillingProfileDefaultDto.emptyForApplication(applicationId);
    }

    @Transactional
    public BillingProfileDefaultDto upsertBillingProfileDefault(UUID applicationId,
            UpsertBillingProfileDefaultRequest req, UUID actorId) {
        requireActiveApplication(applicationId);
        if (req.applicationId() != null && !req.applicationId().equals(applicationId)) {
            throw new IllegalArgumentException("applicationId in body must match application-id header");
        }
        validateInterval(req.defaultIntervalCount());
        validateCycleDay(req.defaultAccountCycleDay());
        BillingProfileDefaultDto existing = billingProfileDefaultRepository.findByApplicationId(applicationId);
        if (existing == null) {
            return billingProfileDefaultRepository.insert(applicationId, req, actorId);
        }
        return billingProfileDefaultRepository.update(existing.billingProfileDefaultId(), applicationId, req, actorId);
    }

    /** Overrides for a single hierarchy level. */
    public List<BillingProfileLevelOverrideDto> listLevelOverridesForLevel(UUID applicationId, UUID levelId) {
        requireActiveApplication(applicationId);
        if (levelId == null) {
            throw new IllegalArgumentException("levelId query parameter is required");
        }
        return billingProfileLevelOverrideRepository.findByApplicationAndLevel(applicationId, levelId);
    }

    /** All overrides for the application (no {@code levelId} filter). */
    public List<BillingProfileLevelOverrideDto> listAllLevelOverrides(UUID applicationId) {
        requireActiveApplication(applicationId);
        return billingProfileLevelOverrideRepository.findAllByApplicationId(applicationId);
    }

    /** Distinct level ids that have at least one override (light index for hierarchy UI). */
    public BillingProfileLevelOverridesIndexDto listLevelOverridesIndex(UUID applicationId) {
        requireActiveApplication(applicationId);
        return new BillingProfileLevelOverridesIndexDto(
                billingProfileLevelOverrideRepository.findDistinctLevelIdsByApplicationId(applicationId));
    }

    @Transactional
    public BillingProfileLevelOverrideDto createLevelOverride(UUID applicationId,
            UpsertBillingProfileLevelOverrideRequest req, UUID actorId) {
        requireActiveApplication(applicationId);
        if (req.applicationId() != null && !req.applicationId().equals(applicationId)) {
            throw new IllegalArgumentException("applicationId in body must match application-id header");
        }
        if (req.levelId() == null) {
            throw new IllegalArgumentException("levelId is required");
        }
        if (req.effectiveFrom() == null) {
            throw new IllegalArgumentException("effectiveFrom is required");
        }
        validateInterval(req.intervalCount());
        validateCycleDay(req.accountCycleDay());
        validateEffectiveDates(req.effectiveFrom(), req.effectiveTo());
        return billingProfileLevelOverrideRepository.insert(applicationId, req, actorId);
    }

    @Transactional
    public BillingProfileLevelOverrideDto updateLevelOverride(UUID applicationId, UUID billingProfileLevelOverrideId,
            UpsertBillingProfileLevelOverrideRequest req, UUID actorId) {
        requireActiveApplication(applicationId);
        BillingProfileLevelOverrideDto existing = billingProfileLevelOverrideRepository.findById(
                billingProfileLevelOverrideId, applicationId);
        if (existing == null) {
            return null;
        }
        if (req.applicationId() != null && !req.applicationId().equals(applicationId)) {
            throw new IllegalArgumentException("applicationId in body must match application-id header");
        }
        if (req.effectiveFrom() == null) {
            throw new IllegalArgumentException("effectiveFrom is required");
        }
        if (req.levelId() != null && !req.levelId().equals(existing.levelId())) {
            throw new IllegalArgumentException("levelId must match the override's level (or be omitted)");
        }
        validateInterval(req.intervalCount());
        validateCycleDay(req.accountCycleDay());
        validateEffectiveDates(req.effectiveFrom(), req.effectiveTo());
        return billingProfileLevelOverrideRepository.update(billingProfileLevelOverrideId, applicationId, req, actorId);
    }

    private void requireActiveApplication(UUID applicationId) {
        if (accessApplicationRepository.findOrgClientIdByApplicationId(applicationId) == null) {
            throw new IllegalArgumentException("Application not found or inactive for application-id");
        }
    }

    private static void validateInterval(Integer intervalCount) {
        if (intervalCount != null && intervalCount < 1) {
            throw new IllegalArgumentException("intervalCount must be >= 1 when provided");
        }
    }

    private static void validateCycleDay(Integer day) {
        if (day != null && (day < 1 || day > 31)) {
            throw new IllegalArgumentException("account cycle day must be between 1 and 31 when provided");
        }
    }

    private static void validateEffectiveDates(LocalDate from, LocalDate to) {
        if (from == null) {
            return;
        }
        if (to != null && to.isBefore(from)) {
            throw new IllegalArgumentException("effectiveTo must be on or after effectiveFrom");
        }
    }
}
