package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import io.clubone.billing.repo.LocationLevelRepository.LocationRow;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolves which locations are in scope for a billing run and which are blocked by other active runs.
 */
@Service
public class BillingRunScopeService {

    public static final String REASON_RUN_ALREADY_EXISTS = "RUN_ALREADY_EXISTS";
    public static final String EXCLUDED_MANUAL = "EXCLUDED_MANUAL";

    private final LocationLevelRepository locationLevelRepository;
    private final BillingRunRepository billingRunRepository;

    public BillingRunScopeService(
            LocationLevelRepository locationLevelRepository,
            BillingRunRepository billingRunRepository) {
        this.locationLevelRepository = locationLevelRepository;
        this.billingRunRepository = billingRunRepository;
    }

    public ScopePreviewResponse scopePreview(ScopePreviewRequest request) {
        if (request.isUseInclusion()) {
            InclusionBranch b = resolveInclusionBranch(request.inclusionScopes());
            return buildPreviewResponse(request.dueDate(), b);
        }
        List<LocationRow> branch = resolveBranchLocations(request);
        return buildPreviewResponseFromBranch(request.dueDate(), branch);
    }

    public ScopeResolutionResult resolveForCreate(CreateBillingRunRequest request) {
        if (request.isUseInclusionScopes()) {
            InclusionBranch b = resolveInclusionBranch(request.inclusionScopes());
            return buildResolution(request.dueDate(), request.excludedLocationIds(), b.branch(), b.summaries());
        }
        List<LocationRow> branch = resolveBranchForCreate(request);
        return buildResolution(request.dueDate(), request.excludedLocationIds(), branch, null);
    }

    private List<LocationRow> resolveBranchForCreate(CreateBillingRunRequest request) {
        if (request.locationLevelId() != null) {
            return locationLevelRepository.resolveLocationsForLevel(
                    request.locationLevelId(), request.includeChildLevels());
        }
        if (request.locationId() != null) {
            List<LocationRow> rows = locationLevelRepository.findLocationsByIds(List.of(request.locationId()));
            if (rows.isEmpty()) {
                return List.of();
            }
            if (!request.includeChildLevels()) {
                return rows;
            }
            return locationLevelRepository.resolveLocationsForLevel(
                    findLevelIdForLocation(request.locationId(), request.applicationId())
                            .orElseThrow(
                                    () -> new IllegalArgumentException(
                                            "No locations.levels row for location and includeChildLocations=true")),
                    true);
        }
        if (request.applicationId() == null) {
            throw new IllegalArgumentException("applicationId is required when locationLevelId is omitted");
        }
        return locationLevelRepository.listAllLocationIdsForApplication(request.applicationId()).stream()
                .map(id -> {
                    var rows = locationLevelRepository.findLocationsByIds(List.of(id));
                    return rows.isEmpty() ? null : rows.get(0);
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(LocationRow::displayName, Comparator.nullsLast(String::compareTo)))
                .toList();
    }

    private Optional<UUID> findLevelIdForLocation(UUID locationId, UUID applicationId) {
        if (locationId == null) {
            return Optional.empty();
        }
        if (applicationId == null) {
            throw new IllegalArgumentException("applicationId is required when resolving child locations from locationId");
        }
        return locationLevelRepository.findLevelIdByReferenceLocation(locationId, applicationId);
    }

    private List<InclusionScopeDto> deduplicateInclusion(List<InclusionScopeDto> raw) {
        Map<UUID, Boolean> merged = new LinkedHashMap<>();
        for (InclusionScopeDto s : raw) {
            if (s == null || s.locationLevelId() == null) {
                throw new IllegalArgumentException("inclusion scope items must have locationLevelId");
            }
            merged.merge(s.locationLevelId(), s.includeChildLevels(), (a, b) -> a || b);
        }
        return merged.entrySet().stream()
                .map(e -> new InclusionScopeDto(e.getKey(), e.getValue() ? true : null))
                .toList();
    }

    private InclusionBranch resolveInclusionBranch(List<InclusionScopeDto> raw) {
        if (raw == null || raw.isEmpty()) {
            return new InclusionBranch(List.of(), List.of());
        }
        List<InclusionScopeDto> deduped = deduplicateInclusion(raw);
        Map<UUID, String> union = new LinkedHashMap<>();
        List<InclusionScopeSummaryDto> summaries = new ArrayList<>();
        for (InclusionScopeDto s : deduped) {
            List<LocationRow> rows = locationLevelRepository.resolveLocationsForLevel(
                    s.locationLevelId(), s.includeChildLevels());
            if (rows.isEmpty()) {
                throw new IllegalArgumentException(
                        "Inclusion scope for locationLevelId " + s.locationLevelId() + " resolved to no billable sites");
            }
            String lname = locationLevelRepository.findLevelName(s.locationLevelId()).orElse(null);
            summaries.add(InclusionScopeSummaryDto.fromInclusion(s, lname, rows.size()));
            for (LocationRow r : rows) {
                union.putIfAbsent(r.locationId(), r.displayName());
            }
        }
        List<LocationRow> branch = union.entrySet().stream()
                .map(e -> new LocationRow(e.getKey(), e.getValue()))
                .sorted(Comparator.comparing(LocationRow::displayName, Comparator.nullsLast(String::compareTo)))
                .toList();
        return new InclusionBranch(branch, List.copyOf(summaries));
    }

    private List<LocationRow> resolveBranchLocations(ScopePreviewRequest request) {
        if (request.locationLevelId() != null) {
            return locationLevelRepository.resolveLocationsForLevel(
                    request.locationLevelId(), request.includeChildLevels());
        }
        if (request.applicationId() == null) {
            throw new IllegalArgumentException("applicationId is required when locationLevelId is null");
        }
        return locationLevelRepository.listAllLocationIdsForApplication(request.applicationId()).stream()
                .map(id -> {
                    var rows = locationLevelRepository.findLocationsByIds(List.of(id));
                    return rows.isEmpty() ? null : rows.get(0);
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(LocationRow::displayName, Comparator.nullsLast(String::compareTo)))
                .toList();
    }

    private ScopePreviewResponse buildPreviewResponse(LocalDate dueDate, InclusionBranch b) {
        var res = buildResolution(dueDate, null, b.branch(), b.summaries());
        return toPreviewResponse(res);
    }

    private ScopePreviewResponse buildPreviewResponseFromBranch(LocalDate dueDate, List<LocationRow> branch) {
        var res = buildResolution(dueDate, null, branch, null);
        return toPreviewResponse(res);
    }

    private static ScopePreviewResponse toPreviewResponse(ScopeResolutionResult res) {
        List<ScopeLocationItemDto> inc = res.included().stream()
                .map(l -> new ScopeLocationItemDto(l.locationId(), l.displayName()))
                .toList();
        return new ScopePreviewResponse(
                res.selectedBranchTotal(),
                inc,
                res.excluded(),
                res.scopeSummary() != null ? res.scopeSummary().inclusionScopeSummaries() : null);
    }

    public ScopeResolutionResult buildResolution(
            LocalDate dueDate, List<UUID> manualExcludes, List<LocationRow> branch,
            List<InclusionScopeSummaryDto> inclusionScopeSummaries) {
        Set<UUID> manual = manualExcludes == null
                ? Set.of()
                : manualExcludes.stream().filter(Objects::nonNull).collect(Collectors.toSet());
        int branchTotal = branch.size();
        List<ScopeLocationExclusionDto> excluded = new ArrayList<>();

        for (UUID ex : manual) {
            String name = resolveDisplayName(ex, branch);
            excluded.add(new ScopeLocationExclusionDto(ex, name, EXCLUDED_MANUAL, null));
        }

        List<LocationRow> afterManual = branch.stream()
                .filter(r -> !manual.contains(r.locationId()))
                .collect(Collectors.toCollection(ArrayList::new));

        var global = billingRunRepository.findActiveGlobalRunForDueDate(dueDate);
        if (global.isPresent()) {
            for (LocationRow r : afterManual) {
                excluded.add(new ScopeLocationExclusionDto(
                        r.locationId(), r.displayName(), REASON_RUN_ALREADY_EXISTS, global.get()));
            }
            afterManual.clear();
        } else {
            Map<UUID, UUID> blockedBy = findLocationIdsBlockedByInFlightRuns(
                    dueDate, afterManual.stream().map(LocationRow::locationId).toList());
            Iterator<LocationRow> it = afterManual.iterator();
            while (it.hasNext()) {
                LocationRow r = it.next();
                UUID blocker = blockedBy.get(r.locationId());
                if (blocker != null) {
                    excluded.add(
                            new ScopeLocationExclusionDto(
                                    r.locationId(), r.displayName(), REASON_RUN_ALREADY_EXISTS, blocker));
                    it.remove();
                }
            }
        }

        BillingScopeSummaryDto summary;
        if (inclusionScopeSummaries != null && !inclusionScopeSummaries.isEmpty()) {
            summary = new BillingScopeSummaryDto(
                    branchTotal, afterManual.size(), excluded.size(), excluded, inclusionScopeSummaries);
        } else {
            summary = BillingScopeSummaryDto.ofBasics(branchTotal, afterManual.size(), excluded.size(), excluded);
        }
        return new ScopeResolutionResult(branchTotal, afterManual, excluded, summary);
    }

    private Map<UUID, UUID> findLocationIdsBlockedByInFlightRuns(LocalDate dueDate, List<UUID> candidateLocationIds) {
        if (candidateLocationIds == null || candidateLocationIds.isEmpty()) {
            return Map.of();
        }
        Set<UUID> want = new HashSet<>(candidateLocationIds);
        List<BillingRunRepository.ActiveNonGlobalRunForDate> runs =
                billingRunRepository.listActiveNonGlobalRunsForDueDateOrdered(dueDate);
        Map<UUID, UUID> blocked = new LinkedHashMap<>();
        for (BillingRunRepository.ActiveNonGlobalRunForDate run : runs) {
            Set<UUID> reserved = new LinkedHashSet<>();
            for (UUID loc : billingRunRepository.findLocationIdsForRunScope(run.billingRunId())) {
                if (loc != null) {
                    reserved.add(loc);
                }
            }
            if (run.locationLevelId() != null) {
                for (LocationRow lr : locationLevelRepository.resolveLocationsForLevel(
                        run.locationLevelId(), run.includeChildLevelsForResolution())) {
                    if (lr.locationId() != null) {
                        reserved.add(lr.locationId());
                    }
                }
            }
            for (UUID loc : reserved) {
                if (want.contains(loc)) {
                    blocked.putIfAbsent(loc, run.billingRunId());
                }
            }
        }
        return blocked;
    }

    private String resolveDisplayName(UUID locationId, List<LocationRow> branch) {
        return branch.stream()
                .filter(r -> r.locationId().equals(locationId))
                .map(LocationRow::displayName)
                .findFirst()
                .orElse(
                        locationLevelRepository.findLocationsByIds(List.of(locationId)).stream()
                                .findFirst()
                                .map(LocationRow::displayName)
                                .orElse(null));
    }

    private record InclusionBranch(
            List<LocationRow> branch, List<InclusionScopeSummaryDto> summaries) {
    }

    public record ScopeResolutionResult(
            int selectedBranchTotal,
            List<LocationRow> included,
            List<ScopeLocationExclusionDto> excluded,
            BillingScopeSummaryDto scopeSummary) {
    }
}
