package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.GatewayMidCurrencyDto;
import io.clubone.billing.api.dto.currency.UpsertGatewayMidCurrencyRequest;
import io.clubone.billing.repo.GatewayMidCurrencyRepository;
import io.clubone.billing.repo.GatewayMidCurrencyRepository.GatewayMidRow;
import io.clubone.billing.security.AccessContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class GatewayMidCurrencyService {

    private final GatewayMidCurrencyRepository repository;

    public GatewayMidCurrencyService(GatewayMidCurrencyRepository repository) {
        this.repository = repository;
    }

    public List<GatewayMidCurrencyDto> list(int limit) {
        return repository.list(limit).stream().map(this::toDto).toList();
    }

    @Transactional
    public GatewayMidCurrencyDto upsert(UpsertGatewayMidCurrencyRequest request) {
        if (request == null
                || request.gatewayCode() == null
                || request.currencyCode() == null
                || request.midCode() == null
                || request.midCode().isBlank()) {
            throw new IllegalArgumentException("gatewayCode, currencyCode and midCode are required");
        }
        String ccy = request.currencyCode().trim().toUpperCase();
        if (ccy.length() != 3) {
            throw new IllegalArgumentException("currencyCode must be ISO-4217 (3 letters)");
        }
        UUID id = repository.upsert(
                request.gatewayCode(),
                ccy,
                request.midCode(),
                request.locationId(),
                request.notes(),
                AccessContext.actorUserId());
        return repository.findById(id)
                .map(this::toDto)
                .orElseGet(() -> new GatewayMidCurrencyDto(
                        id,
                        request.gatewayCode().trim().toUpperCase(),
                        ccy,
                        request.midCode().trim(),
                        request.locationId(),
                        true,
                        request.notes()));
    }

    @Transactional
    public void deactivate(UUID id) {
        repository.deactivate(id, AccessContext.actorUserId());
    }

    public Optional<String> resolveMidForPayment(UUID clientPaymentMethodId, String currencyCode, UUID locationId) {
        Optional<String> gw = repository.findGatewayCodeForPaymentMethod(clientPaymentMethodId);
        if (gw.isEmpty()) {
            return Optional.empty();
        }
        return repository.resolveMid(gw.get(), currencyCode, locationId);
    }

    /**
     * Resolve MID preferring location-scoped config for the client's home club, then currency-only.
     */
    public Optional<String> resolveMidForPayment(UUID clientPaymentMethodId, String currencyCode, UUID locationIdHint,
            UUID clientRoleId) {
        UUID locationId = locationIdHint;
        if (locationId == null && clientRoleId != null) {
            locationId = repository.findLocationIdForClientRole(clientRoleId).orElse(null);
        }
        return resolveMidForPayment(clientPaymentMethodId, currencyCode, locationId);
    }

    private GatewayMidCurrencyDto toDto(GatewayMidRow row) {
        return new GatewayMidCurrencyDto(
                row.gatewayMidCurrencyId(),
                row.gatewayCode(),
                row.currencyCode(),
                row.midCode(),
                row.locationId(),
                row.active(),
                row.notes());
    }
}
