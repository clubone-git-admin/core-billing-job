package io.clubone.billing.api.dto.acquisition;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record AcqSessionListResponse(
        @JsonProperty("sessions") List<AcqSessionDetailDto> sessions,
        @JsonProperty("total") long total
) {}
