package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Generic paginated response DTO.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PageResponse<T>(
    List<T> data,
    Integer total,
    Integer limit,
    Integer offset,
    Boolean hasMore
) {
    public static <T> PageResponse<T> of(List<T> data, Integer total, Integer limit, Integer offset) {
        boolean hasMore = offset + limit < total;
        return new PageResponse<>(data, total, limit, offset, hasMore);
    }
}
