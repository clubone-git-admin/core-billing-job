package io.clubone.billing.security;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletResponse;

public final class FilterErrorWriter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FilterErrorWriter() {
  }

  public static void write(HttpServletResponse response, int status, String code, String message)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    MAPPER.writeValue(response.getWriter(), Map.of(
        "error", code,
        "message", message,
        "timestamp", Instant.now().toString()));
  }
}
