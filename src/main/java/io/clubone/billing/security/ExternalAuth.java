package io.clubone.billing.security;

/** External API Gateway partner principal markers / headers. */
public final class ExternalAuth {

  public static final String REQUEST_ATTR = "clubone.externalAuth";
  public static final String HEADER_CLIENT_ID = "X-External-Client-Id";
  public static final String HEADER_SCOPES = "X-External-Scopes";
  public static final String HEADER_APPLICATION_ID = "X-External-Application-Id";
  public static final String HEADER_APPLICATION_ID_ALT = "application-id";

  public static final java.util.UUID SYNTHETIC_NIL =
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000000");

  private ExternalAuth() {}
}
