package io.clubone.billing.security;

public class ForbiddenException extends RuntimeException {
  public ForbiddenException(String msg) { super(msg); }
}
