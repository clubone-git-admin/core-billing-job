package io.clubone.billing.security;

public class UnauthorizedException extends RuntimeException {
  public UnauthorizedException(String msg) { super(msg); }
}
