package io.clubone.billing.batch.payment;

import org.springframework.stereotype.Component;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.RunMode;

@Component
public class PaymentServiceFactory {

  private final BillingJobProperties props;
  private final NoopPaymentService noop;
  private final HttpPaymentService http;

  public PaymentServiceFactory(BillingJobProperties props, NoopPaymentService noop, HttpPaymentService http) {
    this.props = props;
    this.noop = noop;
    this.http = http;
  } 

  public PaymentService get(RunMode mode) {
	  if (mode == RunMode.MOCK) return noop;      // always noop in MOCK
	  return http;                                 // always http in LIVE
	}

}
