package io.clubone.billing.batch.model;

import io.clubone.billing.batch.RunMode;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

@Data
public class BillingWorkItem {
  private UUID billingRunId;
  private RunMode runMode;
  private UUID invoiceId;
  private UUID subscriptionInstanceId;
  private Integer cycleNumber;
  private LocalDate paymentDueDate;
  private boolean isMock;
  private String historyStatusCode;
  private String failureReason;
  private BigDecimal invoiceSubTotal;
  private BigDecimal invoiceTaxAmount;
  private BigDecimal invoiceDiscountAmount;
  private BigDecimal invoiceTotalAmount;
  private boolean shouldUpdateSchedule;
  private String scheduleNewStatus;
  private boolean shouldUpdateInvoice;
  private boolean invoiceMarkPaid;
  private String paymentGatewayRef;
  private UUID clientRoleId;                 // ✅ store for logging if needed
  private UUID clientPaymentMethodId;        // ✅ for traceability

  private UUID clientPaymentIntentId;        // ✅ from create intent
  private UUID clientPaymentTransactionId;   // ✅ from charge-at-will
  private UUID transactionId;                // ✅ from finalize
  private String paymentStatus;              // ✅ CAPTURED / FAILED / etc (optional)

}
