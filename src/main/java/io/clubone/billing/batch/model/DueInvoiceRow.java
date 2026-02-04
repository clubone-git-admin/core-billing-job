package io.clubone.billing.batch.model;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

@Data
public class DueInvoiceRow {
  private UUID invoiceId;
  private UUID subscriptionInstanceId;
  private Integer cycleNumber;
  private LocalDate paymentDueDate;
  private UUID clientRoleId;
  private BigDecimal subTotal;
  private BigDecimal taxAmount;
  private BigDecimal discountAmount;
  private BigDecimal totalAmount;
  private UUID clientPaymentMethodId; //
}
