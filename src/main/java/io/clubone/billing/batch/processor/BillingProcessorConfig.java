package io.clubone.billing.batch.processor;

import java.time.LocalDate;
import java.util.UUID;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.DueInvoiceRow;
import io.clubone.billing.batch.payment.PaymentServiceFactory;
import io.clubone.billing.repo.BillingRepository;

@Configuration
public class BillingProcessorConfig {

  @Bean
  @StepScope
  public ItemProcessor<DueInvoiceRow, BillingWorkItem> billingItemProcessor(
      @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc,
      BillingRepository repo,
      BillingJobProperties props,
      PaymentServiceFactory paymentFactory,
      @Value("#{jobExecutionContext['billingRunId']}") String runIdStr,
      @Value("#{jobExecutionContext['runMode']}") String modeStr,
      @Value("#{jobParameters['asOfDate']}") String asOfDateStr) {
    UUID runId = UUID.fromString(runIdStr);
    RunMode mode = RunMode.valueOf(modeStr);
    LocalDate asOfDate = LocalDate.parse(asOfDateStr);
    return new BillingItemProcessor(jdbc, repo, props, paymentFactory.get(mode), runId, mode, asOfDate);
  }
}
