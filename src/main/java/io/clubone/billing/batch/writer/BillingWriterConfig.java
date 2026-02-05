package io.clubone.billing.batch.writer;

import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.repo.BillingRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class BillingWriterConfig {

  @Bean
  public ItemWriter<BillingWorkItem> billingItemWriter(
      @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc,
      BillingRepository repo,
      DeadLetterQueueService dlqService,
      BillingMetrics metrics) {
    return new BillingItemWriter(jdbc, repo, dlqService, metrics);
  }
}
