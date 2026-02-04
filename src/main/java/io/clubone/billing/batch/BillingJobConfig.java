package io.clubone.billing.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.DueInvoiceRow;
import io.clubone.billing.repo.BillingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Configuration
public class BillingJobConfig {

	private static final Logger log = LoggerFactory.getLogger(BillingJobConfig.class);

	public static final String JOB_NAME = "billingJob";

  // -----------------------------------------------------------------------
  // Job definition
  // -----------------------------------------------------------------------

  @Bean
  public Job billingJob(JobRepository jobRepository,
                        @Qualifier("startRunStep") Step startRunStep,
                        @Qualifier("processDueInvoicesStep") Step processDueInvoicesStep,
                        @Qualifier("endRunStep") Step endRunStep) {
    return new JobBuilder(JOB_NAME, jobRepository)
        .incrementer(new RunIdIncrementer())
        .start(startRunStep)
        .next(processDueInvoicesStep)
        .next(endRunStep)
        .build();
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  // -----------------------------------------------------------------------
  // Step 1: create billing_run and expose billingRunId + runMode in context
  // -----------------------------------------------------------------------

  @Bean("startRunStep")
  public Step startRunStep(JobRepository jobRepository,
                           PlatformTransactionManager txManager,
                           BillingRepository repo) {
    return new StepBuilder("startRunStep", jobRepository)
        .tasklet((contribution, chunkContext) -> {
          String asOfDateStr = (String) chunkContext.getStepContext().getJobParameters().get("asOfDate");
          String runModeStr = (String) chunkContext.getStepContext().getJobParameters().get("runMode");
          if (runModeStr == null) runModeStr = "MOCK";

          LocalDate asOfDate = LocalDate.parse(asOfDateStr);
          RunMode mode = RunMode.valueOf(runModeStr.toUpperCase(Locale.ROOT));

          UUID runId = repo.createRun(mode.name(), asOfDate);
          log.info("Created billing run: runId={} mode={} asOfDate={}", runId, mode, asOfDate);

          JobExecution je = chunkContext.getStepContext().getStepExecution().getJobExecution();
          je.getExecutionContext().putString("billingRunId", runId.toString());
          je.getExecutionContext().putString("runMode", mode.name());
          return RepeatStatus.FINISHED;
        }, txManager)
        .build();
  }

  // -----------------------------------------------------------------------
  // Step 2: chunk process due invoices (reader → processor → writer)
  // -----------------------------------------------------------------------

  @Bean("processDueInvoicesStep")
  public Step processDueInvoicesStep(JobRepository jobRepository,
                                     PlatformTransactionManager txManager,
                                     org.springframework.batch.item.ItemReader<DueInvoiceRow> dueInvoiceReader,
                                     org.springframework.batch.item.ItemProcessor<DueInvoiceRow, BillingWorkItem> billingItemProcessor,
                                     org.springframework.batch.item.ItemWriter<BillingWorkItem> billingItemWriter) {
    return new StepBuilder("processDueInvoicesStep", jobRepository)
        .<DueInvoiceRow, BillingWorkItem>chunk(300, txManager)
        .reader(dueInvoiceReader)
        .processor(billingItemProcessor)
        .writer(billingItemWriter)
        .faultTolerant()
        .skip(Exception.class)
        .skipLimit(10000)
        .build();
  }

  // -----------------------------------------------------------------------
  // Step 3: close billing_run with a summary
  // -----------------------------------------------------------------------

  @Bean("endRunStep")
  public Step endRunStep(JobRepository jobRepository,
                         PlatformTransactionManager txManager,
                         BillingRepository repo,
                         ObjectMapper mapper) {
    return new StepBuilder("endRunStep", jobRepository)
        .tasklet((contribution, chunkContext) -> {
          JobExecution je = chunkContext.getStepContext().getStepExecution().getJobExecution();
          UUID runId = UUID.fromString(je.getExecutionContext().getString("billingRunId"));

          Map<String, Object> summary = new LinkedHashMap<>();
          summary.put("countsByStatus", repo.countsByStatus(runId));
          String summaryJson = mapper.writeValueAsString(summary);

          repo.completeRun(runId, "COMPLETED", summaryJson);
          log.info("Completed billing run: runId={} summary={}", runId, summaryJson);
          return RepeatStatus.FINISHED;
        }, txManager)
        .build();
  }
}
