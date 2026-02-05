package io.clubone.billing.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clubone.billing.batch.audit.AuditService;
import io.clubone.billing.batch.metrics.BillingMetrics;
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
    ObjectMapper mapper = new ObjectMapper();
    // Register JavaTimeModule to handle Java 8 date/time types (LocalDate, LocalDateTime, etc.)
    mapper.registerModule(new JavaTimeModule());
    // Disable writing dates as timestamps (use ISO-8601 strings instead)
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }

  // -----------------------------------------------------------------------
  // Step 1: create billing_run and expose billingRunId + runMode in context
  // -----------------------------------------------------------------------

  @Bean("startRunStep")
  public Step startRunStep(JobRepository jobRepository,
                           PlatformTransactionManager txManager,
                           BillingRepository repo,
                           AuditService auditService) {
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
          
          // Audit: Job execution started
          auditService.logJobStarted(runId, mode.name(), asOfDateStr, "system");
          
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
                                     BillingJobProperties props,
                                     org.springframework.batch.item.ItemReader<DueInvoiceRow> dueInvoiceReader,
                                     org.springframework.batch.item.ItemProcessor<DueInvoiceRow, BillingWorkItem> billingItemProcessor,
                                     org.springframework.batch.item.ItemWriter<BillingWorkItem> billingItemWriter,
                                     io.clubone.billing.batch.listener.BillingStepExecutionListener stepListener,
                                     io.clubone.billing.batch.listener.BillingItemReadListener readListener,
                                     io.clubone.billing.batch.listener.BillingSkipListener skipListener) {
    return new StepBuilder("processDueInvoicesStep", jobRepository)
        .<DueInvoiceRow, BillingWorkItem>chunk(props.getChunkSize(), txManager)
        .reader(dueInvoiceReader)
        .listener(readListener)
        .processor(billingItemProcessor)
        .writer(billingItemWriter)
        .listener(stepListener)
        .faultTolerant()
        .skip(Exception.class)
        .skipLimit(10000)
        .listener(skipListener)  // Add skip listener to capture processor/writer failures in DLQ
        .build();
  }

  // -----------------------------------------------------------------------
  // Step 3: close billing_run with a summary
  // -----------------------------------------------------------------------

  @Bean("endRunStep")
  public Step endRunStep(JobRepository jobRepository,
                         PlatformTransactionManager txManager,
                         BillingRepository repo,
                         ObjectMapper mapper,
                         BillingMetrics metrics,
                         AuditService auditService) {
    return new StepBuilder("endRunStep", jobRepository)
        .tasklet((contribution, chunkContext) -> {
          JobExecution je = chunkContext.getStepContext().getStepExecution().getJobExecution();
          UUID runId = UUID.fromString(je.getExecutionContext().getString("billingRunId"));
          String runMode = je.getExecutionContext().getString("runMode");

          Map<String, Object> summary = new LinkedHashMap<>();
          summary.put("countsByStatus", repo.countsByStatus(runId));
          String summaryJson = mapper.writeValueAsString(summary);

          repo.completeRun(runId, io.clubone.billing.batch.model.BillingRunStatus.COMPLETED.getCode(), summaryJson);
          log.info("Completed billing run: runId={} summary={}", runId, summaryJson);
          
          // Audit: Job execution completed
          auditService.logJobCompleted(runId, summary, "system");
          
          return RepeatStatus.FINISHED;
        }, txManager)
        .build();
  }
}
