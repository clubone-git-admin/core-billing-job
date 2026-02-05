package io.clubone.billing.batch.schedule;

import io.clubone.billing.batch.RunMode;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Quartz-based job scheduler for automated billing runs.
 * Supports cron-based scheduling for daily/weekly/monthly billing cycles.
 */
@Configuration
@ConditionalOnProperty(name = "clubone.billing.scheduling.enabled", havingValue = "true", matchIfMissing = false)
public class BillingJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(BillingJobScheduler.class);

    /**
     * Scheduled job that runs daily at 2 AM to process due invoices.
     * Configure via: clubone.billing.scheduling.cron
     */
    @Bean
    public JobDetail billingJobDetail() {
        return JobBuilder.newJob(BillingQuartzJob.class)
            .withIdentity("billingJob", "billingGroup")
            .storeDurably()
            .build();
    }

    @Bean
    public Trigger billingJobTrigger() {
        // Default: Run daily at 2 AM
        String cronExpression = System.getProperty("clubone.billing.scheduling.cron", 
            "0 0 2 * * ?"); // Daily at 2 AM

        return TriggerBuilder.newTrigger()
            .forJob(billingJobDetail())
            .withIdentity("billingTrigger", "billingGroup")
            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
            .build();
    }

    /**
     * Quartz job implementation that launches Spring Batch job.
     */
    public static class BillingQuartzJob extends QuartzJobBean {

        private JobLauncher jobLauncher;
        private Job billingJob;

        @Override
        protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
            try {
                LocalDate asOfDate = LocalDate.now();
                RunMode mode = RunMode.LIVE; // Default to LIVE for scheduled runs

                JobParameters params = new JobParametersBuilder()
                    .addString("asOfDate", asOfDate.toString())
                    .addString("runMode", mode.name())
                    .addString("scheduled", "true")
                    .addString("scheduleId", context.getTrigger().getKey().getName())
                    .addLong("ts", System.currentTimeMillis())
                    .toJobParameters();

                log.info("Scheduled billing job triggered: asOfDate={} mode={} scheduleId={}", 
                    asOfDate, mode, context.getTrigger().getKey().getName());

                jobLauncher.run(billingJob, params);

            } catch (Exception e) {
                log.error("Failed to execute scheduled billing job", e);
                throw new JobExecutionException("Failed to execute scheduled billing job", e);
            }
        }

        public void setJobLauncher(JobLauncher jobLauncher) {
            this.jobLauncher = jobLauncher;
        }

        public void setBillingJob(Job billingJob) {
            this.billingJob = billingJob;
        }
    }
}
