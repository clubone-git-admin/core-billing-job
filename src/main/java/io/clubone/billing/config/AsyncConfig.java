package io.clubone.billing.config;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import io.clubone.billing.security.TenantContext;
import io.clubone.billing.security.TenantContexts;

/**
 * Bounded async pool for invoice-gen / mock-charge / actual-charge jobs.
 * Prevents unbounded thread growth (Spring default) that contributed to OOM under load.
 * Propagates {@link TenantContext} onto worker threads when present at submit time.
 */
@Configuration
public class AsyncConfig implements AsyncConfigurer {

  private static final Logger log = LoggerFactory.getLogger(AsyncConfig.class);

  @Value("${clubone.billing.async.core-pool-size:2}")
  private int corePoolSize;

  @Value("${clubone.billing.async.max-pool-size:4}")
  private int maxPoolSize;

  @Value("${clubone.billing.async.queue-capacity:8}")
  private int queueCapacity;

  @Value("${clubone.billing.read.core-pool-size:4}")
  private int readCorePoolSize;

  @Value("${clubone.billing.read.max-pool-size:8}")
  private int readMaxPoolSize;

  @Value("${clubone.billing.read.queue-capacity:64}")
  private int readQueueCapacity;

  @Value("${clubone.billing.invoice-generation.async.core-pool-size:2}")
  private int igCorePoolSize;

  @Value("${clubone.billing.invoice-generation.async.max-pool-size:4}")
  private int igMaxPoolSize;

  @Value("${clubone.billing.invoice-generation.async.queue-capacity:32}")
  private int igQueueCapacity;

  @Value("${clubone.billing.due-preview.async.core-pool-size:1}")
  private int duePreviewCorePoolSize;

  @Value("${clubone.billing.due-preview.async.max-pool-size:2}")
  private int duePreviewMaxPoolSize;

  @Value("${clubone.billing.due-preview.async.queue-capacity:16}")
  private int duePreviewQueueCapacity;

  @Value("${clubone.billing.mock-charge.async.core-pool-size:2}")
  private int mockChargeCorePoolSize;

  @Value("${clubone.billing.mock-charge.async.max-pool-size:4}")
  private int mockChargeMaxPoolSize;

  @Value("${clubone.billing.mock-charge.async.queue-capacity:32}")
  private int mockChargeQueueCapacity;

  @Value("${clubone.billing.actual-charge.async.core-pool-size:2}")
  private int actualChargeCorePoolSize;

  @Value("${clubone.billing.actual-charge.async.max-pool-size:4}")
  private int actualChargeMaxPoolSize;

  @Value("${clubone.billing.actual-charge.async.queue-capacity:32}")
  private int actualChargeQueueCapacity;

  @Bean(name = "billingAsyncExecutor")
  public ThreadPoolTaskExecutor billingAsyncExecutor() {
    ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
    ex.setThreadNamePrefix("billing-async-");
    ex.setCorePoolSize(Math.max(1, corePoolSize));
    ex.setMaxPoolSize(Math.max(corePoolSize, maxPoolSize));
    ex.setQueueCapacity(Math.max(1, queueCapacity));
    ex.setKeepAliveSeconds(60);
    ex.setAllowCoreThreadTimeOut(true);
    ex.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    ex.setWaitForTasksToCompleteOnShutdown(true);
    ex.setAwaitTerminationSeconds(120);
    ex.setTaskDecorator(tenantContextTaskDecorator());
    ex.initialize();
    log.info(
        "billingAsyncExecutor core={} max={} queue={}",
        ex.getCorePoolSize(),
        ex.getMaxPoolSize(),
        queueCapacity);
    return ex;
  }

  /**
   * Dedicated pool for invoice-generation workers so long 10k–50k jobs do not starve mock/actual charge.
   */
  @Bean(name = "invoiceGenerationAsyncExecutor")
  public ThreadPoolTaskExecutor invoiceGenerationAsyncExecutor() {
    return buildStageExecutor(
        "ig-async-", igCorePoolSize, igMaxPoolSize, igQueueCapacity, 300, "invoiceGenerationAsyncExecutor");
  }

  @Bean(name = "duePreviewAsyncExecutor")
  public ThreadPoolTaskExecutor duePreviewAsyncExecutor() {
    return buildStageExecutor(
        "due-preview-async-",
        duePreviewCorePoolSize,
        duePreviewMaxPoolSize,
        duePreviewQueueCapacity,
        300,
        "duePreviewAsyncExecutor");
  }

  @Bean(name = "mockChargeAsyncExecutor")
  public ThreadPoolTaskExecutor mockChargeAsyncExecutor() {
    return buildStageExecutor(
        "mock-charge-async-",
        mockChargeCorePoolSize,
        mockChargeMaxPoolSize,
        mockChargeQueueCapacity,
        300,
        "mockChargeAsyncExecutor");
  }

  @Bean(name = "actualChargeAsyncExecutor")
  public ThreadPoolTaskExecutor actualChargeAsyncExecutor() {
    return buildStageExecutor(
        "actual-charge-async-",
        actualChargeCorePoolSize,
        actualChargeMaxPoolSize,
        actualChargeQueueCapacity,
        600,
        "actualChargeAsyncExecutor");
  }

  private ThreadPoolTaskExecutor buildStageExecutor(
      String prefix, int core, int max, int queue, int awaitSeconds, String logName) {
    ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
    ex.setThreadNamePrefix(prefix);
    ex.setCorePoolSize(Math.max(1, core));
    ex.setMaxPoolSize(Math.max(core, max));
    ex.setQueueCapacity(Math.max(1, queue));
    ex.setKeepAliveSeconds(120);
    ex.setAllowCoreThreadTimeOut(true);
    ex.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    ex.setWaitForTasksToCompleteOnShutdown(true);
    ex.setAwaitTerminationSeconds(Math.max(60, awaitSeconds));
    ex.setTaskDecorator(tenantContextTaskDecorator());
    ex.initialize();
    log.info("{} core={} max={} queue={}", logName, ex.getCorePoolSize(), ex.getMaxPoolSize(), queue);
    return ex;
  }

  /**
   * Bounded pool for independent REST read fan-out (list+count, dashboard slices, get-run enrich).
   * Sized under Hikari headroom so Tomcat request threads don't starve the pool.
   */
  @Bean(name = "billingReadExecutor")
  public ThreadPoolTaskExecutor billingReadExecutor() {
    ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
    ex.setThreadNamePrefix("billing-read-");
    ex.setCorePoolSize(Math.max(2, readCorePoolSize));
    ex.setMaxPoolSize(Math.max(readCorePoolSize, readMaxPoolSize));
    ex.setQueueCapacity(Math.max(8, readQueueCapacity));
    ex.setKeepAliveSeconds(60);
    ex.setAllowCoreThreadTimeOut(true);
    ex.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    ex.setWaitForTasksToCompleteOnShutdown(true);
    ex.setAwaitTerminationSeconds(60);
    ex.setTaskDecorator(tenantContextTaskDecorator());
    ex.initialize();
    log.info(
        "billingReadExecutor core={} max={} queue={}",
        ex.getCorePoolSize(),
        ex.getMaxPoolSize(),
        readQueueCapacity);
    return ex;
  }

  /**
   * Capture request {@link TenantContext} when the async task is submitted (still on the
   * publishing thread) and restore it on the worker — ThreadLocal is not inherited.
   */
  static TaskDecorator tenantContextTaskDecorator() {
    return runnable -> {
      TenantContext captured = TenantContext.get();
      return () -> TenantContexts.run(captured, runnable);
    };
  }

  @Override
  public Executor getAsyncExecutor() {
    return billingAsyncExecutor();
  }
}
