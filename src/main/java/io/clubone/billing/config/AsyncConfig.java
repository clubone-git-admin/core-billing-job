package io.clubone.billing.config;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Bounded async pool for invoice-gen / mock-charge / actual-charge jobs.
 * Prevents unbounded thread growth (Spring default) that contributed to OOM under load.
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
    ex.initialize();
    log.info(
        "billingAsyncExecutor core={} max={} queue={}",
        ex.getCorePoolSize(),
        ex.getMaxPoolSize(),
        queueCapacity);
    return ex;
  }

  @Override
  public Executor getAsyncExecutor() {
    return billingAsyncExecutor();
  }
}
