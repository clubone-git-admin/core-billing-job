package io.clubone.billing.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchDataSourceConfig implements ApplicationContextAware {

	private static final Logger log = LoggerFactory.getLogger(BatchDataSourceConfig.class);

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		try {
			ConfigurableListableBeanFactory beanFactory = ((org.springframework.context.ConfigurableApplicationContext) applicationContext).getBeanFactory();
			StepScope stepScope = new StepScope();
			stepScope.setAutoProxy(true);
			beanFactory.registerScope("step", stepScope);
			log.debug("Registered step scope");
		} catch (Exception e) {
			log.error("Failed to register step scope", e);
			throw new IllegalStateException("Failed to register step scope: " + e.getMessage(), e);
		}
	}

  @Bean
  @Primary
  public PlatformTransactionManager batchTransactionManager(@Qualifier("cluboneDataSource") DataSource dataSource) {
    return new DataSourceTransactionManager(dataSource);
  }

  @Bean
  @Primary
  public JobRepository jobRepository(@Qualifier("cluboneDataSource") DataSource dataSource,
                                     PlatformTransactionManager batchTransactionManager) throws Exception {
    JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
    factory.setDataSource(dataSource);
    factory.setTransactionManager(batchTransactionManager);
    factory.setTablePrefix("batch_job_logs.batch_");
    factory.setDatabaseType("POSTGRES");
    factory.setMaxVarCharLength(2500);
    factory.afterPropertiesSet();
    
    // Get the created repository first
    JobRepository repository = factory.getObject();
    
    // Unwrap proxy to get the actual SimpleJobRepository implementation
    Object targetRepository = repository;
    if (AopUtils.isAopProxy(repository) && repository instanceof org.springframework.aop.framework.Advised) {
      try {
        org.springframework.aop.framework.Advised advised = (org.springframework.aop.framework.Advised) repository;
        targetRepository = advised.getTargetSource().getTarget();
      } catch (Exception e) {
        log.error("Failed to unwrap proxy to get target repository", e);
        throw new IllegalStateException("Failed to unwrap proxy to get target repository", e);
      }
    }
    
    if (!(targetRepository instanceof SimpleJobRepository)) {
      log.error("Expected SimpleJobRepository but got: {}", targetRepository != null ? targetRepository.getClass().getName() : "null");
      throw new IllegalStateException("Expected SimpleJobRepository but got: " + targetRepository.getClass().getName());
    }
    
    SimpleJobRepository simpleRepository = (SimpleJobRepository) targetRepository;
    
    // Use reflection to get the original StepExecutionDao and its incrementer
    StepExecutionDao originalDao = null;
    Object incrementer = null;
    java.lang.reflect.Field daoField = null;
    
    Class<?> clazz = SimpleJobRepository.class;
    while (clazz != null && clazz != Object.class) {
      java.lang.reflect.Field[] fields = clazz.getDeclaredFields();
      for (java.lang.reflect.Field field : fields) {
        if (StepExecutionDao.class.isAssignableFrom(field.getType())) {
          field.setAccessible(true);
          originalDao = (StepExecutionDao) field.get(simpleRepository);
          daoField = field;
          
          // Get the incrementer from the original DAO
          if (originalDao instanceof org.springframework.batch.core.repository.dao.JdbcStepExecutionDao) {
            try {
              java.lang.reflect.Field incrementerField = org.springframework.batch.core.repository.dao.JdbcStepExecutionDao.class.getDeclaredField("stepExecutionIncrementer");
              incrementerField.setAccessible(true);
              incrementer = incrementerField.get(originalDao);
            } catch (NoSuchFieldException | IllegalAccessException e) {
              // If we can't get it, we'll try to create one
            }
          }
          break;
        }
      }
      if (originalDao != null) {
        break;
      }
      clazz = clazz.getSuperclass();
    }
    
    if (originalDao == null || daoField == null) {
      log.error("Could not find StepExecutionDao in SimpleJobRepository");
      throw new IllegalStateException("Could not find StepExecutionDao in SimpleJobRepository");
    }
    
    // Create custom StepExecutionDao that ensures start_time is set
    CustomStepExecutionDao stepExecutionDao = new CustomStepExecutionDao();
    stepExecutionDao.setJdbcTemplate(new JdbcTemplate(dataSource));
    stepExecutionDao.setTablePrefix("batch_job_logs.batch_");
    
    // Set the incrementer (either copied from original or create a default one)
    if (incrementer != null) {
      try {
        java.lang.reflect.Field incrementerField = org.springframework.batch.core.repository.dao.JdbcStepExecutionDao.class.getDeclaredField("stepExecutionIncrementer");
        incrementerField.setAccessible(true);
        incrementerField.set(stepExecutionDao, incrementer);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        log.error("Failed to set StepExecutionIncrementer", e);
        throw new IllegalStateException("Failed to set StepExecutionIncrementer", e);
      }
    } else {
      log.error("Could not extract StepExecutionIncrementer from original DAO");
      throw new IllegalStateException("Could not extract StepExecutionIncrementer from original DAO");
    }
    
    stepExecutionDao.afterPropertiesSet();
    
    // Replace the StepExecutionDao in the actual repository (not the proxy)
    daoField.set(simpleRepository, stepExecutionDao);
    
    return repository;
  }

  @Bean
  @Primary
  public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
    org.springframework.batch.core.launch.support.TaskExecutorJobLauncher jobLauncher = 
        new org.springframework.batch.core.launch.support.TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.setTaskExecutor(new SyncTaskExecutor());
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  @Bean
  @Primary
  public JobExplorer jobExplorer(@Qualifier("cluboneDataSource") DataSource dataSource,
                                 PlatformTransactionManager batchTransactionManager) throws Exception {
    JobExplorerFactoryBean factory = new JobExplorerFactoryBean();
    factory.setDataSource(dataSource);
    factory.setTransactionManager(batchTransactionManager);
    factory.setTablePrefix("batch_job_logs.batch_");
    factory.afterPropertiesSet();
    return factory.getObject();
  }
}
