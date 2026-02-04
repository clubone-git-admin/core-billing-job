package io.clubone.billing.batch;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.JdbcStepExecutionDao;

import java.time.LocalDateTime;

/**
 * Custom StepExecutionDao that ensures start_time is set during insert
 * to work with database tables that have start_time as NOT NULL.
 */
public class CustomStepExecutionDao extends JdbcStepExecutionDao {

  public CustomStepExecutionDao() {
    super();
  }

  @Override
  public void saveStepExecution(StepExecution stepExecution) {
    // Ensure start_time is set if it's null (Spring Batch sometimes inserts with null initially)
    if (stepExecution.getStartTime() == null) {
      stepExecution.setStartTime(LocalDateTime.now());
    }
    // Call parent implementation
    super.saveStepExecution(stepExecution);
  }

  @Override
  public void updateStepExecution(StepExecution stepExecution) {
    // Ensure start_time is set before update as well
    if (stepExecution.getStartTime() == null) {
      stepExecution.setStartTime(LocalDateTime.now());
    }
    super.updateStepExecution(stepExecution);
  }
}
