package io.clubone.billing.batch.reader;

import io.clubone.billing.batch.BillingJobProperties;
import io.clubone.billing.batch.model.DueInvoiceRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import io.clubone.billing.batch.model.BillingStatus;
import io.clubone.billing.batch.model.ScheduleStatus;

@Configuration
public class DueInvoiceReaderConfig {

	private static final Logger log = LoggerFactory.getLogger(DueInvoiceReaderConfig.class);

  @Bean
  @StepScope
  public JdbcPagingItemReader<DueInvoiceRow> dueInvoiceReader(
      @Qualifier("cluboneDataSource") DataSource dataSource,
      BillingJobProperties props,
      @Value("#{jobParameters['asOfDate']}") String asOfDateStr,
      @Value("#{jobExecutionContext['billingRunId']}") String billingRunIdStr,
      @Value("#{jobExecutionContext['runMode']}") String runModeStr) {
    try {
      LocalDate asOfDate = LocalDate.parse(asOfDateStr);
      log.info("Creating due invoice paging reader: asOfDate={} billingRunId={} preventDuplicates={} useLocking={}", 
          asOfDate, billingRunIdStr, props.isPreventDuplicateAcrossRuns(), props.isUseRowLocking());

      // Create paging query provider
      PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
      queryProvider.setSelectClause(
          " sis.invoice_id, sis.subscription_instance_id, sis.cycle_number, sis.payment_due_date, " +
          "i.client_role_id, i.sub_total, i.tax_amount, i.discount_amount, i.total_amount, " +
          "sp.client_payment_method_id");
      queryProvider.setFromClause(
          " client_subscription_billing.subscription_invoice_schedule sis " +
          "JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id " +
          "JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id " +
          "JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sis.subscription_instance_id " +
          "JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id");
      // Build WHERE clause with idempotency checks
      StringBuilder whereClause = new StringBuilder();
      whereClause.append("sis.is_active = true ");
      whereClause.append("AND sis.schedule_status IN ('")
          .append(ScheduleStatus.PENDING.getCode())
          .append("','")
          .append(ScheduleStatus.DUE.getCode())
          .append("') ");
      whereClause.append("AND sis.payment_due_date <= :asOfDate ");
      whereClause.append("AND i.is_active = true ");
      whereClause.append("AND lis.status_name IN ('")
          .append(io.clubone.billing.batch.model.InvoiceStatus.PENDING.getCode()).append("','")
          .append(io.clubone.billing.batch.model.InvoiceStatus.DUE.getCode()).append("') ");
      
      // Idempotency: Prevent processing same invoice in current run
      whereClause.append("AND NOT EXISTS ( ");
      whereClause.append("  SELECT 1 FROM client_subscription_billing.subscription_billing_history h ");
      whereClause.append("  WHERE h.billing_run_id = :billingRunId::uuid AND h.invoice_id = sis.invoice_id ");
      whereClause.append(") ");
      
      // Idempotency: Prevent processing invoices already successfully billed in previous runs (LIVE only)
      if (props.isPreventDuplicateAcrossRuns() && "LIVE".equalsIgnoreCase(runModeStr)) {
          whereClause.append("AND NOT EXISTS ( ");
          whereClause.append("  SELECT 1 FROM client_subscription_billing.subscription_billing_history h ");
          whereClause.append("  JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id ");
          whereClause.append("  WHERE h.invoice_id = sis.invoice_id ");
          whereClause.append("    AND s.status_code IN ('")
              .append(BillingStatus.LIVE_FINALIZED.getCode())
              .append("') ");
          whereClause.append(") ");
          log.info("Cross-run idempotency enabled: Will skip invoices already successfully billed in previous LIVE runs");
      }
      
      queryProvider.setWhereClause(whereClause.toString());
      
      // Set sort keys for paging (use column names without table aliases for PostgreSQL compatibility)
      // The columns must be in the SELECT clause
      Map<String, org.springframework.batch.item.database.Order> sortKeys = new HashMap<>();
      sortKeys.put("payment_due_date", org.springframework.batch.item.database.Order.ASCENDING);
      sortKeys.put("invoice_id", org.springframework.batch.item.database.Order.ASCENDING);
      queryProvider.setSortKeys(sortKeys);

      // Set parameter values
      Map<String, Object> parameterValues = new HashMap<>();
      parameterValues.put("asOfDate", asOfDate);
      parameterValues.put("billingRunId", billingRunIdStr);

      // Log the query for debugging
      log.info("Reader query WHERE clause: {}", whereClause.toString());
      log.info("Reader query parameters: asOfDate={}, billingRunId={}", asOfDate, billingRunIdStr);

      return new JdbcPagingItemReaderBuilder<DueInvoiceRow>()
        .name("dueInvoiceReader")
        .dataSource(dataSource)
        .queryProvider(queryProvider)
        .parameterValues(parameterValues)
        .pageSize(1000) // Process 1000 records per page
        .rowMapper(new DueInvoiceRowMapper())
        .build();
    } catch (Exception e) {
      log.error("Failed to create due invoice reader: asOfDate={} billingRunId={}", asOfDateStr, billingRunIdStr, e);
      throw new IllegalStateException("Failed to create due invoice reader: " + e.getMessage(), e);
    }
  }
}
