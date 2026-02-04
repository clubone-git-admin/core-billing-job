package io.clubone.billing.batch.reader;

import io.clubone.billing.batch.model.DueInvoiceRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.LocalDate;

@Configuration
public class DueInvoiceReaderConfig {

	private static final Logger log = LoggerFactory.getLogger(DueInvoiceReaderConfig.class);

	private static final String DUE_INVOICES_SQL =
		    "SELECT " +
		    "  sis.invoice_id, sis.subscription_instance_id, sis.cycle_number, sis.payment_due_date, " +
		    "  i.client_role_id, i.sub_total, i.tax_amount, i.discount_amount, i.total_amount, " +
		    "  sp.client_payment_method_id " +                                  // ✅ NEW
		    "FROM client_subscription_billing.subscription_invoice_schedule sis " +
		    "JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id " +
		    "JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id " +
		    "JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sis.subscription_instance_id " + // ✅ NEW
		    "JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id " +           // ✅ NEW
		    "WHERE sis.is_active = true " +
		    "  AND sis.schedule_status IN ('PENDING','DUE') " +
		    "  AND sis.payment_due_date <= ?::date " +
		    "  AND i.is_active = true " +
		    "  AND lis.status_name IN ('PENDING','DUE') " +
		    "  AND NOT EXISTS ( " +
		    "    SELECT 1 FROM client_subscription_billing.subscription_billing_history h " +
		    "    WHERE h.billing_run_id = ?::uuid AND h.invoice_id = sis.invoice_id " +
		    "  ) " +
		    "ORDER BY sis.payment_due_date, sis.invoice_id";


  @Bean
  @StepScope
  public JdbcCursorItemReader<DueInvoiceRow> dueInvoiceReader(
      @Qualifier("cluboneDataSource") DataSource dataSource,
      @Value("#{jobParameters['asOfDate']}") String asOfDateStr,
      @Value("#{jobExecutionContext['billingRunId']}") String billingRunIdStr) {
    try {
      LocalDate asOfDate = LocalDate.parse(asOfDateStr);
      log.info("Creating due invoice reader: asOfDate={} billingRunId={}", asOfDate, billingRunIdStr);

      return new JdbcCursorItemReaderBuilder<DueInvoiceRow>()
        .name("dueInvoiceReader")
        .dataSource(dataSource)
        .sql(DUE_INVOICES_SQL)
        .preparedStatementSetter(ps -> {
          ps.setObject(1, asOfDate);
          ps.setString(2, billingRunIdStr);
        })
        .rowMapper(new DueInvoiceRowMapper())
        .build();
    } catch (Exception e) {
      log.error("Failed to create due invoice reader: asOfDate={} billingRunId={}", asOfDateStr, billingRunIdStr, e);
      throw new IllegalStateException("Failed to create due invoice reader: " + e.getMessage(), e);
    }
  }
}
