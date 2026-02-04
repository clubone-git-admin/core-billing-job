package io.clubone.billing.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class BillingRepository {

  private final JdbcTemplate jdbc;

  public BillingRepository(JdbcTemplate jdbcTemplate) {
    this.jdbc = jdbcTemplate;
  }

  public UUID createRun(String mode, java.time.LocalDate asOfDate) {
    UUID id = UUID.randomUUID();
    jdbc.update(
      "INSERT INTO client_subscription_billing.billing_run " +
      "(billing_run_id, run_mode, as_of_date, started_on, status) " +
      "VALUES (?::uuid, ?::text, ?::date, now(), 'RUNNING')",
      id.toString(), mode, asOfDate
    );
    return id;
  }

  public void completeRun(UUID runId, String status, String summaryJson) {
    jdbc.update(
      "UPDATE client_subscription_billing.billing_run " +
      "SET ended_on = now(), status = ?, summary_json = ?::jsonb " +
      "WHERE billing_run_id = ?::uuid",
      status, summaryJson, runId.toString()
    );
  }

  public UUID resolveBillingStatusIdByCode(String code) {
    List<UUID> ids = jdbc.query(
      "SELECT billing_status_id FROM client_subscription_billing.lu_billing_status WHERE status_code = ?",
      (rs, i) -> (UUID) rs.getObject(1),
      code
    );
    return ids.isEmpty() ? null : ids.get(0);
  }

  public List<Map<String, Object>> countsByStatus(UUID runId) {
    return jdbc.queryForList(
      "SELECT s.status_code, COUNT(1) AS cnt " +
      "FROM client_subscription_billing.subscription_billing_history h " +
      "JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id " +
      "WHERE h.billing_run_id = ?::uuid " +
      "GROUP BY s.status_code ORDER BY s.status_code",
      runId.toString()
    );
  }

  public List<Map<String, Object>> history(UUID runId, int limit) {
    return jdbc.queryForList(
      "SELECT h.subscription_billing_history_id, h.invoice_id, h.subscription_instance_id, " +
      "       h.cycle_number, h.payment_due_date, h.billing_attempt_on, h.failure_reason, " +
      "       h.is_mock, " +
      "       h.invoice_sub_total, h.invoice_tax_amount, h.invoice_discount_amount, h.invoice_total_amount, " +
      "       s.status_code " +
      "FROM client_subscription_billing.subscription_billing_history h " +
      "JOIN client_subscription_billing.lu_billing_status s ON s.billing_status_id = h.billing_status_id " +
      "WHERE h.billing_run_id = ?::uuid " +
      "ORDER BY h.billing_attempt_on DESC " +
      "LIMIT ?",
      runId.toString(), limit
    );
  }
}
