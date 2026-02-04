# ClubOne Billing Batch (MOCK + LIVE) - Invoice-driven, Schedule-based

This project runs a Spring Batch job against your existing ClubOne PostgreSQL database:
- Reads **due invoices** from: `client_subscription_billing.subscription_invoice_schedule`
- Reads **amounts** from: `transactions.invoice`
- Writes **billing outcomes** to: `client_subscription_billing.subscription_billing_history`
- Groups each run in: `client_subscription_billing.billing_run`
- Uses Spring Batch metadata tables: `spring_batch.BATCH_*` in the same database.

## Why this job is "full fledged"
- Supports `mode=MOCK` and `mode=LIVE`
- Uses `billing_run_id` for grouping + idempotency (won't re-write same invoice in the same run)
- Has a Payment abstraction for LIVE:
  - `NOOP` strategy for end-to-end wiring
  - `HTTP` strategy to call your core-payment-service for real capture

## IMPORTANT prerequisites in your DB
1) Spring Batch metadata tables exist in `spring_batch` schema and you configured prefix `spring_batch.BATCH_`.
2) Your domain DDL changes exist:
   - `client_subscription_billing.subscription_invoice_schedule`
   - `client_subscription_billing.billing_run`
   - `client_subscription_billing.subscription_billing_history` has columns:
     - is_mock, billing_run_id, simulated_on
     - (recommended) invoice_sub_total, invoice_tax_amount, invoice_discount_amount, invoice_total_amount
3) `client_subscription_billing.lu_billing_status` contains these status codes at minimum:
   - MOCK_EVALUATED
   - MOCK_SKIPPED_NOT_ELIGIBLE
   - MOCK_ERROR
   - LIVE_SUCCESS
   - LIVE_FAILED
   - LIVE_SKIPPED_NOT_ELIGIBLE
   - LIVE_ERROR

If any code is missing, the job will fail fast with: `Missing lu_billing_status.status_code=...`

## Configuration
Set DB password via env var:
```bash
export CLUBONE_DB_PASSWORD="YOUR_PASSWORD"
```

Optional:
```bash
export CLUBONE_JDBC_URL="jdbc:postgresql://.../clubone_dev"
export CLUBONE_DB_USER="clubone_dev_user"
```

### LIVE payments configuration
By default LIVE uses NOOP (doesn't hit gateway) so you can validate flow:

```yaml
clubone.billing.payment.strategy: NOOP
clubone.billing.noop.outcome: SUCCESS
```

To integrate real payments via your core-payment-service:
```yaml
clubone.billing.payment.strategy: HTTP
clubone.billing.payment.http.base-url: http://localhost:8081
clubone.billing.payment.http.create-and-capture-path: /api/payments/capture-invoice
```

### Updating invoice/schedule
You previously said "don't touch invoice table". So:
- `clubone.billing.update-invoice=false` (default)
- `clubone.billing.update-schedule=true` (default)

If you want LIVE run to mark invoices paid:
- set `clubone.billing.update-invoice=true`

## Run
```bash
mvn spring-boot:run
```

## Trigger jobs
MOCK:
```bash
curl -X POST "http://localhost:8080/api/billing/run?mode=MOCK&asOfDate=2026-02-01"
```

LIVE:
```bash
curl -X POST "http://localhost:8080/api/billing/run?mode=LIVE&asOfDate=2026-02-01"
```

Summary:
```bash
curl "http://localhost:8080/api/billing/run/{billingRunId}/summary"
```

History:
```bash
curl "http://localhost:8080/api/billing/run/{billingRunId}/history?limit=200"
```

## Notes for production hardening (next steps)
- Use JdbcPagingItemReader for large volume
- Add optimistic locking / SKIP LOCKED when selecting due invoices (avoid multiple runners)
- Add retry policies for transient payment failures (and separate retry job)
- Update schedule_status to DUE before processing (or derive by date)
- In LIVE, persist gateway references to your payment tables if you want full traceability
