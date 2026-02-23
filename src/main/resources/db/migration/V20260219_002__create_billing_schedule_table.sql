-- Migration: Create billing_schedule table for scheduling APIs
-- Date: 2026-02-19
-- Description: Creates table for scheduled billing runs as specified in API documentation

CREATE TABLE IF NOT EXISTS client_subscription_billing.billing_schedule (
    schedule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_name VARCHAR(255) NOT NULL,
    schedule_type VARCHAR(50) NOT NULL, -- DUE_PREVIEW, INVOICE_GENERATION, etc.
    due_date_pattern VARCHAR(50) NOT NULL, -- DAILY, WEEKLY, MONTHLY, etc.
    due_date_cron VARCHAR(100),
    location_id UUID,
    is_active BOOLEAN DEFAULT true NOT NULL,
    next_run_at TIMESTAMPTZ,
    last_run_id UUID REFERENCES client_subscription_billing.billing_run(billing_run_id) ON DELETE SET NULL,
    last_run_code VARCHAR(50),
    last_run_on TIMESTAMPTZ,
    config JSONB,
    created_by UUID,
    created_on TIMESTAMPTZ DEFAULT now() NOT NULL,
    modified_on TIMESTAMPTZ,
    modified_by UUID,
    CONSTRAINT chk_schedule_type CHECK (schedule_type IN ('DUE_PREVIEW', 'INVOICE_GENERATION', 'MOCK_CHARGE', 'ACTUAL_CHARGE')),
    CONSTRAINT chk_due_date_pattern CHECK (due_date_pattern IN ('DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY', 'QUARTERLY'))
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_billing_schedule_active 
ON client_subscription_billing.billing_schedule(is_active) 
WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_billing_schedule_next_run 
ON client_subscription_billing.billing_schedule(next_run_at) 
WHERE next_run_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_billing_schedule_location 
ON client_subscription_billing.billing_schedule(location_id) 
WHERE location_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_billing_schedule_type 
ON client_subscription_billing.billing_schedule(schedule_type);

-- Foreign key to locations (if location_id is provided)
-- Note: Uncomment if locations.location table exists and you want referential integrity
-- ALTER TABLE client_subscription_billing.billing_schedule
-- ADD CONSTRAINT fk_billing_schedule_location 
-- FOREIGN KEY (location_id) REFERENCES locations.location(location_id) ON DELETE SET NULL;
