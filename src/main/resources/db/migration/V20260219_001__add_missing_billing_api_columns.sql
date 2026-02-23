-- Migration: Add missing columns for Billing API
-- Date: 2026-02-19
-- Description: Adds columns recommended in DATA_MODEL_REVIEW.md

-- Add rejection_reason to billing_run_approval
ALTER TABLE client_subscription_billing.billing_run_approval
ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

-- Add resolution_action to billing_dead_letter_queue
ALTER TABLE client_subscription_billing.billing_dead_letter_queue
ADD COLUMN IF NOT EXISTS resolution_action VARCHAR(50);

-- Add audit log enhancements
ALTER TABLE client_subscription_billing.billing_audit_log
ADD COLUMN IF NOT EXISTS ip_address VARCHAR(45),
ADD COLUMN IF NOT EXISTS user_agent TEXT,
ADD COLUMN IF NOT EXISTS user_email VARCHAR(255);

-- Note: location_name is retrieved via JOIN with locations.location table
-- No need to denormalize unless performance requires it

-- Add index for common queries
CREATE INDEX IF NOT EXISTS idx_billing_run_approval_status_level 
ON client_subscription_billing.billing_run_approval(billing_run_id, approval_level, status_code);

CREATE INDEX IF NOT EXISTS idx_dlq_resolution_action 
ON client_subscription_billing.billing_dead_letter_queue(resolution_action) 
WHERE resolution_action IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_log_user_email 
ON client_subscription_billing.billing_audit_log(user_email) 
WHERE user_email IS NOT NULL;
