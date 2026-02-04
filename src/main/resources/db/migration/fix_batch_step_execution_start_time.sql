-- Fix Spring Batch schema mismatch: make start_time nullable
-- Spring Batch inserts step execution records with NULL start_time initially, then updates it
-- The table currently has start_time as NOT NULL, which causes insertion failures

ALTER TABLE batch_job_logs.batch_step_execution 
ALTER COLUMN start_time DROP NOT NULL;
