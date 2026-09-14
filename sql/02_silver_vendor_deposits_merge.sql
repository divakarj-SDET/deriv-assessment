-- ============================================================================
-- 02_silver_vendor_deposits_merge.sql
-- Bronze -> Silver for the vendor deposit feed.
--
-- Handles three defects present in the delivered files:
--   1. schema drift  : 20240302 renames payment_method -> method
--   2. duplicates    : VDEP002 / VDEP005 redelivered in 20240302
--   3. replay safety : identical re-run must write nothing
-- ============================================================================

-- Step 1 - normalise schema drift.
-- COALESCE over DECLARED aliases only. An unmapped column must NOT be silently
-- dropped: 03_dq_checks.sql raises it at BLOCK severity and aborts the batch.
CREATE OR REPLACE TEMP VIEW vendor_normalised AS
SELECT
    deposit_id,
    client_id,
    CAST(deposit_date AS DATE)              AS deposit_date,
    CAST(amount_usd AS DECIMAL(18,2))       AS amount_usd,
    COALESCE(payment_method, method)        AS payment_method,   -- <-- the drift fix
    currency_original,
    CAST(exchange_rate AS DECIMAL(18,6))    AS exchange_rate,
    status,
    CAST(processing_days AS INT)            AS processing_days,
    CAST(fee_usd AS DECIMAL(18,2))          AS fee_usd,
    'VENDOR'                                AS source_system,
    _source_file                            AS source_file,
    sha2(concat_ws('|', deposit_id, client_id, deposit_date, amount_usd,
                   COALESCE(payment_method, method), currency_original,
                   exchange_rate, status, processing_days, fee_usd), 256) AS row_hash
FROM workspace.deriv_assement_bronze.stream_client_deposits;

-- Step 2 - deduplicate on the business key.
-- The vendor redelivers rows across files. Last file wins, so a CORRECTED row
-- supersedes the original while an IDENTICAL row collapses harmlessly.
-- On the delivered data this reduces 22 clean rows -> 20 distinct.
CREATE OR REPLACE TEMP VIEW vendor_deduped AS
SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
                 PARTITION BY deposit_id
                 ORDER BY source_file DESC     -- later file wins
             ) AS rn
    FROM vendor_normalised
) WHERE rn = 1;

-- Step 3 - idempotent MERGE.
-- The row_hash predicate on WHEN MATCHED is what makes replay a true no-op:
-- an unchanged row matches but fails the predicate, so no write occurs.
MERGE INTO workspace.deriv_assement_silver.silver_deposit AS t
USING (SELECT * FROM vendor_deduped
       WHERE deposit_id NOT IN (SELECT deposit_id
                                FROM workspace.deriv_assement_silver.quarantine_deposit
                                WHERE resolved_at IS NULL)) AS s
   ON t.deposit_id = s.deposit_id AND t.source_system = s.source_system

WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
    t.client_id = s.client_id, t.deposit_date = s.deposit_date,
    t.amount_usd = s.amount_usd, t.payment_method = s.payment_method,
    t.currency_original = s.currency_original, t.exchange_rate = s.exchange_rate,
    t.status = s.status, t.processing_days = s.processing_days,
    t.fee_usd = s.fee_usd, t.source_file = s.source_file,
    t.row_hash = s.row_hash, t.updated_at = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    deposit_id, client_id, deposit_date, amount_usd, payment_method,
    currency_original, exchange_rate, status, processing_days, fee_usd,
    source_system, source_file, row_hash, is_quarantined, effective_from, updated_at
) VALUES (
    s.deposit_id, s.client_id, s.deposit_date, s.amount_usd, s.payment_method,
    s.currency_original, s.exchange_rate, s.status, s.processing_days, s.fee_usd,
    s.source_system, s.source_file, s.row_hash, FALSE, current_timestamp(), current_timestamp()
);

-- Step 4 - release anything in quarantine whose blocker has since cleared.
-- This is what makes the late-arriving-dimension case self-healing: when CL099
-- finally appears in client_signup, VDEP020 flows through with no manual replay.
MERGE INTO workspace.deriv_assement_silver.quarantine_deposit AS q
USING (SELECT client_id FROM workspace.deriv_assement_silver.dim_client_current) AS c
   ON q.client_id = c.client_id AND q.resolved_at IS NULL
WHEN MATCHED THEN UPDATE SET q.resolved_at = current_timestamp();
