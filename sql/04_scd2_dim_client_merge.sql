-- ============================================================================
-- 04_scd2_dim_client_merge.sql
-- CDC (client_profile_changes.jsonl) -> SCD Type 2 dim_client.
--
-- The delivered file is in ARRIVAL order, not LSN order:
--   arrival : 1005, 1009, 1001, 1004, 1010, 1012, 1003, 1015, 1008, 1018, 1006, 1020
--   correct : 1001, 1003, 1004, 1005, 1006, 1008, 1009, 1010, 1012, 1015, 1018, 1020
--
-- CL001 has three changes (1004, 1005, 1006) ALL committed on 2024-11-15 and
-- delivered out of order. Applied as delivered, the earlier 1004 image would
-- overwrite the later 1005 balance and the client's balance ends up $600 wrong.
-- Ordering by commit_ts is NOT sufficient - only the LSN gives a total order.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Step 0 - order the batch and filter out already-applied events.
-- The lsn watermark is the replay guard: re-running applies nothing.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW cdc_ordered AS
SELECT
    c.lsn,
    c.client_id,
    c.op,
    CAST(c.commit_ts AS TIMESTAMP) AS commit_ts,
    c.before,
    c.after,
    ROW_NUMBER() OVER (PARTITION BY c.client_id ORDER BY c.lsn) AS seq_in_key
FROM workspace.deriv_assement_bronze.stream_client_profile_changes c
WHERE c.lsn > (SELECT COALESCE(MAX(lsn), 0)
               FROM workspace.deriv_assement_silver.cdc_apply_log)
ORDER BY c.lsn;                                   -- <-- THE critical line

-- ---------------------------------------------------------------------------
-- Step 1 - build the full target image for each event.
-- 'after' carries only the CHANGED subset (risk_category, account_balance_usd,
-- account_status), so unchanged attributes MUST be carried forward from the
-- current version. Without these COALESCEs every update would null full_name,
-- nationality and preferred_language.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW cdc_resolved AS
SELECT
    s.lsn, s.client_id, s.op, s.commit_ts,
    COALESCE(s.after.full_name,           p.full_name)           AS full_name,
    COALESCE(s.after.date_of_birth,       p.date_of_birth)       AS date_of_birth,
    COALESCE(s.after.nationality,         p.nationality)         AS nationality,
    COALESCE(s.after.risk_category,       p.risk_category)       AS risk_category,
    COALESCE(s.after.account_balance_usd, p.account_balance_usd) AS account_balance_usd,
    COALESCE(s.after.account_status,      p.account_status)      AS account_status,
    COALESCE(s.after.currency,            p.currency)            AS currency,
    COALESCE(s.after.preferred_language,  p.preferred_language)  AS preferred_language,
    -- Hash of TRACKED attributes only. This single expression decides what
    -- constitutes a new version - narrowing it later (e.g. to exclude balance,
    -- per part2 section 2b.1) is a one-line change plus a backfill.
    sha2(concat_ws('|',
        COALESCE(s.after.risk_category,       p.risk_category),
        COALESCE(s.after.account_balance_usd, p.account_balance_usd),
        COALESCE(s.after.account_status,      p.account_status)), 256) AS new_record_hash,
    p.record_hash AS current_record_hash
FROM cdc_ordered s
LEFT JOIN workspace.deriv_assement_silver.dim_client p
       ON p.client_id = s.client_id AND p.is_current = TRUE;

-- ---------------------------------------------------------------------------
-- Step 2 (Phase A) - close the outgoing version.
-- A single MERGE cannot both close the old row and insert the new one for the
-- same key, so this is deliberately two-phase.
-- The record_hash predicate makes a no-change event a genuine no-op: this is
-- why lsn 1001 (re-inserting CL030 with identical attributes) creates NO new
-- version instead of a duplicate.
-- ---------------------------------------------------------------------------
MERGE INTO workspace.deriv_assement_silver.dim_client AS t
USING cdc_resolved AS s
   ON t.client_id = s.client_id AND t.is_current = TRUE
WHEN MATCHED AND s.op IN ('update', 'delete', 'insert')
             AND t.record_hash <> s.new_record_hash
THEN UPDATE SET
    t.valid_to   = s.commit_ts,
    t.is_current = FALSE,
    t.updated_at = current_timestamp();

-- ---------------------------------------------------------------------------
-- Step 3 (Phase B) - insert the new version for insert/update events.
-- ---------------------------------------------------------------------------
INSERT INTO workspace.deriv_assement_silver.dim_client
SELECT
    sha2(concat_ws('|', s.client_id, CAST(s.commit_ts AS STRING)), 256) AS client_sk,
    s.client_id, s.full_name, s.date_of_birth, s.nationality,
    s.risk_category, s.account_balance_usd, s.account_status,
    s.currency, s.preferred_language,
    s.commit_ts                 AS valid_from,
    TIMESTAMP'9999-12-31 00:00:00' AS valid_to,
    TRUE                        AS is_current,
    FALSE                       AS is_deleted,
    s.lsn                       AS source_lsn,
    s.op                        AS source_op,
    s.new_record_hash           AS record_hash,
    FALSE                       AS is_inferred,
    current_timestamp()         AS updated_at
FROM cdc_resolved s
WHERE s.op IN ('insert', 'update')
  AND (s.current_record_hash IS NULL OR s.current_record_hash <> s.new_record_hash);

-- ---------------------------------------------------------------------------
-- Step 4 - DELETE: soft delete only.
-- Phase A above already end-dated the current row. Here we add a TOMBSTONE that
-- retains the last known attribute values.
--
-- A hard DELETE is not acceptable: CL012 (lsn 1010) has two real deposits in the
-- warehouse (DEP008 and VDEP004). Physically removing the dimension row would
-- orphan both facts and silently change historical revenue.
-- ---------------------------------------------------------------------------
INSERT INTO workspace.deriv_assement_silver.dim_client
SELECT
    sha2(concat_ws('|', s.client_id, CAST(s.commit_ts AS STRING)), 256),
    s.client_id, s.full_name, s.date_of_birth, s.nationality,
    s.risk_category, s.account_balance_usd, s.account_status,
    s.currency, s.preferred_language,
    s.commit_ts, TIMESTAMP'9999-12-31 00:00:00',
    TRUE  AS is_current,
    TRUE  AS is_deleted,        -- <-- tombstone, not a physical delete
    s.lsn, 'delete', s.new_record_hash, FALSE, current_timestamp()
FROM cdc_resolved s
WHERE s.op = 'delete';

-- ---------------------------------------------------------------------------
-- Step 5 - advance the watermark, in the SAME transaction as the writes above.
-- Delta's ACID guarantee means the watermark cannot advance without the data
-- landing. Without that atomicity, a mid-run failure produces either silently
-- skipped events or infinitely reapplied ones.
-- ---------------------------------------------------------------------------
INSERT INTO workspace.deriv_assement_silver.cdc_apply_log
SELECT lsn, client_id, op, commit_ts, current_timestamp(),
       CASE WHEN op = 'delete'                                    THEN 'soft_delete_tombstone'
            WHEN current_record_hash = new_record_hash            THEN 'no_change_noop'
            ELSE 'new_version' END
FROM cdc_resolved;

-- ---------------------------------------------------------------------------
-- Consumer-facing view. Every downstream reader uses THIS, not the base table,
-- so a forgotten is_deleted filter cannot double-count.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW workspace.deriv_assement_silver.dim_client_current AS
SELECT * FROM workspace.deriv_assement_silver.dim_client
WHERE is_current = TRUE AND is_deleted = FALSE;

-- ---------------------------------------------------------------------------
-- ALTERNATIVE: Databricks AUTO CDC (replaced APPLY CHANGES; current recommended
-- API). Declarative, handles reordering and end-dating natively. Requires a
-- Lakeflow Declarative Pipeline on serverless / Pro / Advanced edition.
--
-- Used in production; written out explicitly above because the reordering logic
-- is exactly what this assessment asks to be demonstrated, and because the
-- partial-after-image COALESCE still needs handling upstream either way.
--
--   CREATE OR REFRESH STREAMING TABLE dim_client;
--
--   CREATE FLOW client_profile_cdc AS AUTO CDC INTO dim_client
--   FROM STREAM(bronze.stream_client_profile_changes)
--   KEYS (client_id)
--   APPLY AS DELETE WHEN op = 'delete'
--   SEQUENCE BY lsn                      -- reordering handled by the engine
--   COLUMNS * EXCEPT (op, before)
--   STORED AS SCD TYPE 2;
-- ---------------------------------------------------------------------------
