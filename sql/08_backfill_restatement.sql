-- ============================================================================
-- 08_backfill_restatement.sql
-- Reload a historical date range WITHOUT corrupting existing history.
-- Worked example: re-process November 2024.
--
-- DISQUALIFIED IMMEDIATELY: DELETE FROM dim_client WHERE month(valid_from) = 11.
-- That destroys SCD2 versions which OPENED in November but are still the current
-- version, orphaning every fact loaded since.
--
-- The principle throughout: this is RE-DERIVATION, not mutation. Bronze is
-- append-only and still holds every original CDC event, so November is rebuilt
-- from the same immutable source that produced it the first time.
-- ============================================================================

SET VAR backfill_from = TIMESTAMP'2024-11-01 00:00:00';
SET VAR backfill_to   = TIMESTAMP'2024-12-01 00:00:00';

-- ---------------------------------------------------------------------------
-- STEP 1 - bound the blast radius precisely.
-- The affected set is NOT "versions whose valid_from is in November". It is
-- every version whose validity INTERVAL OVERLAPS November. A version opened in
-- September and still open in November is affected; the naive predicate misses it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW affected_versions AS
SELECT * FROM workspace.deriv_assement_silver.dim_client
WHERE valid_from < TIMESTAMP'2024-12-01' AND valid_to >= TIMESTAMP'2024-11-01';

SELECT COUNT(*) AS versions_affected, COUNT(DISTINCT client_id) AS clients_affected
FROM affected_versions;

-- ---------------------------------------------------------------------------
-- STEP 2 - snapshot BEFORE touching anything. This is the real safety net:
-- Delta time travel turns a botched backfill into a one-statement recovery
-- instead of an incident.
-- ---------------------------------------------------------------------------
DESCRIBE HISTORY workspace.deriv_assement_silver.dim_client;   -- note version N
-- Rollback if verification fails:
--   RESTORE TABLE workspace.deriv_assement_silver.dim_client TO VERSION AS OF N;

-- Capture pre-backfill control totals for the STEP 7 comparison.
CREATE OR REPLACE TABLE workspace.deriv_assement_silver.backfill_control_before AS
SELECT COUNT(*) AS row_count, SUM(amount_usd) AS total_usd, COUNT(DISTINCT client_id) AS clients
FROM workspace.deriv_assement_gold.fact_deposit
WHERE date_key BETWEEN 20241101 AND 20241130;

-- ---------------------------------------------------------------------------
-- STEP 3 - rewind the CDC watermark. Delete the LOG entries, not the history.
-- The watermark is control data; removing these rows makes the pipeline eligible
-- to reprocess exactly this window and nothing else.
-- ---------------------------------------------------------------------------
DELETE FROM workspace.deriv_assement_silver.cdc_apply_log
WHERE commit_ts >= TIMESTAMP'2024-11-01' AND commit_ts < TIMESTAMP'2024-12-01';

-- ---------------------------------------------------------------------------
-- STEP 4 - surgically unwind ONLY the versions created by the LSNs being
-- replayed. Versions created outside November are never touched, so October's
-- history survives by construction rather than by hope.
-- ---------------------------------------------------------------------------
DELETE FROM workspace.deriv_assement_silver.dim_client
WHERE source_lsn IN (
    SELECT lsn FROM workspace.deriv_assement_bronze.stream_client_profile_changes
    WHERE commit_ts >= TIMESTAMP'2024-11-01' AND commit_ts < TIMESTAMP'2024-12-01');

-- Reopen the version that immediately preceded the deleted ones, so every client
-- again has exactly one current version and no dangling end-date.
MERGE INTO workspace.deriv_assement_silver.dim_client AS t
USING (SELECT client_id, MAX(valid_from) AS max_vf
       FROM workspace.deriv_assement_silver.dim_client GROUP BY client_id) AS s
   ON t.client_id = s.client_id AND t.valid_from = s.max_vf
WHEN MATCHED THEN UPDATE SET
    t.valid_to = TIMESTAMP'9999-12-31 00:00:00',
    t.is_current = TRUE,
    t.updated_at = current_timestamp();

-- ---------------------------------------------------------------------------
-- STEP 5 - replay. Re-run 04_scd2_dim_client_merge.sql unchanged.
-- Because it is ordered by lsn and guarded by cdc_apply_log, it rebuilds exactly
-- the same November timeline: same inputs, same order, same output.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- STEP 6 - restate the FACTS by partition, atomically.
-- replaceWhere is atomic: a reader sees either the old November or the new one,
-- never a half-loaded month. A DELETE + INSERT pair exposes an empty window
-- mid-run, which for a C-suite report means a zero where revenue should be.
--
--   (df.write.format("delta").mode("overwrite")
--      .option("replaceWhere", "date_key >= 20241101 AND date_key <= 20241130")
--      .saveAsTable("workspace.deriv_assement_gold.fact_deposit"))
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- STEP 7 - VERIFY BEFORE RELEASING. All invariant queries must return zero rows.
-- ---------------------------------------------------------------------------
-- (a) exactly one current version per client
SELECT 'FAIL: multiple current versions' AS invariant, client_id, COUNT(*)
FROM workspace.deriv_assement_silver.dim_client
WHERE is_current = TRUE GROUP BY client_id HAVING COUNT(*) > 1;

-- (b) contiguous timeline: no gaps, no overlaps
SELECT 'FAIL: timeline gap or overlap' AS invariant, client_id, valid_to, next_vf
FROM (SELECT client_id, valid_to,
             LEAD(valid_from) OVER (PARTITION BY client_id ORDER BY valid_from) AS next_vf
      FROM workspace.deriv_assement_silver.dim_client)
WHERE next_vf IS NOT NULL AND next_vf <> valid_to;

-- (c) no version opens after it closes
SELECT 'FAIL: inverted interval' AS invariant, client_id, valid_from, valid_to
FROM workspace.deriv_assement_silver.dim_client WHERE valid_from >= valid_to;

-- (d) no fact orphaned by the restatement
SELECT 'FAIL: orphaned fact' AS invariant, f.deposit_id, f.client_id
FROM workspace.deriv_assement_gold.fact_deposit f
LEFT JOIN workspace.deriv_assement_gold.dim_client d ON d.client_sk = f.client_sk
WHERE d.client_sk IS NULL;

-- (e) control totals moved only as expected
SELECT b.row_count AS before_rows, a.row_count AS after_rows,
       b.total_usd AS before_usd,  a.total_usd AS after_usd,
       a.total_usd - b.total_usd   AS delta_usd
FROM workspace.deriv_assement_silver.backfill_control_before b
CROSS JOIN (SELECT COUNT(*) AS row_count, SUM(amount_usd) AS total_usd
            FROM workspace.deriv_assement_gold.fact_deposit
            WHERE date_key BETWEEN 20241101 AND 20241130) a;

-- Only after all of the above pass is the restated window published.
